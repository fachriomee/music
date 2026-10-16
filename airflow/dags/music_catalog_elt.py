"""
### music_catalog_elt — Massive Music song-metadata pipeline (everything in ONE file)

Answers, for every song of the internal catalog:

* **Q1** How many **videos** does the song have on YouTube?
* **Q2** How many **ISRCs** (recordings) does the song have on Spotify?

```
preflight ─► load_catalog ─┬─► extract_spotify ─► preprocess_spotify ─┬─► dbt_transform ─► dq_checks ─► publish_dashboards
                           └─► extract_youtube ─► preprocess_youtube ─┤
                                                                      └─► notify_quarantine (e-mail)
```

| task | what it does |
|---|---|
| `load_catalog` | Google-Sheet export → **pandas** (standardise, validate, de-duplicate) → bad rows to `dq.quarantine_records`, good rows **upserted** into `clean.catalog_songs` as **SCD Type 2** |
| `extract_*` | API → `raw.*`: every response kept verbatim, append-only, flushed every 200 rows (resumable) |
| `preprocess_*` | `raw.*` rows newer than the **watermark** → pandas (flatten JSON, standardise, validate, de-duplicate) → quarantine / **upsert** `clean.*` (SCD Type 2) |
| `dbt_transform` | `clean.*` → staging → intermediate (song ↔ recording / video matching) → marts (Cosmos: one task per model + its tests) |
| `dq_checks` | statistical checks → `ops.dq_results`; fails the run on `error` checks |
| `notify_quarantine` | e-mails `ALERT_EMAIL_TO` when this run quarantined records |
| `publish_dashboards` | Superset dashboards as code (Song Catalog, Pipeline Health, Data Quarantine) |

Always **incremental**: only songs never searched, older than `*_REFRESH_DAYS`, or changed in the catalog
(new SCD2 version) are sent to the APIs. Every task that fails after its last retry e-mails `ALERT_EMAIL_TO`.

**Schedule:** 15:15 Asia/Jakarta (08:15 UTC), just after the YouTube quota resets at midnight
America/Los_Angeles. **Param** `max_songs` caps the songs searched per API in a manual run.

Code style: plain functions + dicts (no classes, comprehensions or lambdas) so every step can be
followed in a debugger. Search for `SECTION <n>` to jump:

1 config · 2 helpers · 3 e-mail alerts · 4 ClickHouse · 5 DDL · 6 run log & watermarks · 7 HTTP ·
8 text · 9 upsert + SCD2 · 10 quarantine · 11 catalog · 12 song selection · 13-14 Spotify ·
15-16 YouTube · 17 quarantine e-mail · 18 data quality · 19 Superset · 20 DAG
"""
from __future__ import annotations

import base64
import hashlib
import html
import json
import logging
import math
import os
import random
import re
import smtplib
import ssl
import time
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import clickhouse_connect
import numpy as np
import pandas as pd
import pendulum
import requests
from airflow.sdk import Param, dag, get_current_context, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior

log = logging.getLogger("music_catalog_elt")


# =============================================================================
# SECTION 1 — configuration (every value comes from .env via docker-compose)
# =============================================================================
DAG_ID = "music_catalog_elt"
DAG_FOLDER = Path(__file__).resolve().parent
DBT_PROJECT_DIR = DAG_FOLDER / "dbt" / "music_dw"
DBT_EXECUTABLE = "/opt/dbt_venv/bin/dbt"          # isolated venv, see docker/airflow/Dockerfile
JAKARTA = ZoneInfo("Asia/Jakarta")
PACIFIC = ZoneInfo("America/Los_Angeles")          # the YouTube quota day
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
OPEN_END = datetime(2299, 12, 31, 23, 59, 59, tzinfo=timezone.utc)   # valid_to of an open SCD2 version

# Checked by the preflight task (not at parse time, so a missing value becomes a
# red task with a clear message instead of a DAG that silently disappears).
REQUIRED_ENV = [
    "CLICKHOUSE_ETL_USER", "CLICKHOUSE_ETL_PASSWORD", "CLICKHOUSE_BI_USER", "CLICKHOUSE_BI_PASSWORD",
    "SPOTIFY_CLIENT_ID", "SPOTIFY_CLIENT_SECRET", "YOUTUBE_API_KEY",
    "SUPERSET_ADMIN_USER", "SUPERSET_ADMIN_PASSWORD", "ALERT_EMAIL_TO",
]

_SETTINGS: dict[str, Any] = {}


def env(name: str, default: str = "") -> str:
    """String env var; empty or blank counts as 'not set'."""
    value = os.getenv(name, "")
    if value.strip() == "":
        return default
    return value.strip()


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


def load_settings() -> dict[str, Any]:
    """Read every setting from the environment into one nested dict."""
    recipients = []
    for address in env("ALERT_EMAIL_TO").split(","):
        if address.strip():
            recipients.append(address.strip())
    smtp_user = env("SMTP_USER")
    mail_from = env("SMTP_MAIL_FROM", smtp_user)
    if mail_from == "":
        mail_from = "massive-music-elt@localhost"
    return {
        "clickhouse": {
            "host": env("CLICKHOUSE_HOST", "clickhouse"),
            "port": env_int("CLICKHOUSE_PORT", 8123),
            "user": env("CLICKHOUSE_ETL_USER"),
            "password": env("CLICKHOUSE_ETL_PASSWORD"),
        },
        "catalog": {
            "source": env("CATALOG_SOURCE", "file"),            # file | gsheet_csv
            "file_path": env("CATALOG_FILE_PATH", "/opt/airflow/data/catalog/DE_TS_Song_Catalog_Data.xlsx"),
            "sheet_name": env("CATALOG_SHEET_NAME", "Data"),
            "gsheet_id": env("GSHEET_ID"),
        },
        "spotify": {
            "client_id": env("SPOTIFY_CLIENT_ID"),
            "client_secret": env("SPOTIFY_CLIENT_SECRET"),
            "market": env("SPOTIFY_MARKET", "ID"),
            # Development-Mode apps: /search accepts limit <= 10 (limit=20 -> HTTP 400)
            "search_limit": min(env_int("SPOTIFY_SEARCH_LIMIT", 10), 10),
            # measured on real data: page 2 of the title+artist query was always empty
            "artist_query_pages": env_int("SPOTIFY_ARTIST_QUERY_PAGES", 1),
            "max_pages": env_int("SPOTIFY_MAX_PAGES", 3),
            "min_interval_seconds": env_float("SPOTIFY_MIN_INTERVAL_SECONDS", 0.25),
            "refresh_days": env_int("SPOTIFY_REFRESH_DAYS", 7),
        },
        "youtube": {
            "api_key": env("YOUTUBE_API_KEY"),
            "region_code": env("YOUTUBE_REGION_CODE", "ID"),
            # search.list costs 100 units whatever maxResults is (<= 50)
            "max_results": min(env_int("YOUTUBE_MAX_RESULTS", 50), 50),
            "video_category_id": env("YOUTUBE_VIDEO_CATEGORY_ID"),
            "daily_quota_units": env_int("YOUTUBE_DAILY_QUOTA_UNITS", 10000),
            "quota_safety_ratio": env_float("YOUTUBE_QUOTA_SAFETY_RATIO", 0.9),
            "refresh_days": env_int("YOUTUBE_REFRESH_DAYS", 30),
        },
        "superset": {
            "url": env("SUPERSET_URL", "http://superset:8088"),          # as seen from Airflow
            "public_url": env("SUPERSET_UI_URL", "http://localhost:8088"),  # as seen by people (e-mail links)
            "admin_user": env("SUPERSET_ADMIN_USER"),
            "admin_password": env("SUPERSET_ADMIN_PASSWORD"),
            "bi_user": env("CLICKHOUSE_BI_USER"),
            "bi_password": env("CLICKHOUSE_BI_PASSWORD"),
        },
        "email": {
            "to": recipients,
            "smtp_host": env("SMTP_HOST"),
            "smtp_port": env_int("SMTP_PORT", 587),
            "security": env("SMTP_SECURITY", "starttls").lower(),    # starttls | ssl | none
            "user": smtp_user,
            "password": env("SMTP_PASSWORD"),
            "mail_from": mail_from,
        },
        "pipeline": {
            "max_songs_per_run": env_int("MAX_SONGS_PER_RUN", 0),
            "scd_max_delete_ratio": env_float("SCD_MAX_DELETE_RATIO", 0.2),
            "airflow_url": env("AIRFLOW_UI_URL", "http://localhost:8080"),
        },
    }


def get_settings() -> dict[str, Any]:
    """Settings cached per process (env vars do not change while a task runs)."""
    if "settings" not in _SETTINGS:
        _SETTINGS["settings"] = load_settings()
    return _SETTINGS["settings"]


def validate_settings(settings: dict[str, Any]) -> list[str]:
    """Raise on missing/invalid required values; return non-fatal warnings."""
    missing = []
    for name in REQUIRED_ENV:
        if env(name) == "":
            missing.append(name)
    if missing:
        raise ValueError("Missing values in .env: " + ", ".join(missing))
    if settings["catalog"]["source"] not in ("file", "gsheet_csv"):
        raise ValueError(f"CATALOG_SOURCE must be 'file' or 'gsheet_csv', got {settings['catalog']['source']!r}")
    if settings["catalog"]["source"] == "gsheet_csv" and settings["catalog"]["gsheet_id"] == "":
        raise ValueError("CATALOG_SOURCE=gsheet_csv requires GSHEET_ID")
    if settings["email"]["security"] not in ("starttls", "ssl", "none"):
        raise ValueError("SMTP_SECURITY must be starttls, ssl or none")
    warnings = []
    if not email_can_send(settings["email"]):
        warnings.append("e-mail alerts are disabled until SMTP_HOST, SMTP_USER and SMTP_PASSWORD "
                        "(Gmail: an App Password) are set in .env")
    return warnings


# =============================================================================
# SECTION 2 — small helpers
# =============================================================================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: Any) -> datetime:
    """datetime from ClickHouse / pandas -> timezone-aware UTC datetime."""
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def wib(value: datetime) -> str:
    return as_utc(value).astimezone(JAKARTA).strftime("%Y-%m-%d %H:%M:%S WIB")


def ch_timestamp(value: datetime) -> str:
    """UTC timestamp text with milliseconds, for toDateTime64({x:String}, 3, 'UTC')."""
    return as_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def is_missing(value: Any) -> bool:
    """None, NaN, NaT, pd.NA and blank strings are all 'missing'."""
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def to_int_or_none(value: Any) -> int | None:
    """'1234' / 1234.0 / 1234 -> 1234; missing or garbage -> None."""
    if is_missing(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_int_or_zero(value: Any) -> int:
    number = to_int_or_none(value)
    if number is None:
        return 0
    return number


def to_flag(value: Any) -> int:
    """Truthy API booleans -> 1, everything else -> 0 (stored as UInt8)."""
    if value is True:
        return 1
    if isinstance(value, str) and value.strip().lower() in ("true", "1", "yes"):
        return 1
    if isinstance(value, (int, np.integer)) and int(value) == 1:
        return 1
    return 0


def batch_id_from_context() -> str:
    """Airflow run_id: unique per DAG run and stable across task retries,
    so it is the idempotency key of everything a run writes."""
    return get_current_context()["run_id"]


def max_songs_from_context() -> int:
    """`max_songs` trigger param, falling back to MAX_SONGS_PER_RUN (0 = no cap)."""
    value = get_current_context()["params"].get("max_songs")
    if value:
        return int(value)
    return get_settings()["pipeline"]["max_songs_per_run"]


# =============================================================================
# SECTION 3 — e-mail alerts (SMTP): task failures + quarantined data
# =============================================================================
def email_can_send(cfg: dict[str, Any]) -> bool:
    """A real provider (Gmail, SES, ...) needs TLS + login; only a local relay
    (SMTP_SECURITY=none) may send without credentials."""
    if cfg["smtp_host"] == "" or len(cfg["to"]) == 0:
        return False
    if cfg["security"] != "none" and (cfg["user"] == "" or cfg["password"] == ""):
        return False
    return True


def smtp_send(cfg: dict[str, Any], subject: str, text_body: str, html_body: str) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = cfg["mail_from"]
    message["To"] = ", ".join(cfg["to"])
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")

    if cfg["security"] == "ssl":
        server = smtplib.SMTP_SSL(cfg["smtp_host"], cfg["smtp_port"], timeout=30,
                                  context=ssl.create_default_context())
    else:
        server = smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"], timeout=30)
    try:
        if cfg["security"] == "starttls":
            server.starttls(context=ssl.create_default_context())
        if cfg["user"]:
            server.login(cfg["user"], cfg["password"])
        server.send_message(message)
    finally:
        try:
            server.quit()
        except Exception:
            server.close()


def record_alert(batch_id: str, alert_type: str, subject: str, recipients: list[str],
                 status: str, error: str) -> None:
    """One row per alert in ops.alert_log (shown on the Pipeline Health dashboard)."""
    try:
        client = get_client()
        client.insert(
            "ops.alert_log",
            [[uuid.uuid4(), utcnow(), batch_id, alert_type, subject, ", ".join(recipients), status, error[:1000]]],
            column_names=["alert_id", "sent_at", "batch_id", "alert_type", "subject", "recipients",
                          "status", "error_message"],
        )
    except Exception as exc:  # observability must never break the pipeline
        log.warning("could not write ops.alert_log: %s", exc)


def send_email(subject: str, text_body: str, html_body: str, alert_type: str, batch_id: str) -> str:
    """Send one alert e-mail. Never raises: returns 'sent', 'skipped' or 'failed'."""
    cfg = get_settings()["email"]
    status = "sent"
    error = ""
    if not email_can_send(cfg):
        status = "skipped"
        error = "SMTP not configured (SMTP_HOST / SMTP_USER / SMTP_PASSWORD / ALERT_EMAIL_TO in .env)"
        log.warning("e-mail '%s' NOT sent: %s", subject, error)
    else:
        try:
            smtp_send(cfg, subject, text_body, html_body)
            log.info("e-mail '%s' sent to %s", subject, cfg["to"])
        except Exception as exc:  # alerting must never mask the original problem
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
            log.error("e-mail '%s' could not be sent: %s", subject, error)
    record_alert(batch_id, alert_type, subject, cfg["to"], status, error)
    return status


def html_table(headers: list[str], rows: list[list[Any]]) -> str:
    cell = "border:1px solid #d1d5db;padding:6px 10px;text-align:left;vertical-align:top;font-size:13px"
    parts = ['<table style="border-collapse:collapse;margin:8px 0 16px 0">', "<tr>"]
    for header in headers:
        parts.append(f'<th style="{cell};background:#f3f4f6">{html.escape(str(header))}</th>')
    parts.append("</tr>")
    for row in rows:
        parts.append("<tr>")
        for value in row:
            parts.append(f'<td style="{cell}">{html.escape(str(value))}</td>')
        parts.append("</tr>")
    parts.append("</table>")
    return "".join(parts)


def email_html(title: str, color: str, intro: str, body_html: str, footer: str) -> str:
    return (
        '<div style="font-family:Arial,Helvetica,sans-serif;color:#1b1b1f;max-width:900px">'
        f'<div style="border-left:6px solid {color};padding:4px 0 4px 12px;margin-bottom:12px">'
        f'<div style="font-size:12px;color:#6b7280">Massive Music · song-metadata pipeline</div>'
        f'<div style="font-size:20px;font-weight:bold">{html.escape(title)}</div></div>'
        f'<p style="font-size:14px">{intro}</p>{body_html}'
        f'<p style="font-size:12px;color:#6b7280">{footer}</p></div>'
    )


def text_table(rows: list[list[Any]]) -> str:
    lines = []
    for row in rows:
        values = []
        for value in row:
            values.append(str(value))
        lines.append("  " + " | ".join(values))
    return "\n".join(lines)


def on_task_failure(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback (runs once the LAST retry has failed)."""
    try:
        notify_task_failure(context)
    except Exception as exc:  # a broken callback must not hide the task error
        log.error("failure callback crashed: %s", exc)


def notify_task_failure(context: dict[str, Any]) -> str:
    ti = context.get("ti")
    if ti is None:
        ti = context.get("task_instance")
    dag_id = str(getattr(ti, "dag_id", DAG_ID))
    task_id = str(getattr(ti, "task_id", "?"))
    run_id = str(getattr(ti, "run_id", "") or context.get("run_id", "?"))
    attempt = str(getattr(ti, "try_number", "?"))
    exception = context.get("exception")
    if exception is None:
        error_text = "unknown error - see the task log"
    else:
        error_text = f"{type(exception).__name__}: {exception}"
    airflow_url = get_settings()["pipeline"]["airflow_url"].rstrip("/")
    log_url = f"{airflow_url}/dags/{dag_id}/runs/{quote(run_id, safe='')}/tasks/{task_id}"

    details = [
        ["DAG", dag_id],
        ["Task", task_id],
        ["Run", run_id],
        ["Attempt", attempt],
        ["Failed at", wib(utcnow())],
        ["Error", error_text[:1500]],
        ["Task log", log_url],
    ]
    subject = f"[Massive Music ELT] FAILED: {dag_id}.{task_id}"
    text_body = (f"Task {dag_id}.{task_id} failed after its last retry.\n\n{text_table(details)}\n\n"
                 "Downstream tasks were skipped; the next run is incremental and resumes where this one stopped.")
    html_body = email_html(
        f"Task failed: {task_id}", "#e3262e",
        f"The task <b>{html.escape(task_id)}</b> failed after its last retry. Downstream tasks were skipped.",
        html_table(["Field", "Value"], details) + f'<p><a href="{html.escape(log_url)}">Open the task log in Airflow</a></p>',
        "Runs are incremental: fix the cause and clear the task (or wait for the next schedule); "
        "nothing already loaded is lost.",
    )
    return send_email(subject, text_body, html_body, "task_failure", run_id)


# =============================================================================
# SECTION 4 — ClickHouse helpers (clickhouse-connect over HTTP)
# =============================================================================
def get_client() -> Any:
    """Client for the least-privilege `etl` principal."""
    cfg = get_settings()["clickhouse"]
    return clickhouse_connect.get_client(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["user"],
        password=cfg["password"],
        settings={"max_execution_time": 600},
        connect_timeout=10,
        send_receive_timeout=300,
    )


def query_rows(client: Any, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    """SELECT -> list of {column: value}."""
    if params is None:
        params = {}
    result = client.query(sql, parameters=params)
    rows = []
    for values in result.result_rows:
        row = {}
        for column_name, value in zip(result.column_names, values):
            row[column_name] = value
        rows.append(row)
    return rows


def query_df(client: Any, table_sql: str, columns: list[str], params: dict | None = None) -> pd.DataFrame:
    """SELECT <columns> <table_sql> -> DataFrame of plain Python objects
    (dtype=object), so values round-trip unchanged when written back (SCD2
    closing rows). Columns are named explicitly because an EMPTY result
    carries no column names in clickhouse-connect."""
    if params is None:
        params = {}
    result = client.query(f"SELECT {', '.join(columns)} {table_sql}", parameters=params)
    return pd.DataFrame(result.result_rows, columns=columns, dtype=object)


def clickhouse_value(value: Any) -> Any:
    """pandas / numpy value -> plain Python value accepted by clickhouse-connect."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return as_utc(value)
    if isinstance(value, datetime):
        return as_utc(value)
    if isinstance(value, np.generic):
        return value.item()
    return value


def insert_df(client: Any, table: str, frame: pd.DataFrame, columns: list[str]) -> int:
    """INSERT the given columns of a DataFrame. Returns the number of rows."""
    if frame is None or len(frame) == 0:
        return 0
    rows = []
    for record in frame[columns].itertuples(index=False, name=None):
        row = []
        for value in record:
            row.append(clickhouse_value(value))
        rows.append(row)
    client.insert(table, rows, column_names=columns)
    return len(rows)


# --- insert buffer for the raw landing zone ----------------------------------
# ClickHouse prefers few, larger inserts (each insert creates a data part).
# Flushing every 200 rows also makes the long API extractions resumable: if a
# task dies after 400 songs, those 400 are already stored and skipped next time.
def new_insert_buffer(client: Any, table: str, columns: list[str], flush_every: int = 200) -> dict[str, Any]:
    return {"client": client, "table": table, "columns": columns, "flush_every": flush_every,
            "rows": [], "total_written": 0}


def buffer_add(buffer: dict[str, Any], row: dict[str, Any]) -> None:
    values = []
    for column in buffer["columns"]:
        values.append(row[column])          # KeyError early = schema drift guard
    buffer["rows"].append(values)
    if len(buffer["rows"]) >= buffer["flush_every"]:
        buffer_flush(buffer)


def buffer_flush(buffer: dict[str, Any]) -> None:
    if not buffer["rows"]:
        return
    buffer["client"].insert(buffer["table"], buffer["rows"], column_names=buffer["columns"])
    buffer["total_written"] = buffer["total_written"] + len(buffer["rows"])
    buffer["rows"] = []


def buffer_rows_accepted(buffer: dict[str, Any]) -> int:
    return buffer["total_written"] + len(buffer["rows"])


def flush_all_buffers(buffers: list[dict[str, Any]]) -> None:
    """Flush every buffer even if one fails (partial progress is valuable),
    then re-raise the first error. Meant for `finally:` blocks."""
    first_error = None
    for buffer in buffers:
        try:
            buffer_flush(buffer)
        except Exception as exc:
            log.error("could not flush %d rows into %s: %s", len(buffer["rows"]), buffer["table"], exc)
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


# =============================================================================
# SECTION 5 — DDL (applied idempotently by the preflight task on every run)
# -----------------------------------------------------------------------------
#  raw    bronze  verbatim API responses, append-only MergeTree
#  clean  silver  pandas output, UPSERTED: ReplacingMergeTree(updated_at) -
#                 INSERT a newer row state with the same ORDER BY key and
#                 ClickHouse keeps the latest; reads use FINAL.
#                 SCD Type 2 tables are keyed (business key, version_no).
#  dq             quarantined records (rejected by validation, with reason)
#  ops            run log, quota ledger, DQ results, watermarks, alert log
#  staging / intermediate / marts are built by dbt.
# =============================================================================
DDL_SQL = """
CREATE TABLE IF NOT EXISTS raw.spotify_search_responses
(
    request_id        UUID,
    batch_id          String,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3),
    song_code         UInt32,
    query_type        LowCardinality(String)  COMMENT 'title_artist | title_only',
    query_text        String,
    market            LowCardinality(String),
    page_offset       UInt16,
    page_limit        UInt8,
    http_status       UInt16,
    error_message     String,
    response_json     String CODEC(ZSTD(3))  COMMENT 'verbatim /v1/search body'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, song_code)
COMMENT 'One row per Spotify /v1/search call (append-only)';

CREATE TABLE IF NOT EXISTS raw.spotify_track_responses
(
    request_id        UUID,
    batch_id          String,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3),
    spotify_track_id  String,
    http_status       UInt16,
    error_message     String,
    response_json     String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, spotify_track_id)
COMMENT 'Fallback /v1/tracks/{id} calls for search hits without external_ids.isrc';

CREATE TABLE IF NOT EXISTS raw.youtube_search_responses
(
    request_id        UUID,
    batch_id          String,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3),
    song_code         UInt32,
    query_text        String,
    region_code       LowCardinality(String),
    max_results       UInt8,
    video_category_id String,
    http_status       UInt16,
    error_message     String,
    quota_cost        UInt16                 COMMENT 'search.list = 100 units',
    response_json     String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ingested_at, song_code)
COMMENT 'One row per YouTube search.list call (append-only)';

CREATE TABLE IF NOT EXISTS raw.youtube_video_responses
(
    request_id        UUID,
    batch_id          String,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3),
    video_ids         Array(String)          COMMENT 'up to 50 ids per videos.list call',
    http_status       UInt16,
    error_message     String,
    quota_cost        UInt16                 COMMENT 'videos.list = 1 unit',
    response_json     String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY ingested_at
COMMENT 'One row per YouTube videos.list call (append-only)';

CREATE TABLE IF NOT EXISTS clean.catalog_songs
(
    song_code         UInt32                 COMMENT 'catalog business key (CODE)',
    version_no        UInt32                 COMMENT 'SCD2 version 1..n per song_code',
    song_title        String,
    original_artist   Nullable(String),
    source_row_number UInt32                 COMMENT 'type-1 attribute: overwritten in place',
    record_hash       String                 COMMENT 'sha256 of the SCD2-tracked attributes',
    valid_from        DateTime64(3, 'UTC'),
    valid_to          DateTime64(3, 'UTC')   COMMENT '2299-12-31 23:59:59 while the version is open',
    is_current        UInt8,
    is_deleted        UInt8                  COMMENT '1 = the song disappeared from the sheet',
    batch_id          String                 COMMENT 'run that created this version',
    updated_at        DateTime64(3, 'UTC')   COMMENT 'ReplacingMergeTree version: latest row state wins'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (song_code, version_no)
COMMENT 'Catalog songs, SCD Type 2 (upsert: insert newer row state, read with FINAL)';

CREATE TABLE IF NOT EXISTS clean.spotify_tracks
(
    spotify_track_id       String,
    version_no             UInt32,
    track_name             String,
    isrc                   String           COMMENT 'normalised and validated ISRC',
    artist_names           Array(String),
    artist_ids             Array(String),
    album_id               String,
    album_name             String,
    album_type             LowCardinality(String),
    release_date           Nullable(Date32),
    release_date_precision LowCardinality(String),
    duration_ms            UInt32,
    is_explicit            UInt8,
    track_number           UInt16           COMMENT 'type-1',
    disc_number            UInt8            COMMENT 'type-1',
    isrc_source            LowCardinality(String) COMMENT 'type-1: search | track_lookup',
    record_hash            String,
    valid_from             DateTime64(3, 'UTC'),
    valid_to               DateTime64(3, 'UTC'),
    is_current             UInt8,
    is_deleted             UInt8,
    batch_id               String,
    updated_at             DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (spotify_track_id, version_no)
COMMENT 'Spotify tracks (releases), SCD Type 2';

CREATE TABLE IF NOT EXISTS clean.spotify_searches
(
    song_code         UInt32,
    last_batch_id     String                 COMMENT 'latest run whose search for the song returned data',
    last_searched_at  DateTime64(3, 'UTC'),
    calls             UInt16,
    failed_calls      UInt16,
    is_success        UInt8,
    hit_count         UInt32,
    updated_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY song_code
COMMENT 'Latest Spotify search outcome per catalog song (upsert)';

CREATE TABLE IF NOT EXISTS clean.spotify_search_hits
(
    song_code             UInt32,
    spotify_track_id      String,
    best_search_rank      UInt16,
    found_by_artist_query UInt8,
    found_by_queries      Array(String),
    search_batch_id       String,
    last_seen_at          DateTime64(3, 'UTC'),
    updated_at            DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (song_code, spotify_track_id)
COMMENT 'Song -> Spotify track candidates returned by the searches (upsert)';

CREATE TABLE IF NOT EXISTS clean.youtube_videos
(
    video_id               String,
    version_no             UInt32,
    video_title            String,
    channel_id             String,
    channel_title          String,
    published_at           Nullable(DateTime64(3, 'UTC')),
    category_id            LowCardinality(String),
    duration_seconds       UInt32,
    is_licensed_content    UInt8,
    default_audio_language String,
    view_count             Nullable(UInt64) COMMENT 'type-1 (overwritten in place)',
    like_count             Nullable(UInt64) COMMENT 'type-1',
    comment_count          Nullable(UInt64) COMMENT 'type-1',
    stats_fetched_at       DateTime64(3, 'UTC') COMMENT 'type-1',
    record_hash            String,
    valid_from             DateTime64(3, 'UTC'),
    valid_to               DateTime64(3, 'UTC'),
    is_current             UInt8,
    is_deleted             UInt8            COMMENT '1 = video became unavailable (deleted/private)',
    batch_id               String,
    updated_at             DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (video_id, version_no)
COMMENT 'YouTube videos, SCD Type 2 (+ type-1 statistics)';

CREATE TABLE IF NOT EXISTS clean.youtube_searches
(
    song_code         UInt32,
    last_batch_id     String,
    last_searched_at  DateTime64(3, 'UTC'),
    query_text        String,
    is_success        UInt8,
    hit_count         UInt32,
    updated_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY song_code
COMMENT 'Latest YouTube search outcome per catalog song (upsert)';

CREATE TABLE IF NOT EXISTS clean.youtube_search_hits
(
    song_code         UInt32,
    video_id          String,
    search_rank       UInt16,
    video_title       String,
    channel_id        String,
    channel_title     String,
    published_at      Nullable(DateTime64(3, 'UTC')),
    search_batch_id   String,
    last_seen_at      DateTime64(3, 'UTC'),
    updated_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (song_code, video_id)
COMMENT 'Song -> YouTube video candidates returned by the search (upsert)';

CREATE TABLE IF NOT EXISTS dq.quarantine_records
(
    batch_id          String,
    detected_at       DateTime64(3, 'UTC'),
    source            LowCardinality(String)  COMMENT 'catalog | spotify | youtube',
    entity            LowCardinality(String)  COMMENT 'catalog_song | spotify_track | youtube_video',
    record_key        String,
    record_label      String                  COMMENT 'human-readable identification',
    reject_reason     LowCardinality(String),
    severity          LowCardinality(String)  COMMENT 'warn | error',
    record_json       String                  COMMENT 'the rejected record as JSON'
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (batch_id, source, entity, record_key, reject_reason)
COMMENT 'Records rejected by validation: kept with a reason instead of silently dropped';

CREATE TABLE IF NOT EXISTS ops.pipeline_runs
(
    batch_id          String,
    task_name         LowCardinality(String),
    attempt_id        UUID,
    status            LowCardinality(String)  COMMENT 'running | success | partial | failed',
    started_at        DateTime64(3, 'UTC'),
    finished_at       Nullable(DateTime64(3, 'UTC')),
    rows_loaded       UInt64,
    api_calls         UInt32,
    api_errors        UInt32,
    details           String                  COMMENT 'JSON counters',
    error_message     String,
    updated_at        DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (batch_id, task_name)
COMMENT 'Latest status per (run, task); read with FINAL';

CREATE TABLE IF NOT EXISTS ops.api_quota_usage
(
    event_id          UUID,
    batch_id          String,
    recorded_at       DateTime64(3, 'UTC') DEFAULT now64(3),
    api               LowCardinality(String),
    endpoint          LowCardinality(String)  COMMENT 'search.list | videos.list',
    quota_date        Date                    COMMENT 'quota day in America/Los_Angeles',
    units             UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(quota_date)
ORDER BY (api, quota_date, recorded_at)
COMMENT 'Every quota-consuming YouTube call';

CREATE TABLE IF NOT EXISTS ops.dq_results
(
    batch_id          String,
    checked_at        DateTime64(3, 'UTC') DEFAULT now64(3),
    check_name        LowCardinality(String),
    layer             LowCardinality(String),
    severity          LowCardinality(String),
    status            LowCardinality(String)  COMMENT 'pass | warn | fail',
    metric_value      Float64,
    threshold         String,
    details           String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(checked_at)
ORDER BY (check_name, checked_at)
COMMENT 'History of post-load data-quality checks';

CREATE TABLE IF NOT EXISTS ops.load_watermarks
(
    source            LowCardinality(String)  COMMENT 'spotify | youtube',
    watermark         DateTime64(3, 'UTC')    COMMENT 'raw rows with ingested_at <= watermark are processed',
    batch_id          String,
    rows_processed    UInt64,
    updated_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY source
COMMENT 'Incremental raw -> clean processing position per source';

CREATE TABLE IF NOT EXISTS ops.alert_log
(
    alert_id          UUID,
    sent_at           DateTime64(3, 'UTC'),
    batch_id          String,
    alert_type        LowCardinality(String)  COMMENT 'task_failure | quarantine',
    subject           String,
    recipients        String,
    status            LowCardinality(String)  COMMENT 'sent | skipped | failed',
    error_message     String
)
ENGINE = MergeTree
ORDER BY sent_at
COMMENT 'Every e-mail alert the pipeline tried to send';
"""


def split_sql_statements(sql: str) -> list[str]:
    """Split on ';' at end of line (the HTTP interface runs ONE statement per request)."""
    statements = []
    for chunk in re.split(r";\s*(?:\n|$)", sql):
        if chunk.strip() != "":
            statements.append(chunk.strip())
    return statements


def apply_ddl(client: Any) -> int:
    count = 0
    for statement in split_sql_statements(DDL_SQL):
        client.command(statement)
        count += 1
    return count


# =============================================================================
# SECTION 6 — run log (ops.pipeline_runs) and incremental watermarks
# =============================================================================
RUN_LOG_COLUMNS = ["batch_id", "task_name", "attempt_id", "status", "started_at", "finished_at", "rows_loaded",
                   "api_calls", "api_errors", "details", "error_message", "updated_at"]


def write_run_log(client: Any, run_log: dict[str, Any]) -> None:
    """Insert the current state as a new row (ReplacingMergeTree keeps the latest)."""
    row = [run_log["batch_id"], run_log["task_name"], run_log["attempt_id"], run_log["status"],
           run_log["started_at"], run_log["finished_at"], run_log["rows_loaded"], run_log["api_calls"],
           run_log["api_errors"], json.dumps(run_log["details"], default=str), run_log["error_message"][:2000],
           utcnow()]
    try:
        client.insert("ops.pipeline_runs", [row], column_names=RUN_LOG_COLUMNS)
    except Exception as exc:  # observability must never break the pipeline
        log.warning("could not write ops.pipeline_runs: %s", exc)


def run_log_start(client: Any, batch_id: str, task_name: str) -> dict[str, Any]:
    run_log = {
        "client": client, "batch_id": batch_id, "task_name": task_name, "status": "running",
        "rows_loaded": 0, "api_calls": 0, "api_errors": 0, "details": {}, "error_message": "",
        "attempt_id": uuid.uuid4(), "started_at": utcnow(), "finished_at": None,
    }
    write_run_log(client, run_log)
    return run_log


def run_log_mark_partial(run_log: dict[str, Any], reason: str) -> None:
    """Finished on purpose without processing everything (e.g. YouTube quota)."""
    run_log["status"] = "partial"
    run_log["details"]["partial_reason"] = reason


def run_log_finish(run_log: dict[str, Any], error: BaseException | None = None) -> None:
    run_log["finished_at"] = utcnow()
    if error is not None:
        run_log["status"] = "failed"
        run_log["error_message"] = f"{type(error).__name__}: {error}"
    elif run_log["status"] == "running":
        run_log["status"] = "success"
    write_run_log(run_log["client"], run_log)
    log.info("[%s] status=%s rows=%d api_calls=%d api_errors=%d details=%s", run_log["task_name"],
             run_log["status"], run_log["rows_loaded"], run_log["api_calls"], run_log["api_errors"],
             run_log["details"])


def get_watermark(client: Any, source: str) -> datetime:
    rows = query_rows(client, "SELECT watermark FROM ops.load_watermarks FINAL WHERE source = {s:String}",
                      {"s": source})
    if not rows:
        return EPOCH
    return as_utc(rows[0]["watermark"])


def set_watermark(client: Any, source: str, watermark: datetime, batch_id: str, rows_processed: int) -> None:
    client.insert("ops.load_watermarks", [[source, watermark, batch_id, rows_processed, utcnow()]],
                  column_names=["source", "watermark", "batch_id", "rows_processed", "updated_at"])


def newest_timestamp(frames: list[pd.DataFrame], column: str, default: datetime) -> datetime:
    newest = default
    for frame in frames:
        for value in frame[column].tolist():
            moment = as_utc(value)
            if moment > newest:
                newest = moment
    return newest


# =============================================================================
# SECTION 7 — HTTP resilience shared by the Spotify and YouTube clients
# -----------------------------------------------------------------------------
#  429                -> retry, honouring Retry-After ...
#  429 + Retry-After > 120 s -> NOT retried: a daily quota is exhausted (Spotify
#                        Development Mode answers QUOTA_EXCEEDED, Retry-After
#                        ~23 h); the caller stops the run as 'partial'
#  5xx / network      -> retry with exponential backoff + jitter
#  other 4xx          -> NOT retried (would waste quota); stored with its status
#  10 failures in a row -> circuit breaker aborts the task (fail fast)
# =============================================================================
RETRYABLE_STATUS = (429, 500, 502, 503, 504)
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRY_AFTER_SECONDS = 120.0
NETWORK_ERROR_STATUS = 599          # synthetic status for connection errors / timeouts


def make_result(status: int, text: str) -> dict[str, Any]:
    return {"status": status, "text": text, "ok": 200 <= status < 300, "retry_after": None}


def error_and_body(result: dict[str, Any]) -> tuple[str, str]:
    """(error_message, response_json) for the raw tables."""
    if result["ok"]:
        return "", result["text"]
    return result["text"][:1000], ""


def backoff_seconds(attempt: int, retry_after: float | None) -> float:
    """Retry-After when given, else 1-2 s, 2-3 s, 4-5 s, 8-9 s ... (cap 30 s)."""
    if retry_after is not None and retry_after > 0:
        return min(retry_after, MAX_RETRY_AFTER_SECONDS)
    return min(2 ** (attempt - 1) + random.uniform(0, 1), 30.0)


def parse_retry_after(header_value: str | None) -> float | None:
    if not header_value:
        return None
    try:
        return float(header_value)
    except ValueError:
        return None


def send_once(session: Any, method: str, url: str, kwargs: dict[str, Any]) -> tuple[dict[str, Any], bool, float | None]:
    """One attempt -> (result, should_retry, retry_after_seconds)."""
    try:
        response = session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)
    except (requests.ConnectionError, requests.Timeout) as exc:
        detail = type(exc).__name__
        return make_result(NETWORK_ERROR_STATUS, f'{{"error": "network", "detail": "{detail}"}}'), True, None
    result = make_result(response.status_code, response.text)
    if response.status_code in RETRYABLE_STATUS:
        return result, True, parse_retry_after(response.headers.get("Retry-After"))
    return result, False, None


def http_request(session: Any, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    """Send with retries; when retries run out the LAST error is returned (not raised)."""
    result = make_result(NETWORK_ERROR_STATUS, '{"error": "network", "detail": "no attempt made"}')
    for attempt in range(1, MAX_ATTEMPTS + 1):
        result, should_retry, retry_after = send_once(session, method, url, kwargs)
        result["retry_after"] = retry_after
        if not should_retry:
            return result
        if retry_after is not None and retry_after > MAX_RETRY_AFTER_SECONDS:
            # a quota measured in hours: waiting inside the task is pointless
            log.warning("HTTP %s asks to retry after %.0fs (> %.0fs): giving up for this run",
                        result["status"], retry_after, MAX_RETRY_AFTER_SECONDS)
            return result
        if attempt == MAX_ATTEMPTS:
            break
        delay = backoff_seconds(attempt, retry_after)
        log.warning("retrying HTTP call (attempt %d of %d, status %s) in %.1fs", attempt, MAX_ATTEMPTS,
                    result["status"], delay)
        time.sleep(delay)
    return result


def new_circuit_breaker(name: str, threshold: int = 10) -> dict[str, Any]:
    return {"name": name, "threshold": threshold, "consecutive_failures": 0}


def circuit_breaker_record(breaker: dict[str, Any], ok: bool) -> None:
    if ok:
        breaker["consecutive_failures"] = 0
    else:
        breaker["consecutive_failures"] = breaker["consecutive_failures"] + 1
    if breaker["consecutive_failures"] >= breaker["threshold"]:
        raise RuntimeError(f"{breaker['name']}: circuit breaker open after "
                           f"{breaker['threshold']} consecutive failed API calls")


def new_throttle(min_interval: float) -> dict[str, Any]:
    return {"min_interval": min_interval, "last_call": 0.0}


def throttle_wait(throttle: dict[str, Any]) -> None:
    delay = throttle["last_call"] + throttle["min_interval"] - time.monotonic()
    if delay > 0:
        time.sleep(delay)
    throttle["last_call"] = time.monotonic()


# =============================================================================
# SECTION 8 — text helpers (search queries + standardisation)
# -----------------------------------------------------------------------------
# The AUTHORITATIVE matching keys (title_base, artist tokens ...) live in dbt
# (dbt/music_dw/macros/normalize.sql). Python only cleans values and builds the
# API search queries.
# =============================================================================
ZERO_WIDTH = re.compile("[​-‏⁠-⁤﻿]")   # e.g. U+2060 in '⁠SEROTONIN 6 AM'
WHITESPACE = re.compile(r"\s+")
VERSION_KEYWORDS = (
    "feat", "ft", "featuring", "remaster", "remastered", "live", "acoustic", "accoustic", "akustik",
    "version", "ver", "versi", "edit", "remix", "mix", "instrumental", "karaoke", "cover", "piano",
    "chill", "unplugged", "demo", "mono", "stereo", "explicit", "clean", "sped up", "slowed", "reverb",
    "from", "ost", "soundtrack", "original", "orchestra", "orkestra",
)


def keywords_regex(keywords: tuple[str, ...]) -> str:
    escaped = []
    for keyword in keywords:
        escaped.append(re.escape(keyword))
    return "|".join(escaped)


VERSION_BRACKET = re.compile(rf"[\(\[][^\)\]]*\b({keywords_regex(VERSION_KEYWORDS)})\b[^\)\]]*[\)\]]", re.IGNORECASE)
VERSION_DASH = re.compile(rf"\s[-–—]\s.*\b({keywords_regex(VERSION_KEYWORDS)})\b.*$", re.IGNORECASE)
ARTIST_SPLIT = re.compile(r"\s*(?:,|&|\bfeat\.?|\bft\.?|\bx\b|\bwith\b)\s*", re.IGNORECASE)
ISRC_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$")         # CC XXX YY NNNNN
SPOTIFY_ID_PATTERN = re.compile(r"^[0-9A-Za-z]{22}$")
YOUTUBE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")
ISO_DURATION = re.compile(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$")


def clean_text(value: Any) -> str:
    """Unicode NFKC, drop invisible characters, collapse whitespace. Keeps case."""
    if is_missing(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = ZERO_WIDTH.sub("", text)
    return WHITESPACE.sub(" ", text).strip()


def clean_text_or_none(value: Any) -> str | None:
    text = clean_text(value)
    if text == "":
        return None
    return text


def clean_html_text(value: Any) -> str:
    """YouTube titles arrive HTML-escaped ('Rock &amp; Roll', '&#39;')."""
    return clean_text(html.unescape(clean_text(value)))


def query_title(value: Any) -> str:
    """Title for API queries: version qualifiers and double quotes removed."""
    text = clean_text(value)
    text = VERSION_BRACKET.sub(" ", text)
    text = VERSION_DASH.sub(" ", text)
    text = WHITESPACE.sub(" ", text.replace('"', " ")).strip()
    if text:
        return text
    return clean_text(value).replace('"', " ")      # '() BATTESIMO' would become empty


def split_artists(value: Any) -> list[str]:
    """'ALSA, CHRIS ANDRIAN YANG' -> ['ALSA', 'CHRIS ANDRIAN YANG']."""
    text = clean_text(value)
    if not text:
        return []
    artists = []
    for part in ARTIST_SPLIT.split(text):
        if part and part.strip():
            artists.append(part.strip().replace('"', " "))
    return artists


def normalize_isrc(value: Any) -> str | None:
    """'id-a12-23-00001' -> 'IDA122300001'; missing -> None."""
    if is_missing(value):
        return None
    text = re.sub(r"[^A-Za-z0-9]", "", str(value)).upper()
    if text == "":
        return None
    return text


def is_valid_isrc(value: Any) -> bool:
    return isinstance(value, str) and ISRC_PATTERN.match(value) is not None


def is_spotify_track_id(value: Any) -> bool:
    return isinstance(value, str) and SPOTIFY_ID_PATTERN.match(value) is not None


def is_youtube_video_id(value: Any) -> bool:
    return isinstance(value, str) and YOUTUBE_ID_PATTERN.match(value) is not None


def iso_duration_seconds(value: Any) -> int:
    """'PT1H2M3S' -> 3723; anything else -> 0."""
    match = ISO_DURATION.match(clean_text(value))
    if match is None:
        return 0
    multipliers = (86400, 3600, 60, 1)
    total = 0
    for index in range(4):
        part = match.group(index + 1)
        if part:
            total += int(part) * multipliers[index]
    return total


def parse_utc_timestamp(value: Any) -> datetime | None:
    """'2019-05-01T10:00:00Z' -> aware UTC datetime; garbage -> None."""
    text = clean_text(value)
    if text == "":
        return None
    try:
        return as_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def parse_release_date(value: Any, precision: Any) -> date | None:
    """Spotify gives 'YYYY', 'YYYY-MM' or 'YYYY-MM-DD' depending on precision."""
    text = clean_text(value)
    if text == "" or text.startswith("0000"):
        return None
    if len(text) == 4:
        text = text + "-01-01"
    elif len(text) == 7:
        text = text + "-01"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


# =============================================================================
# SECTION 9 — UPSERT + SCD Type 2 engine (pandas decides, ClickHouse stores)
# -----------------------------------------------------------------------------
# ClickHouse has no in-place UPDATE for OLAP tables; the idiomatic upsert is a
# ReplacingMergeTree(updated_at): INSERT a newer state of a row with the same
# ORDER BY key and the newest state wins (merge time, or FINAL at read time).
#
# SCD Type 2 on top of it, key = (business key, version_no):
#   new key           -> INSERT version 1 (or last version + 1 if it came back)
#   tracked change    -> re-INSERT the open version closed (valid_to = now,
#                        is_current = 0) + INSERT version n+1 (open)
#   type-1 change     -> re-INSERT the open version with the new values
#   deleted at source -> re-INSERT the open version closed with is_deleted = 1
#   unchanged         -> nothing is written (incremental by construction)
# =============================================================================
SCD_COLUMNS = ["record_hash", "valid_from", "valid_to", "is_current", "is_deleted", "batch_id", "updated_at"]


def scd_spec(table: str, key: str, key_type: str, tracked: list[str], type1: list[str],
             full_snapshot: bool) -> dict[str, Any]:
    """full_snapshot=True: the incoming frame is the WHOLE source, so missing keys were deleted."""
    return {
        "table": table, "key": key, "key_type": key_type, "tracked": tracked, "type1": type1,
        "full_snapshot": full_snapshot,
        "columns": [key, "version_no"] + tracked + type1 + SCD_COLUMNS,
    }


def canonical_value(value: Any) -> Any:
    """Stable, JSON-friendly form of a value (for hashing and comparisons)."""
    value = clickhouse_value(value)
    if isinstance(value, datetime):
        return as_utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, list):
        items = []
        for item in value:
            items.append(canonical_value(item))
        return items
    return value


def tracked_hash(row: pd.Series, columns: list[str]) -> str:
    values = []
    for column in columns:
        values.append(canonical_value(row[column]))
    payload = json.dumps(values, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def type1_differs(row: pd.Series, columns: list[str]) -> bool:
    for column in columns:
        if canonical_value(row[column]) != canonical_value(row[column + "__current"]):
            return True
    return False


def fill_column(frame: pd.DataFrame, column: str, value: Any) -> None:
    """Set a constant column as plain Python objects. pandas would turn a
    datetime into datetime64[ns], which cannot hold OPEN_END (2299-12-31)."""
    frame[column] = pd.Series([value] * len(frame), index=frame.index, dtype=object)


def coerce_key(frame: pd.DataFrame, key: str, key_type: str) -> pd.DataFrame:
    """Same dtype on both sides of a merge (int64 vs object would refuse to merge)."""
    frame = frame.copy()
    if key_type == "int":
        frame[key] = frame[key].astype("int64")
    else:
        frame[key] = frame[key].astype(str)
    return frame


def plan_scd2(current: pd.DataFrame, last_versions: pd.DataFrame, incoming: pd.DataFrame,
              spec: dict[str, Any], now: datetime, batch_id: str, deleted_keys: list | None = None,
              max_delete_ratio: float = 1.0) -> dict[str, Any]:
    """PURE function (no I/O): which row states must be INSERTED.

    current       open versions (is_current = 1) with every spec column
    last_versions key + last_version_no for every key ever seen (closed too)
    incoming      one row per key: key + tracked + type-1 columns
    deleted_keys  keys the source reported as deleted (non-snapshot sources)
    """
    key = spec["key"]
    tracked = spec["tracked"]
    type1 = spec["type1"]
    columns = spec["columns"]
    current = coerce_key(current, key, spec["key_type"])
    last_versions = coerce_key(last_versions, key, spec["key_type"])
    incoming = coerce_key(incoming, key, spec["key_type"])
    if incoming[key].duplicated().any():
        raise ValueError(f"{spec['table']}: incoming rows must be unique per {key}")

    incoming["record_hash"] = ""
    if len(incoming) > 0:
        incoming["record_hash"] = incoming.apply(tracked_hash, axis=1, args=(tracked,))

    last_version_by_key = dict(zip(last_versions[key].tolist(), last_versions["last_version_no"].tolist()))
    current_keys = set(current[key].tolist())
    incoming_keys = set(incoming[key].tolist())
    is_known = incoming[key].isin(current_keys)

    # 1) keys without an open version: version 1, or last version + 1 when a deleted key comes back
    new_rows = incoming[~is_known][[key] + tracked + type1 + ["record_hash"]].copy()
    versions = []
    for value in new_rows[key].tolist():
        versions.append(int(last_version_by_key.get(value, 0)) + 1)
    new_rows["version_no"] = pd.Series(versions, index=new_rows.index, dtype=object)

    # 2) known keys: compare the tracked-attribute hash with the open version
    compare = incoming[is_known].merge(current, on=key, how="left", suffixes=("", "__current"))
    changed = compare[compare["record_hash"] != compare["record_hash__current"]]
    unchanged = compare[compare["record_hash"] == compare["record_hash__current"]]

    changed_keys = set(changed[key].tolist())
    closed_rows = current[current[key].isin(changed_keys)].copy()
    fill_column(closed_rows, "valid_to", now)
    fill_column(closed_rows, "is_current", 0)

    next_rows = changed[[key] + tracked + type1 + ["record_hash"]].copy()
    next_versions = []
    for value in changed["version_no"].tolist():
        next_versions.append(int(value) + 1)
    next_rows["version_no"] = pd.Series(next_versions, index=next_rows.index, dtype=object)

    # 3) unchanged tracked attributes but different type-1 values: overwrite the open version
    type1_rows = unchanged.iloc[0:0]
    if len(type1) > 0 and len(unchanged) > 0:
        type1_mask = unchanged.apply(type1_differs, axis=1, args=(type1,)).astype(bool)
        type1_rows = unchanged[type1_mask]
    type1_rows = type1_rows[[key, "version_no"] + tracked + type1 + ["record_hash", "valid_from", "valid_to",
                                                                      "is_current", "is_deleted", "batch_id"]].copy()

    # 4) deletions
    if spec["full_snapshot"]:
        gone = current[~current[key].isin(incoming_keys)].copy()
        if len(current) > 0 and len(gone) / len(current) > max_delete_ratio:
            raise RuntimeError(
                f"SCD2 guard on {spec['table']}: {len(gone)} of {len(current)} current rows would be closed as "
                f"deleted (> {max_delete_ratio:.0%}). Refusing to load - check the source before re-running.")
    else:
        if deleted_keys is None:
            deleted_keys = []
        gone = current[current[key].isin(set(coerce_key(pd.DataFrame({key: deleted_keys}), key,
                                                         spec["key_type"])[key].tolist()))].copy()
    fill_column(gone, "valid_to", now)
    fill_column(gone, "is_current", 0)
    fill_column(gone, "is_deleted", 1)

    # assemble every row state to INSERT
    for frame in (new_rows, next_rows):
        fill_column(frame, "valid_from", now)
        fill_column(frame, "valid_to", OPEN_END)
        fill_column(frame, "is_current", 1)
        fill_column(frame, "is_deleted", 0)
        fill_column(frame, "batch_id", batch_id)
    parts = []
    for frame in (new_rows, closed_rows, next_rows, type1_rows, gone):
        if len(frame) > 0:
            frame = frame.copy()
            fill_column(frame, "updated_at", now)
            parts.append(frame[columns])
    if parts:
        rows = pd.concat(parts, ignore_index=True)
    else:
        rows = pd.DataFrame(columns=columns)
    return {
        "rows": rows,
        "inserted": len(new_rows),
        "updated": len(next_rows),
        "type1_updated": len(type1_rows),
        "deleted": len(gone),
        "unchanged": len(unchanged) - len(type1_rows),
    }


def scd2_upsert(client: Any, spec: dict[str, Any], incoming: pd.DataFrame, batch_id: str,
                deleted_keys: list | None = None) -> dict[str, int]:
    """Read the open versions, plan with pandas, INSERT the new row states."""
    current = query_df(client, f"FROM {spec['table']} FINAL WHERE is_current = 1", spec["columns"])
    last_versions = query_df(client, f"FROM (SELECT {spec['key']}, max(version_no) AS last_version_no "
                                     f"FROM {spec['table']} FINAL GROUP BY {spec['key']})",
                             [spec["key"], "last_version_no"])
    plan = plan_scd2(current, last_versions, incoming, spec, utcnow(), batch_id, deleted_keys,
                     get_settings()["pipeline"]["scd_max_delete_ratio"])
    written = insert_df(client, spec["table"], plan["rows"], spec["columns"])
    stats = {"rows_written": written}
    for name in ("inserted", "updated", "type1_updated", "deleted", "unchanged"):
        stats[name] = plan[name]
    log.info("SCD2 %s: %s", spec["table"], stats)
    return stats


def upsert(client: Any, table: str, frame: pd.DataFrame, columns: list[str]) -> int:
    """Plain upsert (no history): newest row state per ORDER BY key wins."""
    if len(frame) == 0:
        return 0
    frame = frame.copy()
    frame["updated_at"] = utcnow()
    return insert_df(client, table, frame, columns + ["updated_at"])


# =============================================================================
# SECTION 10 — quarantine: rejected records are kept with a reason
# =============================================================================
QUARANTINE_COLUMNS = ["batch_id", "detected_at", "source", "entity", "record_key", "record_label",
                      "reject_reason", "severity", "record_json"]


def quarantine_frame(rejected: pd.DataFrame, source: str, entity: str, key_column: str, label_column: str,
                     batch_id: str, severity: str = "warn") -> pd.DataFrame:
    """One quarantine row per rejected record (rejected needs a reject_reason column)."""
    detected_at = utcnow()
    records = []
    for row in rejected.to_dict("records"):
        payload = {}
        for column, value in row.items():
            payload[column] = canonical_value(value)
        records.append({
            "batch_id": batch_id,
            "detected_at": detected_at,
            "source": source,
            "entity": entity,
            "record_key": clean_text(row.get(key_column)),
            "record_label": clean_text(row.get(label_column))[:300],
            "reject_reason": row["reject_reason"],
            "severity": severity,
            "record_json": json.dumps(payload, ensure_ascii=False, default=str),
        })
    return pd.DataFrame(records, columns=QUARANTINE_COLUMNS)


def write_quarantine(client: Any, frame: pd.DataFrame) -> int:
    written = insert_df(client, "dq.quarantine_records", frame, QUARANTINE_COLUMNS)
    if written:
        log.warning("quarantined %d records: %s", written, frame["reject_reason"].value_counts().to_dict())
    return written


def count_reasons(frame: pd.DataFrame) -> dict[str, int]:
    counts = {}
    if len(frame) == 0:
        return counts
    for reason, number in frame["reject_reason"].value_counts().items():
        counts[str(reason)] = int(number)
    return counts


# =============================================================================
# SECTION 11 — catalog: Google-Sheet export -> pandas -> quarantine / SCD2
# =============================================================================
CATALOG_REQUIRED_HEADERS = ["CODE", "ORIGINAL ARTIST", "SONG TITLE"]
CATALOG_SCD = scd_spec("clean.catalog_songs", key="song_code", key_type="int",
                       tracked=["song_title", "original_artist"], type1=["source_row_number"],
                       full_snapshot=True)


def read_catalog_sheet(cfg: dict[str, Any]) -> tuple[pd.DataFrame, str]:
    """The '[DE TS] Songs Data' sheet as a DataFrame (cells untouched)."""
    if cfg["source"] == "gsheet_csv":
        # works for sheets shared as "anyone with the link can view"
        url = (f"https://docs.google.com/spreadsheets/d/{cfg['gsheet_id']}/gviz/tq"
               f"?tqx=out:csv&sheet={quote(cfg['sheet_name'])}")
        return pd.read_csv(url, dtype=object), f"gsheet:{cfg['gsheet_id']}#{cfg['sheet_name']}"
    path = Path(cfg["file_path"])
    if path.suffix.lower() in (".xlsx", ".xlsm"):
        sheet = pd.read_excel(path, sheet_name=cfg["sheet_name"], dtype=object, engine="openpyxl")
    else:
        sheet = pd.read_csv(path, dtype=object, encoding="utf-8-sig")
    return sheet, f"{path.name}#{cfg['sheet_name']}"


def cell_to_text(value: Any) -> str | None:
    """Excel numbers arrive as floats: 13809.0 -> '13809'; empty -> None."""
    if is_missing(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def parse_song_code(value: Any) -> int | None:
    """'13809' -> 13809; non-numeric or <= 0 -> None (UInt32 business key)."""
    text = clean_text(value)
    if not text.isdigit():
        return None
    number = int(text)
    if number <= 0 or number > 4294967295:
        return None
    return number


def catalog_label(row: dict[str, Any]) -> str:
    """Readable identification of a catalog row: 'KEMANA — KOES PLUS', '(no title)'."""
    title = clean_text(row["song_title"])
    if title == "":
        title = "(no title)"
    artist = clean_text(row["original_artist"])
    if artist == "":
        return title
    return title + " — " + artist


def preprocess_catalog(sheet: pd.DataFrame) -> dict[str, Any]:
    """PURE: raw sheet -> {'clean': valid songs, 'rejected': rows with reject_reason}.

    Standardise : NFKC, invisible characters removed, whitespace collapsed,
                  Excel floats -> text, blank artist -> NULL
    Validate    : missing_code, non_numeric_code, missing_title, duplicate_code
                  (first rule that fails wins; the first occurrence of a code is kept)
    """
    headers = {}
    for column in sheet.columns:
        headers[column] = str(column).strip().upper()
    sheet = sheet.rename(columns=headers)
    missing = []
    for header in CATALOG_REQUIRED_HEADERS:
        if header not in sheet.columns:
            missing.append(header)
    if missing:
        # schema contract with the business: fail loudly when columns are renamed
        raise ValueError(f"Catalog is missing required columns {missing}; got {list(sheet.columns)}")

    frame = pd.DataFrame(index=sheet.index)
    frame["source_row_number"] = pd.Series(range(1, len(sheet) + 1), index=sheet.index)
    frame["code_text"] = sheet["CODE"].map(cell_to_text).map(clean_text_or_none)
    frame["song_title"] = sheet["SONG TITLE"].map(cell_to_text).map(clean_text_or_none)
    frame["original_artist"] = sheet["ORIGINAL ARTIST"].map(cell_to_text).map(clean_text_or_none)
    empty_row = frame["code_text"].isna() & frame["song_title"].isna() & frame["original_artist"].isna()
    frame = frame[~empty_row].copy()
    frame["song_code"] = frame["code_text"].map(parse_song_code).astype("Int64")

    frame["reject_reason"] = pd.Series([None] * len(frame), index=frame.index, dtype=object)
    rule = frame["code_text"].isna()
    frame.loc[rule, "reject_reason"] = "missing_code"
    rule = frame["reject_reason"].isna() & frame["song_code"].isna()
    frame.loc[rule, "reject_reason"] = "non_numeric_code"
    rule = frame["reject_reason"].isna() & frame["song_title"].isna()
    frame.loc[rule, "reject_reason"] = "missing_title"
    still_valid = frame[frame["reject_reason"].isna()]
    duplicated = still_valid["song_code"].duplicated(keep="first")
    frame.loc[duplicated[duplicated].index, "reject_reason"] = "duplicate_code"

    labels = []
    for row in frame.to_dict("records"):
        labels.append(catalog_label(row))
    frame["record_label"] = pd.Series(labels, index=frame.index, dtype=object)
    frame["record_key"] = frame["code_text"].map(clean_text)
    no_code = frame["record_key"] == ""
    frame.loc[no_code, "record_key"] = "row " + frame.loc[no_code, "source_row_number"].astype(str)

    valid = frame[frame["reject_reason"].isna()]
    clean = valid[["song_code", "song_title", "original_artist", "source_row_number"]].copy()
    rejected = frame[frame["reject_reason"].notna()].copy()
    return {"clean": clean, "rejected": rejected, "rows_read": len(frame)}


def run_load_catalog(batch_id: str) -> dict[str, Any]:
    settings = get_settings()
    client = get_client()
    run_log = run_log_start(client, batch_id, "load_catalog")
    try:
        sheet, source_ref = read_catalog_sheet(settings["catalog"])
        result = preprocess_catalog(sheet)
        quarantined = write_quarantine(client, quarantine_frame(result["rejected"], "catalog", "catalog_song",
                                                                "record_key", "record_label", batch_id))
        if len(result["clean"]) == 0:
            # an empty sheet is far more likely a broken export than a business decision
            raise ValueError("Catalog has 0 valid rows - refusing to close every song")
        stats = scd2_upsert(client, CATALOG_SCD, result["clean"], batch_id)
        run_log["rows_loaded"] = stats["rows_written"]
        details = run_log["details"]
        details["source_ref"] = source_ref
        details["rows_read"] = result["rows_read"]
        details["valid_songs"] = len(result["clean"])
        details["quarantined"] = quarantined
        details["quarantine_reasons"] = count_reasons(result["rejected"])
        for name in ("inserted", "updated", "type1_updated", "deleted", "unchanged"):
            details["scd2_" + name] = stats[name]
    except BaseException as exc:      # BaseException: timeouts are recorded too
        run_log_finish(run_log, exc)
        raise
    run_log_finish(run_log)
    return dict(run_log["details"])


# =============================================================================
# SECTION 12 — incremental song selection (which songs go to the APIs?)
# -----------------------------------------------------------------------------
#  current catalog (clean.catalog_songs, open versions)
#    LEFT JOIN last successful search (raw responses = source of truth)
#  due when: never searched | older than REFRESH_DAYS | the song changed after
#            its last search (new SCD2 version, e.g. corrected title)
#  order  : never searched first, then the stalest (fair use of the quota)
# =============================================================================
SEARCH_DONE = {
    "spotify": ("raw.spotify_search_responses", "query_type = 'title_only' AND page_offset = 0 AND http_status = 200"),
    "youtube": ("raw.youtube_search_responses", "http_status = 200"),
}


def select_due_songs(client: Any, api: str, refresh_days: int, limit: int) -> list[dict[str, Any]]:
    table, done_predicate = SEARCH_DONE[api]
    limit_clause = ""
    if limit > 0:
        limit_clause = "LIMIT {limit:UInt32}"
    sql = f"""
        WITH
            catalog AS (
                SELECT song_code, song_title, original_artist, source_row_number, valid_from
                FROM clean.catalog_songs FINAL
                WHERE is_current = 1 AND is_deleted = 0
            ),
            done AS (
                SELECT song_code, max(ingested_at) AS last_ok
                FROM {table}
                WHERE {done_predicate}
                GROUP BY song_code
            )
        SELECT c.song_code AS code, c.song_title AS title, c.original_artist AS artist
        FROM catalog AS c
        LEFT JOIN done AS d ON d.song_code = c.song_code
        WHERE d.last_ok IS NULL
           OR d.last_ok < now64(3) - toIntervalDay({{days:UInt32}})
           OR c.valid_from > d.last_ok
        ORDER BY d.last_ok ASC NULLS FIRST, c.source_row_number
        {limit_clause}
        SETTINGS join_use_nulls = 1
    """
    rows = query_rows(client, sql, {"days": refresh_days, "limit": limit})
    songs = []
    for row in rows:
        songs.append({"code": int(row["code"]), "title": row["title"], "artist": row["artist"]})
    return songs


# =============================================================================
# SECTION 13 — Spotify: extract (Web API, Client Credentials) -> raw
# -----------------------------------------------------------------------------
# Per song: title_artist query  track:"<title>" artist:"<first artist>"  (precision)
#           title_only   query  track:"<title>"                          (covers = recall)
# Paging stops on an EMPTY page, or on a page without a single track whose name
# contains the searched title (the rest is noise; it saves quota). `next` and
# `total` are NOT used: in Development Mode they are unreliable (the market
# filter returned 7 items on page 1 and 10 on page 2 of the same search).
# Search hits without external_ids.isrc are looked up with GET /v1/tracks/{id}.
# Development Mode also has a DAILY QUOTA: HTTP 429 QUOTA_EXCEEDED with
# Retry-After ~23 h. The run then stops as 'partial'; the next run continues.
# =============================================================================
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API = "https://api.spotify.com/v1"
SPOTIFY_SEARCH_COLUMNS = ["request_id", "batch_id", "song_code", "query_type", "query_text", "market",
                          "page_offset", "page_limit", "http_status", "error_message", "response_json"]
SPOTIFY_TRACK_COLUMNS = ["request_id", "batch_id", "spotify_track_id", "http_status", "error_message",
                         "response_json"]


def new_spotify_client(cfg: dict[str, Any]) -> dict[str, Any]:
    return {"cfg": cfg, "session": requests.Session(), "throttle": new_throttle(cfg["min_interval_seconds"]),
            "token": None, "token_expires_at": 0.0}


def spotify_refresh_token(client: dict[str, Any]) -> None:
    cfg = client["cfg"]
    basic = base64.b64encode(f"{cfg['client_id']}:{cfg['client_secret']}".encode()).decode()
    result = http_request(client["session"], "POST", SPOTIFY_TOKEN_URL, data={"grant_type": "client_credentials"},
                          headers={"Authorization": f"Basic {basic}"})
    if not result["ok"]:
        # 400/401 = wrong credentials or app not allowed: nothing else can work
        raise RuntimeError(f"Spotify token request failed: HTTP {result['status']} {result['text'][:200]}")
    payload = json.loads(result["text"])
    client["token"] = payload["access_token"]
    client["token_expires_at"] = time.time() + int(payload.get("expires_in", 3600)) - 60


def spotify_get(client: dict[str, Any], url: str, params: dict[str, Any]) -> dict[str, Any]:
    if not client["token"] or time.time() >= client["token_expires_at"]:
        spotify_refresh_token(client)
    throttle_wait(client["throttle"])
    result = http_request(client["session"], "GET", url, params=params,
                          headers={"Authorization": f"Bearer {client['token']}"})
    if result["status"] == 401:            # token revoked / expired early: refresh once
        spotify_refresh_token(client)
        throttle_wait(client["throttle"])
        result = http_request(client["session"], "GET", url, params=params,
                              headers={"Authorization": f"Bearer {client['token']}"})
    return result


def spotify_plan_queries(song: dict[str, Any], cfg: dict[str, Any]) -> list[tuple[str, str, int]]:
    """[(query_type, q, pages)] for one song."""
    title = query_title(song["title"])
    plan = []
    artists = split_artists(song["artist"])
    if artists:
        plan.append(("title_artist", f'track:"{title}" artist:"{artists[0]}"', max(cfg["artist_query_pages"], 1)))
    plan.append(("title_only", f'track:"{title}"', max(cfg["max_pages"], 1)))
    return plan


def compare_key(value: Any) -> str:
    """Loose comparison key: lower case, punctuation -> space ('Kemana-Mana' -> 'kemana mana')."""
    text = re.sub(r"[^\w]+", " ", clean_text(value).lower())
    return WHITESPACE.sub(" ", text).strip()


def spotify_page_info(response_text: str, title_key: str) -> dict[str, Any]:
    """Counts of one search page: items, items whose name contains the searched
    title, and ids of items without external_ids.isrc."""
    tracks = json.loads(response_text).get("tracks") or {}
    info = {"items": 0, "title_hits": 0, "missing_isrc": []}
    for item in tracks.get("items") or []:
        if not item:
            continue                                  # Spotify can return null items
        info["items"] += 1
        if title_key and title_key in compare_key(item.get("name")):
            info["title_hits"] += 1
        external_ids = item.get("external_ids") or {}
        if not external_ids.get("isrc") and item.get("id"):
            info["missing_isrc"].append(item["id"])
    return info


def spotify_call(state: dict[str, Any], url: str, params: dict[str, Any]) -> dict[str, Any] | None:
    """One API call. None = HTTP 429 (daily quota / rate limit): the run must stop."""
    result = spotify_get(state["api"], url, params)
    if result["status"] == 429:
        retry_after = result["retry_after"]
        if retry_after is None:
            retry_after = 0
        state["stop_reason"] = f"Spotify rate limit / daily quota (HTTP 429, retry after {int(retry_after)} s)"
        return None
    state["run_log"]["api_calls"] += 1
    if not result["ok"]:
        state["run_log"]["api_errors"] += 1
    return result


def spotify_extract_song(state: dict[str, Any], song: dict[str, Any]) -> None:
    """Search pages for one song (+ ISRC fallback lookups) -> raw buffers."""
    cfg = state["cfg"]
    details = state["run_log"]["details"]
    title_key = compare_key(query_title(song["title"]))
    missing_isrc = set()
    for query_type, query, pages in spotify_plan_queries(song, cfg):
        for page in range(pages):
            offset = page * cfg["search_limit"]
            result = spotify_call(state, f"{SPOTIFY_API}/search", {
                "q": query, "type": "track", "market": cfg["market"], "limit": cfg["search_limit"], "offset": offset})
            if result is None:
                return                                # quota exhausted: the song is searched next run
            error_message, response_json = error_and_body(result)
            buffer_add(state["search_buffer"], {
                "request_id": uuid.uuid4(), "batch_id": state["batch_id"], "song_code": song["code"],
                "query_type": query_type, "query_text": query, "market": cfg["market"], "page_offset": offset,
                "page_limit": cfg["search_limit"], "http_status": result["status"], "error_message": error_message,
                "response_json": response_json})
            circuit_breaker_record(state["breaker"], result["ok"])
            if not result["ok"]:
                break                                 # no paging after an error
            info = spotify_page_info(result["text"], title_key)
            details["hits"] += info["items"]
            for track_id in info["missing_isrc"]:
                missing_isrc.add(track_id)
            if info["items"] == 0 or info["title_hits"] == 0:
                break                                 # empty or irrelevant page: the next ones are noise
    for track_id in sorted(missing_isrc):
        result = spotify_call(state, f"{SPOTIFY_API}/tracks/{track_id}", {"market": cfg["market"]})
        if result is None:
            return
        details["isrc_lookups"] += 1
        error_message, response_json = error_and_body(result)
        buffer_add(state["track_buffer"], {
            "request_id": uuid.uuid4(), "batch_id": state["batch_id"], "spotify_track_id": track_id,
            "http_status": result["status"], "error_message": error_message, "response_json": response_json})
        circuit_breaker_record(state["breaker"], result["ok"])


def run_extract_spotify(batch_id: str, max_songs: int) -> dict[str, Any]:
    settings = get_settings()
    cfg = settings["spotify"]
    client = get_client()
    songs = select_due_songs(client, "spotify", cfg["refresh_days"], max_songs)
    log.info("spotify: %d songs due", len(songs))

    run_log = run_log_start(client, batch_id, "extract_spotify")
    state = {
        "api": new_spotify_client(cfg), "cfg": cfg, "batch_id": batch_id, "run_log": run_log,
        "breaker": new_circuit_breaker("spotify"), "stop_reason": "",
        "search_buffer": new_insert_buffer(client, "raw.spotify_search_responses", SPOTIFY_SEARCH_COLUMNS),
        "track_buffer": new_insert_buffer(client, "raw.spotify_track_responses", SPOTIFY_TRACK_COLUMNS),
    }
    details = run_log["details"]
    details["songs_due"] = len(songs)
    details["songs_processed"] = 0
    details["songs_deferred"] = 0
    details["hits"] = 0
    details["isrc_lookups"] = 0
    try:
        try:
            for song in songs:
                spotify_extract_song(state, song)
                if state["stop_reason"]:
                    break
                details["songs_processed"] += 1
            if state["stop_reason"]:
                details["songs_deferred"] = len(songs) - details["songs_processed"]
                run_log_mark_partial(run_log, state["stop_reason"])
                log.warning("spotify: stopped early (%s); %d songs deferred to the next run", state["stop_reason"],
                            details["songs_deferred"])
            run_log["rows_loaded"] = (buffer_rows_accepted(state["search_buffer"])
                                      + buffer_rows_accepted(state["track_buffer"]))
        finally:
            flush_all_buffers([state["track_buffer"], state["search_buffer"]])      # keep partial progress
    except BaseException as exc:
        run_log_finish(run_log, exc)
        raise
    run_log_finish(run_log)
    summary = dict(details)
    summary["api_calls"] = run_log["api_calls"]
    summary["api_errors"] = run_log["api_errors"]
    return summary


# =============================================================================
# SECTION 14 — Spotify: preprocess raw -> pandas -> clean (upsert + SCD2)
# =============================================================================
SPOTIFY_TRACK_SCD = scd_spec(
    "clean.spotify_tracks", key="spotify_track_id", key_type="str",
    tracked=["track_name", "isrc", "artist_names", "artist_ids", "album_id", "album_name", "album_type",
             "release_date", "release_date_precision", "duration_ms", "is_explicit"],
    type1=["track_number", "disc_number", "isrc_source"], full_snapshot=False)
SPOTIFY_STATUS_COLUMNS = ["song_code", "last_batch_id", "last_searched_at", "calls", "failed_calls", "is_success",
                          "hit_count"]
SPOTIFY_HIT_COLUMNS = ["song_code", "spotify_track_id", "best_search_rank", "found_by_artist_query",
                       "found_by_queries", "search_batch_id", "last_seen_at"]


def frame_column(frame: pd.DataFrame, name: str) -> pd.Series:
    """Column of a json_normalize() frame, or all-None when no record had the field."""
    if name in frame.columns:
        return frame[name]
    return pd.Series([None] * len(frame), index=frame.index, dtype=object)


def latest_batch_by_song(rows: pd.DataFrame) -> pd.DataFrame:
    """song_code -> batch of its most recent call."""
    newest = rows.sort_values("ingested_at").drop_duplicates("song_code", keep="last")
    return newest[["song_code", "batch_id"]].rename(columns={"batch_id": "last_batch_id"})


def spotify_search_items(searches: pd.DataFrame) -> pd.DataFrame:
    """Explode successful search responses: one row per (call, returned track)."""
    records = []
    for row in searches.to_dict("records"):
        if int(row["http_status"]) != 200:
            continue
        tracks = json.loads(row["response_json"]).get("tracks") or {}
        position = 0
        for item in tracks.get("items") or []:
            position += 1
            if not item:
                continue                         # Spotify can return null items
            records.append({
                "song_code": row["song_code"], "batch_id": row["batch_id"], "ingested_at": row["ingested_at"],
                "query_type": row["query_type"], "search_rank": int(row["page_offset"]) + position,
                "spotify_track_id": clean_text(item.get("id")), "item": item})
    return pd.DataFrame(records, columns=["song_code", "batch_id", "ingested_at", "query_type", "search_rank",
                                          "spotify_track_id", "item"])


def spotify_search_status(searches: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """One row per song from the song's most recent search batch."""
    latest = latest_batch_by_song(searches)
    rows = searches.merge(latest, left_on=["song_code", "batch_id"], right_on=["song_code", "last_batch_id"])
    rows["is_ok"] = rows["http_status"] == 200
    status = rows.groupby("song_code", as_index=False).agg(
        last_batch_id=("batch_id", "first"), last_searched_at=("ingested_at", "max"),
        calls=("request_id", "count"), ok_calls=("is_ok", "sum"))
    status["failed_calls"] = status["calls"] - status["ok_calls"]
    status["is_success"] = (status["ok_calls"] > 0).astype(int)
    latest_items = items.merge(latest, left_on=["song_code", "batch_id"], right_on=["song_code", "last_batch_id"])
    hit_counts = latest_items.groupby("song_code").size().rename("hit_count").reset_index()
    status = status.merge(hit_counts, on="song_code", how="left")
    status["hit_count"] = status["hit_count"].fillna(0).astype(int)
    return status


def spotify_hits(items: pd.DataFrame, latest: pd.DataFrame) -> pd.DataFrame:
    """(song, track) candidates of each song's most recent search batch, de-duplicated."""
    rows = items.merge(latest, left_on=["song_code", "batch_id"], right_on=["song_code", "last_batch_id"])
    rows = rows[rows["spotify_track_id"] != ""]
    hits = rows.groupby(["song_code", "spotify_track_id"], as_index=False).agg(
        best_search_rank=("search_rank", "min"), last_seen_at=("ingested_at", "max"),
        search_batch_id=("batch_id", "first"))
    queries_by_pair = {}
    for row in rows[["song_code", "spotify_track_id", "query_type"]].itertuples(index=False):
        pair = (row.song_code, row.spotify_track_id)
        if pair not in queries_by_pair:
            queries_by_pair[pair] = set()
        queries_by_pair[pair].add(row.query_type)
    found_by = []
    by_artist = []
    for row in hits[["song_code", "spotify_track_id"]].itertuples(index=False):
        queries = sorted(queries_by_pair[(row.song_code, row.spotify_track_id)])
        found_by.append(queries)
        by_artist.append(int("title_artist" in queries))
    hits["found_by_queries"] = pd.Series(found_by, index=hits.index, dtype=object)
    hits["found_by_artist_query"] = pd.Series(by_artist, index=hits.index)
    return hits


def isrc_from_lookups(lookups: pd.DataFrame) -> dict[str, str]:
    """track id -> ISRC from the fallback GET /v1/tracks/{id} calls (latest wins)."""
    isrc_by_track = {}
    for row in lookups.sort_values("ingested_at").to_dict("records"):
        if int(row["http_status"]) != 200:
            continue
        external_ids = json.loads(row["response_json"]).get("external_ids") or {}
        if external_ids.get("isrc"):
            isrc_by_track[row["spotify_track_id"]] = external_ids["isrc"]
    return isrc_by_track


def spotify_artist_names(artists: Any) -> list[str]:
    names = []
    if isinstance(artists, list):
        for artist in artists:
            if isinstance(artist, dict) and clean_text(artist.get("name")):
                names.append(clean_text(artist.get("name")))
    return names


def spotify_artist_ids(artists: Any) -> list[str]:
    ids = []
    if isinstance(artists, list):
        for artist in artists:
            if isinstance(artist, dict) and clean_text(artist.get("id")):
                ids.append(clean_text(artist.get("id")))
    return ids


def release_date_of(row: pd.Series) -> date | None:
    return parse_release_date(row["release_date_raw"], row["release_date_precision"])


def spotify_tracks_frame(items: pd.DataFrame, isrc_by_track: dict[str, str]) -> pd.DataFrame:
    """PURE: one standardised + validated row per Spotify track id.

    Standardise: names NFKC-cleaned, ISRC upper-cased without separators,
                 release date by precision (YYYY -> YYYY-01-01), numbers typed
    Validate   : invalid_track_id, missing_isrc, invalid_isrc_format
    """
    columns = ["spotify_track_id", "track_name", "artist_names", "artist_ids", "album_id", "album_name",
               "album_type", "release_date_raw", "release_date_precision", "release_date", "duration_ms",
               "is_explicit", "track_number", "disc_number", "isrc_raw", "isrc", "isrc_source", "song_codes",
               "reject_reason"]
    if len(items) == 0:
        return pd.DataFrame(columns=columns)
    songs_by_track = {}
    for row in items[["spotify_track_id", "song_code"]].itertuples(index=False):
        if row.spotify_track_id not in songs_by_track:
            songs_by_track[row.spotify_track_id] = set()
        songs_by_track[row.spotify_track_id].add(int(row.song_code))

    latest = items.sort_values("ingested_at").drop_duplicates("spotify_track_id", keep="last")
    flat = pd.json_normalize(latest["item"].tolist())
    flat.index = latest.index
    tracks = pd.DataFrame(index=latest.index)
    tracks["spotify_track_id"] = latest["spotify_track_id"]
    tracks["track_name"] = frame_column(flat, "name").map(clean_text)
    tracks["artist_names"] = frame_column(flat, "artists").map(spotify_artist_names)
    tracks["artist_ids"] = frame_column(flat, "artists").map(spotify_artist_ids)
    tracks["album_id"] = frame_column(flat, "album.id").map(clean_text)
    tracks["album_name"] = frame_column(flat, "album.name").map(clean_text)
    tracks["album_type"] = frame_column(flat, "album.album_type").map(clean_text)
    tracks["release_date_raw"] = frame_column(flat, "album.release_date").map(clean_text)
    tracks["release_date_precision"] = frame_column(flat, "album.release_date_precision").map(clean_text)
    tracks["release_date"] = pd.Series(tracks.apply(release_date_of, axis=1).tolist(), index=tracks.index,
                                       dtype=object)
    tracks["duration_ms"] = frame_column(flat, "duration_ms").map(to_int_or_zero)
    tracks["is_explicit"] = frame_column(flat, "explicit").map(to_flag)
    tracks["track_number"] = frame_column(flat, "track_number").map(to_int_or_zero)
    tracks["disc_number"] = frame_column(flat, "disc_number").map(to_int_or_zero)

    search_isrc = frame_column(flat, "external_ids.isrc").map(clean_text_or_none)
    lookup_isrc = tracks["spotify_track_id"].map(isrc_by_track)
    has_search = ~search_isrc.map(is_missing).astype(bool)
    has_lookup = ~lookup_isrc.map(is_missing).astype(bool)
    tracks["isrc_source"] = "missing"
    tracks.loc[has_lookup, "isrc_source"] = "track_lookup"
    tracks.loc[has_search, "isrc_source"] = "search"
    tracks["isrc_raw"] = search_isrc.where(has_search, lookup_isrc)
    tracks["isrc"] = tracks["isrc_raw"].map(normalize_isrc)
    song_codes = []
    for track_id in tracks["spotify_track_id"].tolist():
        song_codes.append(sorted(songs_by_track.get(track_id, set())))
    tracks["song_codes"] = pd.Series(song_codes, index=tracks.index, dtype=object)

    tracks["reject_reason"] = pd.Series([None] * len(tracks), index=tracks.index, dtype=object)
    rule = ~tracks["spotify_track_id"].map(is_spotify_track_id).astype(bool)
    tracks.loc[rule, "reject_reason"] = "invalid_track_id"
    rule = tracks["reject_reason"].isna() & tracks["isrc"].map(is_missing).astype(bool)
    tracks.loc[rule, "reject_reason"] = "missing_isrc"
    rule = tracks["reject_reason"].isna() & ~tracks["isrc"].map(is_valid_isrc).astype(bool)
    tracks.loc[rule, "reject_reason"] = "invalid_isrc_format"
    tracks["record_label"] = tracks["track_name"] + " — " + tracks["artist_names"].map(", ".join)
    return tracks


def run_preprocess_spotify(batch_id: str) -> dict[str, Any]:
    client = get_client()
    run_log = run_log_start(client, batch_id, "preprocess_spotify")
    details = run_log["details"]
    try:
        watermark = get_watermark(client, "spotify")
        params = {"wm": ch_timestamp(watermark)}
        newer = "WHERE ingested_at > toDateTime64({wm:String}, 3, 'UTC')"
        searches = query_df(client, f"FROM raw.spotify_search_responses {newer}", [
            "request_id", "batch_id", "ingested_at", "song_code", "query_type", "page_offset", "http_status",
            "response_json"], params)
        lookups = query_df(client, f"FROM raw.spotify_track_responses {newer}", [
            "batch_id", "ingested_at", "spotify_track_id", "http_status", "response_json"], params)
        details["watermark_from"] = watermark.isoformat()
        details["raw_search_rows"] = len(searches)
        details["raw_lookup_rows"] = len(lookups)
        if len(searches) > 0:
            items = spotify_search_items(searches)
            status = spotify_search_status(searches, items)
            hits = spotify_hits(items, latest_batch_by_song(searches))
            tracks = spotify_tracks_frame(items, isrc_from_lookups(lookups))

            # a refresh that failed completely must not hide the last good result
            known = set(query_df(client, "FROM clean.spotify_searches FINAL", ["song_code"])["song_code"].tolist())
            keep = (status["is_success"] == 1) | ~status["song_code"].isin(known)
            details["songs_upserted"] = upsert(client, "clean.spotify_searches", status[keep], SPOTIFY_STATUS_COLUMNS)
            details["hits_upserted"] = upsert(client, "clean.spotify_search_hits", hits, SPOTIFY_HIT_COLUMNS)

            valid = tracks[tracks["reject_reason"].isna()]
            rejected = tracks[tracks["reject_reason"].notna()]
            details["quarantined"] = write_quarantine(client, quarantine_frame(
                rejected, "spotify", "spotify_track", "spotify_track_id", "record_label", batch_id))
            details["quarantine_reasons"] = count_reasons(rejected)
            stats = scd2_upsert(client, SPOTIFY_TRACK_SCD, valid[SPOTIFY_TRACK_SCD["tracked"]
                                                                 + SPOTIFY_TRACK_SCD["type1"]
                                                                 + ["spotify_track_id"]], batch_id)
            for name in ("inserted", "updated", "type1_updated", "unchanged"):
                details["tracks_" + name] = stats[name]
            run_log["rows_loaded"] = details["songs_upserted"] + details["hits_upserted"] + stats["rows_written"]
        new_watermark = newest_timestamp([searches, lookups], "ingested_at", watermark)
        set_watermark(client, "spotify", new_watermark, batch_id, len(searches) + len(lookups))
        details["watermark_to"] = new_watermark.isoformat()
    except BaseException as exc:
        run_log_finish(run_log, exc)
        raise
    run_log_finish(run_log)
    return dict(details)


# =============================================================================
# SECTION 15 — YouTube: quota ledger + extract (Data API v3) -> raw
# -----------------------------------------------------------------------------
# Quota: 10,000 units/day, search.list = 100, videos.list = 1 per <= 50 ids.
# 90 % safety => ~89 songs/day. The ledger is asked BEFORE every call; when the
# budget is spent the task ends as 'partial' and the next run continues.
# =============================================================================
YOUTUBE_API = "https://www.googleapis.com/youtube/v3"
SEARCH_COST = 100
VIDEOS_COST = 1
VIDEOS_PER_CALL = 50
QUOTA_ERROR_REASONS = ("quotaExceeded", "dailyLimitExceeded")
YOUTUBE_SEARCH_COLUMNS = ["request_id", "batch_id", "song_code", "query_text", "region_code", "max_results",
                          "video_category_id", "http_status", "error_message", "quota_cost", "response_json"]
YOUTUBE_VIDEO_COLUMNS = ["request_id", "batch_id", "video_ids", "http_status", "error_message", "quota_cost",
                         "response_json"]
QUOTA_COLUMNS = ["event_id", "batch_id", "api", "endpoint", "quota_date", "units"]


def pacific_today() -> date:
    return datetime.now(tz=PACIFIC).date()


def new_quota_ledger(client: Any, batch_id: str, daily_units: int, safety_ratio: float) -> dict[str, Any]:
    """Budget = daily units x safety ratio, minus what was already spent today (Pacific)."""
    quota_date = pacific_today()
    rows = query_rows(client, "SELECT sum(units) AS spent FROM ops.api_quota_usage "
                              "WHERE api = 'youtube' AND quota_date = {d:Date}", {"d": quota_date})
    spent = 0
    if rows and rows[0]["spent"] is not None:
        spent = int(rows[0]["spent"])
    return {"batch_id": batch_id, "quota_date": quota_date, "budget": int(daily_units * safety_ratio),
            "spent": spent, "writer": new_insert_buffer(client, "ops.api_quota_usage", QUOTA_COLUMNS, 10)}


def quota_can_spend(ledger: dict[str, Any], units: int) -> bool:
    return ledger["budget"] - ledger["spent"] >= units


def quota_record(ledger: dict[str, Any], endpoint: str, units: int) -> None:
    ledger["spent"] = ledger["spent"] + units          # in memory first: checks stay exact
    buffer_add(ledger["writer"], {"event_id": uuid.uuid4(), "batch_id": ledger["batch_id"], "api": "youtube",
                                  "endpoint": endpoint, "quota_date": ledger["quota_date"], "units": units})


def youtube_is_quota_exceeded(result: dict[str, Any]) -> bool:
    """403 with reason quotaExceeded / dailyLimitExceeded => stop for today."""
    if result["status"] != 403:
        return False
    try:
        payload = json.loads(result["text"])
    except ValueError:
        return False
    if not isinstance(payload, dict):
        return False
    for error in (payload.get("error") or {}).get("errors") or []:
        if error.get("reason") in QUOTA_ERROR_REASONS:
            return True
    return False


def youtube_query(title: Any, artist: Any) -> str:
    artists = split_artists(artist)
    if artists:
        return f"{query_title(title)} {artists[0]}".strip()
    return query_title(title)


def youtube_search(api: dict[str, Any], query: str) -> dict[str, Any]:
    cfg = api["cfg"]
    params = {"part": "snippet", "q": query, "type": "video", "maxResults": cfg["max_results"],
              "regionCode": cfg["region_code"], "key": cfg["api_key"]}
    if cfg["video_category_id"]:
        params["videoCategoryId"] = cfg["video_category_id"]      # requires type=video
    throttle_wait(api["throttle"])
    return http_request(api["session"], "GET", f"{YOUTUBE_API}/search", params=params)


def youtube_list_videos(api: dict[str, Any], video_ids: list[str]) -> dict[str, Any]:
    throttle_wait(api["throttle"])
    return http_request(api["session"], "GET", f"{YOUTUBE_API}/videos", params={
        "part": "snippet,contentDetails,statistics", "id": ",".join(video_ids), "maxResults": 50,
        "key": api["cfg"]["api_key"]})


def youtube_collect_ids(response_text: str, seen: set[str], pending: list[str]) -> None:
    """Queue every video id of a search response not seen in this run (de-dup across songs)."""
    for item in json.loads(response_text).get("items") or []:
        video_id = (item.get("id") or {}).get("videoId")
        if video_id and video_id not in seen:
            seen.add(video_id)
            pending.append(video_id)


def youtube_fetch_details(state: dict[str, Any], ids: list[str]) -> None:
    """One videos.list call (<= 50 ids) -> raw.youtube_video_responses."""
    if not ids or not quota_can_spend(state["ledger"], VIDEOS_COST):
        return
    result = youtube_list_videos(state["api"], ids)
    state["run_log"]["api_calls"] += 1
    quota_record(state["ledger"], "videos.list", VIDEOS_COST)
    if not result["ok"]:
        state["run_log"]["api_errors"] += 1
    error_message, response_json = error_and_body(result)
    buffer_add(state["video_buffer"], {
        "request_id": uuid.uuid4(), "batch_id": state["batch_id"], "video_ids": ids,
        "http_status": result["status"], "error_message": error_message, "quota_cost": VIDEOS_COST,
        "response_json": response_json})
    circuit_breaker_record(state["breaker"], result["ok"])


def run_extract_youtube(batch_id: str, max_songs: int) -> dict[str, Any]:
    settings = get_settings()
    cfg = settings["youtube"]
    client = get_client()
    api = {"cfg": cfg, "session": requests.Session(), "throttle": new_throttle(0.1)}
    ledger = new_quota_ledger(client, batch_id, cfg["daily_quota_units"], cfg["quota_safety_ratio"])
    songs = select_due_songs(client, "youtube", cfg["refresh_days"], max_songs)
    log.info("youtube: %d songs due, budget %d units, already spent today %d", len(songs), ledger["budget"],
             ledger["spent"])

    run_log = run_log_start(client, batch_id, "extract_youtube")
    state = {"api": api, "ledger": ledger, "run_log": run_log, "batch_id": batch_id,
             "breaker": new_circuit_breaker("youtube"),
             "video_buffer": new_insert_buffer(client, "raw.youtube_video_responses", YOUTUBE_VIDEO_COLUMNS)}
    search_buffer = new_insert_buffer(client, "raw.youtube_search_responses", YOUTUBE_SEARCH_COLUMNS)
    details = run_log["details"]
    details["songs_due"] = len(songs)
    details["songs_processed"] = 0
    details["songs_deferred"] = 0
    details["quota_budget"] = ledger["budget"]
    details["quota_spent_before"] = ledger["spent"]
    pending = []
    seen = set()
    stop_reason = ""
    try:
        try:
            for index in range(len(songs)):
                song = songs[index]
                # reserve the search AND one videos.list unit before spending anything
                if not quota_can_spend(ledger, SEARCH_COST + VIDEOS_COST):
                    stop_reason = "daily quota budget exhausted"
                    details["songs_deferred"] = len(songs) - index
                    break
                query = youtube_query(song["title"], song["artist"])
                result = youtube_search(api, query)
                run_log["api_calls"] += 1
                if youtube_is_quota_exceeded(result):
                    stop_reason = "API returned quotaExceeded"
                    details["songs_deferred"] = len(songs) - index
                    break
                quota_record(ledger, "search.list", SEARCH_COST)   # charged even when it fails validation
                if not result["ok"]:
                    run_log["api_errors"] += 1
                error_message, response_json = error_and_body(result)
                buffer_add(search_buffer, {
                    "request_id": uuid.uuid4(), "batch_id": batch_id, "song_code": song["code"],
                    "query_text": query, "region_code": cfg["region_code"], "max_results": cfg["max_results"],
                    "video_category_id": cfg["video_category_id"], "http_status": result["status"],
                    "error_message": error_message, "quota_cost": SEARCH_COST, "response_json": response_json})
                circuit_breaker_record(state["breaker"], result["ok"])
                details["songs_processed"] += 1
                if result["ok"]:
                    youtube_collect_ids(result["text"], seen, pending)
                while len(pending) >= VIDEOS_PER_CALL:
                    youtube_fetch_details(state, pending[:VIDEOS_PER_CALL])
                    pending = pending[VIDEOS_PER_CALL:]
            youtube_fetch_details(state, pending)                    # remainder (< 50 ids)
            details["videos_found"] = len(seen)
            details["quota_spent_after"] = ledger["spent"]
            if stop_reason:
                run_log_mark_partial(run_log, stop_reason)
                log.warning("youtube: stopped early (%s); %d songs deferred to the next run", stop_reason,
                            details["songs_deferred"])
            run_log["rows_loaded"] = buffer_rows_accepted(search_buffer) + buffer_rows_accepted(state["video_buffer"])
        finally:
            # raw rows AND quota events already spent must survive any error
            flush_all_buffers([state["video_buffer"], search_buffer, ledger["writer"]])
    except BaseException as exc:
        run_log_finish(run_log, exc)
        raise
    run_log_finish(run_log)
    summary = dict(details)
    summary["api_calls"] = run_log["api_calls"]
    summary["api_errors"] = run_log["api_errors"]
    return summary


# =============================================================================
# SECTION 16 — YouTube: preprocess raw -> pandas -> clean (upsert + SCD2)
# =============================================================================
YOUTUBE_VIDEO_SCD = scd_spec(
    "clean.youtube_videos", key="video_id", key_type="str",
    tracked=["video_title", "channel_id", "channel_title", "published_at", "category_id", "duration_seconds",
             "is_licensed_content", "default_audio_language"],
    type1=["view_count", "like_count", "comment_count", "stats_fetched_at"], full_snapshot=False)
YOUTUBE_STATUS_COLUMNS = ["song_code", "last_batch_id", "last_searched_at", "query_text", "is_success", "hit_count"]
YOUTUBE_HIT_COLUMNS = ["song_code", "video_id", "search_rank", "video_title", "channel_id", "channel_title",
                       "published_at", "search_batch_id", "last_seen_at"]


def youtube_search_frames(searches: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """PURE: latest search per song -> {'status': per song, 'hits': per (song, video)}."""
    latest = searches.sort_values("ingested_at").drop_duplicates("song_code", keep="last")
    status_records = []
    hit_records = []
    for row in latest.to_dict("records"):
        items = []
        if int(row["http_status"]) == 200:
            items = json.loads(row["response_json"]).get("items") or []
        status_records.append({
            "song_code": row["song_code"], "last_batch_id": row["batch_id"], "last_searched_at": row["ingested_at"],
            "query_text": row["query_text"], "is_success": int(int(row["http_status"]) == 200),
            "hit_count": len(items)})
        rank = 0
        for item in items:
            rank += 1
            snippet = item.get("snippet") or {}
            hit_records.append({
                "song_code": row["song_code"], "video_id": clean_text((item.get("id") or {}).get("videoId")),
                "search_rank": rank, "video_title": clean_html_text(snippet.get("title")),
                "channel_id": clean_text(snippet.get("channelId")),
                "channel_title": clean_html_text(snippet.get("channelTitle")),
                "published_at": parse_utc_timestamp(snippet.get("publishedAt")),
                "search_batch_id": row["batch_id"], "last_seen_at": row["ingested_at"]})
    status = pd.DataFrame(status_records, columns=YOUTUBE_STATUS_COLUMNS)
    hits = pd.DataFrame(hit_records, columns=YOUTUBE_HIT_COLUMNS)
    hits = hits[hits["video_id"].map(is_youtube_video_id).astype(bool)]
    hits = hits.sort_values("search_rank").drop_duplicates(["song_code", "video_id"], keep="first")
    return {"status": status, "hits": hits}


def youtube_videos_frame(video_rows: pd.DataFrame, hits: pd.DataFrame) -> dict[str, Any]:
    """PURE: videos.list responses -> {'videos': validated rows, 'unavailable': ids}.

    A video asked for but NOT returned by videos.list was deleted or made
    private after search.list returned it: it is quarantined (video_unavailable)
    and its open SCD2 version is closed with is_deleted = 1.
    """
    records = []
    last_returned = {}
    last_missing = {}
    for row in video_rows.sort_values("ingested_at").to_dict("records"):
        if int(row["http_status"]) != 200:
            continue
        returned = set()
        for item in json.loads(row["response_json"]).get("items") or []:
            video_id = clean_text(item.get("id"))
            returned.add(video_id)
            last_returned[video_id] = row["ingested_at"]
            records.append({"video_id": video_id, "ingested_at": row["ingested_at"], "item": item})
        for video_id in row["video_ids"]:
            if video_id not in returned:
                last_missing[video_id] = row["ingested_at"]
    unavailable = []
    for video_id, missing_at in last_missing.items():
        if video_id not in last_returned or as_utc(last_returned[video_id]) < as_utc(missing_at):
            unavailable.append(video_id)

    columns = ["video_id", "video_title", "channel_id", "channel_title", "published_at", "category_id",
               "duration_seconds", "is_licensed_content", "default_audio_language", "view_count", "like_count",
               "comment_count", "stats_fetched_at", "reject_reason"]
    videos = pd.DataFrame(columns=columns)
    if records:
        items = pd.DataFrame(records)
        latest = items.sort_values("ingested_at").drop_duplicates("video_id", keep="last")
        flat = pd.json_normalize(latest["item"].tolist())
        flat.index = latest.index
        videos = pd.DataFrame(index=latest.index)
        videos["video_id"] = latest["video_id"]
        videos["video_title"] = frame_column(flat, "snippet.title").map(clean_html_text)
        videos["channel_id"] = frame_column(flat, "snippet.channelId").map(clean_text)
        videos["channel_title"] = frame_column(flat, "snippet.channelTitle").map(clean_html_text)
        videos["published_at"] = pd.Series(frame_column(flat, "snippet.publishedAt").map(parse_utc_timestamp)
                                           .tolist(), index=latest.index, dtype=object)
        videos["category_id"] = frame_column(flat, "snippet.categoryId").map(clean_text)
        videos["duration_seconds"] = frame_column(flat, "contentDetails.duration").map(iso_duration_seconds)
        videos["is_licensed_content"] = frame_column(flat, "contentDetails.licensedContent").map(to_flag)
        videos["default_audio_language"] = frame_column(flat, "snippet.defaultAudioLanguage").map(clean_text)
        videos["view_count"] = frame_column(flat, "statistics.viewCount").map(to_int_or_none).astype("Int64")
        videos["like_count"] = frame_column(flat, "statistics.likeCount").map(to_int_or_none).astype("Int64")
        videos["comment_count"] = frame_column(flat, "statistics.commentCount").map(to_int_or_none).astype("Int64")
        videos["stats_fetched_at"] = latest["ingested_at"]
        videos["reject_reason"] = pd.Series([None] * len(videos), index=videos.index, dtype=object)
        rule = ~videos["video_id"].map(is_youtube_video_id).astype(bool)
        videos.loc[rule, "reject_reason"] = "invalid_video_id"
        rule = videos["reject_reason"].isna() & (videos["channel_id"] == "")
        videos.loc[rule, "reject_reason"] = "missing_channel_id"

    titles = {}
    for row in hits[["video_id", "video_title", "song_code"]].itertuples(index=False):
        titles[row.video_id] = f"{row.video_title} (song {row.song_code})"
    unavailable_records = []
    for video_id in sorted(unavailable):
        unavailable_records.append({"video_id": video_id, "record_label": titles.get(video_id, ""),
                                    "reject_reason": "video_unavailable"})
    videos["record_label"] = videos["video_title"]
    return {"videos": videos,
            "unavailable": pd.DataFrame(unavailable_records, columns=["video_id", "record_label", "reject_reason"])}


def run_preprocess_youtube(batch_id: str) -> dict[str, Any]:
    client = get_client()
    run_log = run_log_start(client, batch_id, "preprocess_youtube")
    details = run_log["details"]
    try:
        watermark = get_watermark(client, "youtube")
        params = {"wm": ch_timestamp(watermark)}
        newer = "WHERE ingested_at > toDateTime64({wm:String}, 3, 'UTC')"
        searches = query_df(client, f"FROM raw.youtube_search_responses {newer}", [
            "batch_id", "ingested_at", "song_code", "query_text", "http_status", "response_json"], params)
        video_rows = query_df(client, f"FROM raw.youtube_video_responses {newer}", [
            "batch_id", "ingested_at", "video_ids", "http_status", "response_json"], params)
        details["watermark_from"] = watermark.isoformat()
        details["raw_search_rows"] = len(searches)
        details["raw_video_rows"] = len(video_rows)
        if len(searches) > 0 or len(video_rows) > 0:
            frames = youtube_search_frames(searches)
            status = frames["status"]
            known = set(query_df(client, "FROM clean.youtube_searches FINAL", ["song_code"])["song_code"].tolist())
            keep = (status["is_success"] == 1) | ~status["song_code"].isin(known)
            details["songs_upserted"] = upsert(client, "clean.youtube_searches", status[keep], YOUTUBE_STATUS_COLUMNS)
            details["hits_upserted"] = upsert(client, "clean.youtube_search_hits", frames["hits"], YOUTUBE_HIT_COLUMNS)

            result = youtube_videos_frame(video_rows, frames["hits"])
            videos = result["videos"]
            valid = videos[videos["reject_reason"].isna()]
            rejected = pd.concat([videos[videos["reject_reason"].notna()], result["unavailable"]], ignore_index=True)
            details["quarantined"] = write_quarantine(client, quarantine_frame(
                rejected, "youtube", "youtube_video", "video_id", "record_label", batch_id))
            details["quarantine_reasons"] = count_reasons(rejected)
            stats = scd2_upsert(client, YOUTUBE_VIDEO_SCD, valid[["video_id"] + YOUTUBE_VIDEO_SCD["tracked"]
                                                                 + YOUTUBE_VIDEO_SCD["type1"]], batch_id,
                                deleted_keys=result["unavailable"]["video_id"].tolist())
            for name in ("inserted", "updated", "type1_updated", "deleted", "unchanged"):
                details["videos_" + name] = stats[name]
            run_log["rows_loaded"] = details["songs_upserted"] + details["hits_upserted"] + stats["rows_written"]
        new_watermark = newest_timestamp([searches, video_rows], "ingested_at", watermark)
        set_watermark(client, "youtube", new_watermark, batch_id, len(searches) + len(video_rows))
        details["watermark_to"] = new_watermark.isoformat()
    except BaseException as exc:
        run_log_finish(run_log, exc)
        raise
    run_log_finish(run_log)
    return dict(details)


# =============================================================================
# SECTION 17 — quarantine e-mail (sent when THIS run quarantined records)
# =============================================================================
def run_notify_quarantine(batch_id: str, table: str = "dq.quarantine_records") -> dict[str, Any]:
    """`table` is only overridden by the integration test (a throw-away copy)."""
    settings = get_settings()
    client = get_client()
    records = query_df(client, f"FROM {table} FINAL WHERE batch_id = {{b:String}} "
                               "ORDER BY source, entity, reject_reason, record_key",
                       ["source", "entity", "reject_reason", "record_key", "record_label", "severity"], {"b": batch_id})
    if len(records) == 0:
        log.info("no quarantined records in %s - no e-mail", batch_id)
        return {"quarantined": 0, "email": "not needed"}

    earlier = query_df(client, "FROM (SELECT DISTINCT source, entity, record_key, reject_reason "
                               f"FROM {table} FINAL WHERE batch_id != {{b:String}})",
                       ["source", "entity", "record_key", "reject_reason"], {"b": batch_id})
    earlier["seen_before"] = 1
    records = records.merge(earlier, on=["source", "entity", "record_key", "reject_reason"], how="left")
    records["is_new"] = records["seen_before"].isna().astype(int)
    summary = records.groupby(["source", "entity", "reject_reason"], as_index=False).agg(
        records=("record_key", "count"), new=("is_new", "sum"))

    summary_rows = []
    for row in summary.itertuples(index=False):
        summary_rows.append([row.source, row.entity, row.reject_reason, int(row.records), int(row.new)])
    sample_rows = []
    for row in records.head(30).itertuples(index=False):
        sample_rows.append([row.source, row.reject_reason, row.record_key, row.record_label])
    total = len(records)
    new_total = int(records["is_new"].sum())
    dashboard = settings["superset"]["public_url"].rstrip("/") + "/superset/dashboard/data-quarantine/"

    subject = f"[Massive Music ELT] QUARANTINE: {total} record(s) rejected ({new_total} new)"
    text_body = (
        f"Run {batch_id} quarantined {total} record(s) ({new_total} new, {total - new_total} already known).\n"
        "They were NOT loaded into the clean/marts layers; everything else was.\n\n"
        f"Summary (source | entity | reason | records | new):\n{text_table(summary_rows)}\n\n"
        f"First records (source | reason | key | label):\n{text_table(sample_rows)}\n\n"
        f"Dashboard: {dashboard}\nTable: dq.quarantine_records (batch_id = '{batch_id}')"
    )
    html_body = email_html(
        f"{total} record(s) quarantined", "#f2a900",
        f"Run <b>{html.escape(batch_id)}</b> rejected <b>{total}</b> record(s) "
        f"(<b>{new_total}</b> new). They were kept out of the clean and marts layers; the rest of the data loaded normally.",
        "<b>By reason</b>" + html_table(["Source", "Entity", "Reason", "Records", "New"], summary_rows)
        + "<b>First records</b>" + html_table(["Source", "Reason", "Key", "Record"], sample_rows)
        + f'<p><a href="{html.escape(dashboard)}">Open the Data Quarantine dashboard</a></p>',
        f"Query: SELECT * FROM dq.quarantine_records FINAL WHERE batch_id = '{html.escape(batch_id)}'",
    )
    status = send_email(subject, text_body, html_body, "quarantine", batch_id)
    return {"quarantined": total, "new": new_total, "email": status}


# =============================================================================
# SECTION 18 — post-transform data-quality checks -> ops.dq_results
# -----------------------------------------------------------------------------
# Layered strategy: (1) pandas validation + quarantine while loading,
# (2) dbt tests on every model, (3) these statistical / cross-run checks.
# A failing 'error' check fails the task AFTER every result is stored.
# =============================================================================
DQ_COLUMNS = ["batch_id", "check_name", "layer", "severity", "status", "metric_value", "threshold", "details"]


def make_check(name: str, layer: str, severity: str, passed: bool, metric: float, threshold: str,
               details: dict) -> dict[str, Any]:
    status = "pass"
    if not passed and severity == "error":
        status = "fail"
    elif not passed:
        status = "warn"
    return {"name": name, "layer": layer, "severity": severity, "status": status, "metric": float(metric),
            "threshold": threshold, "details": details}


def scalar(client: Any, sql: str, params: dict | None = None) -> float:
    rows = query_rows(client, sql, params)
    if not rows:
        return 0.0
    value = list(rows[0].values())[0]
    if value is None:
        return 0.0
    return float(value)


def check_catalog_has_songs(client: Any, batch_id: str) -> dict[str, Any]:
    n = scalar(client, "SELECT count() FROM clean.catalog_songs FINAL WHERE is_current = 1 AND is_deleted = 0")
    return make_check("catalog_current_songs", "clean", "error", n > 0, n, "> 0", {})


def check_catalog_quarantine_rate(client: Any, batch_id: str) -> dict[str, Any]:
    quarantined = scalar(client, "SELECT count() FROM dq.quarantine_records FINAL "
                                 "WHERE batch_id = {b:String} AND source = 'catalog'", {"b": batch_id})
    songs = scalar(client, "SELECT count() FROM clean.catalog_songs FINAL WHERE is_current = 1 AND is_deleted = 0")
    rate = 0.0
    if songs + quarantined > 0:
        rate = quarantined / (songs + quarantined)
    return make_check("catalog_quarantine_rate", "clean", "warn", rate <= 0.02, round(rate, 4), "<= 2%",
                      {"quarantined": quarantined, "valid_songs": songs})


def check_overview_completeness(client: Any, batch_id: str) -> dict[str, Any]:
    row = query_rows(client, "SELECT (SELECT count() FROM marts.dim_song) AS songs, "
                             "(SELECT count() FROM marts.mart_song_overview) AS overview")[0]
    return make_check("overview_completeness", "marts", "error", row["songs"] == row["overview"],
                      row["overview"], f"= dim_song ({row['songs']})", dict(row))


def api_error_rate(client: Any, table: str, name: str, batch_id: str) -> dict[str, Any]:
    row = query_rows(client, f"SELECT count() AS calls, countIf(http_status != 200) AS errors "
                             f"FROM {table} WHERE batch_id = {{b:String}}", {"b": batch_id})[0]
    rate = 0.0
    if row["calls"]:
        rate = row["errors"] / row["calls"]
    severity = "warn"
    limit = 0.05
    if rate > 0.25:          # > 25 % = something is broken (credentials, outage)
        severity = "error"
        limit = 0.25
    return make_check(name, "raw", severity, rate <= limit, round(rate, 4), "warn > 5%, error > 25%", dict(row))


def check_spotify_error_rate(client: Any, batch_id: str) -> dict[str, Any]:
    return api_error_rate(client, "raw.spotify_search_responses", "spotify_api_error_rate", batch_id)


def check_youtube_error_rate(client: Any, batch_id: str) -> dict[str, Any]:
    return api_error_rate(client, "raw.youtube_search_responses", "youtube_api_error_rate", batch_id)


def match_rate(client: Any, status_column: str, name: str, minimum: float) -> dict[str, Any]:
    row = query_rows(client, f"SELECT countIf({status_column} = 'matched') AS matched, "
                             f"countIf({status_column} IN ('matched', 'no_match')) AS searched "
                             f"FROM marts.mart_song_overview")[0]
    rate = 0.0
    if row["searched"]:
        rate = row["matched"] / row["searched"]
    passed = rate >= minimum or row["searched"] == 0
    return make_check(name, "marts", "warn", passed, round(rate, 4), f">= {minimum:.0%}", dict(row))


def check_spotify_match_rate(client: Any, batch_id: str) -> dict[str, Any]:
    return match_rate(client, "spotify_status", "spotify_match_rate", 0.60)


def check_youtube_match_rate(client: Any, batch_id: str) -> dict[str, Any]:
    return match_rate(client, "youtube_status", "youtube_match_rate", 0.50)


def check_recording_drift(client: Any, batch_id: str) -> dict[str, Any]:
    """Distinct ISRCs must not drop > 30 % vs the previous run (silent data loss)."""
    current = scalar(client, "SELECT count() FROM marts.dim_recording")
    previous = scalar(client, "SELECT metric_value FROM ops.dq_results WHERE check_name = 'recording_count' "
                              "AND batch_id != {b:String} ORDER BY checked_at DESC LIMIT 1", {"b": batch_id})
    client.insert("ops.dq_results", [[batch_id, "recording_count", "marts", "warn", "pass", current, "", "{}"]],
                  column_names=DQ_COLUMNS)
    drop = 0.0
    if previous:
        drop = (previous - current) / previous
    return make_check("recording_count_drift", "marts", "warn", drop <= 0.30, round(drop, 4),
                      "drop <= 30% vs previous run", {"current": current, "previous": previous})


def check_scd2_change_volume(client: Any, batch_id: str) -> dict[str, Any]:
    """Anomaly guard: a normal day changes few catalog songs; a mass edit is suspicious."""
    changed = scalar(client, "SELECT count() FROM clean.catalog_songs FINAL WHERE batch_id = {b:String} "
                             "AND version_no > 1", {"b": batch_id})
    songs = scalar(client, "SELECT count() FROM clean.catalog_songs FINAL WHERE is_current = 1")
    rate = 0.0
    if songs:
        rate = changed / songs
    return make_check("catalog_scd2_change_rate", "clean", "warn", rate <= 0.10, round(rate, 4), "<= 10% per run",
                      {"new_versions": changed, "current_songs": songs})


def check_youtube_backlog(client: Any, batch_id: str) -> dict[str, Any]:
    n = scalar(client, "SELECT countIf(youtube_status = 'pending_quota') FROM marts.mart_song_overview")
    return make_check("youtube_quota_backlog", "marts", "warn", True, n, "info only",
                      {"note": "songs not searched yet because of the daily YouTube quota"})


DQ_CHECKS = [
    check_catalog_has_songs,
    check_catalog_quarantine_rate,
    check_overview_completeness,
    check_spotify_error_rate,
    check_youtube_error_rate,
    check_spotify_match_rate,
    check_youtube_match_rate,
    check_recording_drift,
    check_scd2_change_volume,
    check_youtube_backlog,
]


def run_dq_checks(batch_id: str) -> dict[str, Any]:
    client = get_client()
    run_log = run_log_start(client, batch_id, "dq_checks")
    try:
        results = []
        for check in DQ_CHECKS:
            try:
                result = check(client, batch_id)
            except Exception as exc:          # a broken check is itself a failure
                result = make_check(check.__name__, "n/a", "error", False, 0, "check must run",
                                    {"exception": repr(exc)})
            results.append(result)
        values = []
        failed = []
        warned = []
        for result in results:
            values.append([batch_id, result["name"], result["layer"], result["severity"], result["status"],
                           result["metric"], result["threshold"], json.dumps(result["details"], default=str)])
            run_log["details"][result["name"]] = result["status"]
            if result["status"] == "fail":
                failed.append(result["name"])
            elif result["status"] == "warn":
                warned.append(result["name"])
            log.info("DQ %-28s %-5s metric=%s threshold=%s %s", result["name"], result["status"].upper(),
                     result["metric"], result["threshold"], result["details"])
        client.insert("ops.dq_results", values, column_names=DQ_COLUMNS)
        if failed:
            raise RuntimeError(f"DQ gate failed: {failed} (warnings: {warned})")
    except BaseException as exc:
        run_log_finish(run_log, exc)
        raise
    run_log_finish(run_log)
    return {"failed": failed, "warnings": warned}


# =============================================================================
# SECTION 19 — Superset dashboards as code (REST API, idempotent)
# -----------------------------------------------------------------------------
# Published as the LAST task because the datasets point at dbt marts. Every
# object is looked up by name and updated in place, so re-running is safe.
# =============================================================================
SUPERSET_DATABASE_NAME = "ClickHouse Warehouse"


def superset_login(cfg: dict[str, Any]) -> dict[str, Any]:
    base_url = cfg["url"].rstrip("/")
    session = requests.Session()
    login = session.post(f"{base_url}/api/v1/security/login", timeout=60, json={
        "username": cfg["admin_user"], "password": cfg["admin_password"], "provider": "db", "refresh": True})
    login.raise_for_status()
    session.headers["Authorization"] = f"Bearer {login.json()['access_token']}"
    csrf = session.get(f"{base_url}/api/v1/security/csrf_token/", timeout=30)
    csrf.raise_for_status()
    session.headers["X-CSRFToken"] = csrf.json()["result"]
    session.headers["Referer"] = base_url
    return {"url": base_url, "session": session}


def superset_call(api: dict[str, Any], method: str, path: str, payload: dict | None = None) -> dict:
    response = api["session"].request(method, f"{api['url']}{path}", json=payload, timeout=120)
    if response.status_code >= 400:
        raise RuntimeError(f"Superset {method} {path} -> {response.status_code}: {response.text[:500]}")
    if not response.text:
        return {}
    return response.json()


def superset_find(api: dict[str, Any], resource: str, field_name: str, value: str) -> dict | None:
    page = 0
    while True:
        data = superset_call(api, "GET", f"/api/v1/{resource}/?q=(page:{page},page_size:100)")
        for item in data.get("result", []):
            if item.get(field_name) == value:
                return item
        if (page + 1) * 100 >= data.get("count", 0):
            return None
        page += 1


def sql_metric(expression: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


def chart_spec(name: str, dataset: str, viz_type: str, params: dict, width: int, height: int, row: int) -> dict:
    """width/height/row place the chart on the dashboard grid (12 columns)."""
    return {"name": name, "dataset": dataset, "viz_type": viz_type, "params": params, "width": width,
            "height": height, "row": row}


def bar_params(x_axis: str, metric: dict, limit: int = 15, horizontal: bool = True) -> dict:
    orientation = "vertical"
    if horizontal:
        orientation = "horizontal"
    return {"x_axis": x_axis, "metrics": [metric], "groupby": [], "adhoc_filters": [], "orientation": orientation,
            "x_axis_sort": metric["label"], "x_axis_sort_asc": False, "timeseries_limit_metric": metric,
            "order_desc": True, "row_limit": limit, "show_legend": False, "show_value": True, "rich_tooltip": True,
            "truncateXAxis": True, "y_axis_format": "SMART_NUMBER"}


def daily_bar_params(time_column: str, metric: dict, groupby: list[str]) -> dict:
    return {"x_axis": time_column, "time_grain_sqla": "P1D", "metrics": [metric], "groupby": groupby,
            "adhoc_filters": [], "row_limit": 5000, "stack": "Stack", "show_legend": True, "rich_tooltip": True,
            "x_axis_time_format": "%Y-%m-%d", "x_axis_sort_asc": True}


def big_number_params(metric: dict, subheader: str, number_format: str = "SMART_NUMBER") -> dict:
    return {"metric": metric, "adhoc_filters": [], "subheader": subheader, "header_font_size": 0.4,
            "subheader_font_size": 0.15, "y_axis_format": number_format}


def pie_params(groupby: str, metric: dict) -> dict:
    return {"groupby": [groupby], "metric": metric, "adhoc_filters": [], "row_limit": 50, "donut": True,
            "show_legend": True, "legendOrientation": "right", "label_type": "key_percent", "show_labels": True,
            "sort_by_metric": True}


def table_params(columns: list[str], order_by: str, limit: int = 1000) -> dict:
    return {"query_mode": "raw", "all_columns": columns, "adhoc_filters": [],
            "order_by_cols": [json.dumps([order_by, False])], "row_limit": limit, "server_pagination": False,
            "include_search": True, "page_length": 20, "table_timestamp_format": "smart_date"}


def song_catalog_charts() -> list[dict]:
    overview = "marts.mart_song_overview"
    charts = [
        chart_spec("Songs in catalog", overview, "big_number_total",
                   big_number_params(sql_metric("count()", "Songs"), "catalog songs (current SCD2 version)"), 3, 25, 1),
        chart_spec("Distinct ISRCs found", overview, "big_number_total",
                   big_number_params(sql_metric("sum(isrc_count)", "ISRCs"), "recordings on Spotify"), 3, 25, 1),
        chart_spec("Distinct videos found", overview, "big_number_total",
                   big_number_params(sql_metric("sum(video_count)", "Videos"), "matched YouTube videos"), 3, 25, 1),
        chart_spec("YouTube coverage", overview, "big_number_total",
                   big_number_params(sql_metric("countIf(youtube_status != 'pending_quota') / count()", "Searched"),
                                     "songs already searched on YouTube", ".1%"), 3, 25, 1),
        chart_spec("Top 15 songs by ISRC count", overview, "echarts_timeseries_bar",
                   bar_params("song_label", sql_metric("sum(isrc_count)", "ISRCs")), 6, 60, 2),
        chart_spec("Top 15 songs by YouTube video count", overview, "echarts_timeseries_bar",
                   bar_params("song_label", sql_metric("sum(video_count)", "Videos")), 6, 60, 2),
    ]
    buckets = bar_params("isrc_count_bucket", sql_metric("count()", "Songs"), 10, horizontal=False)
    buckets["x_axis_sort"] = None
    buckets["x_axis_sort_asc"] = True
    charts.append(chart_spec("Songs by number of ISRCs", overview, "echarts_timeseries_bar", buckets, 4, 50, 3))
    charts.append(chart_spec("Videos by type", "marts.bridge_song_video", "pie",
                             pie_params("video_type", sql_metric("uniqExact(video_id)", "Videos")), 4, 50, 3))
    charts.append(chart_spec("Recordings by relationship to original", "marts.bridge_song_recording", "pie",
                             pie_params("match_type", sql_metric("uniqExact(isrc)", "ISRCs")), 4, 50, 3))
    answer_columns = ["song_code", "song_title", "original_artist", "isrc_count", "isrc_count_original_artist",
                      "isrc_count_other_artists", "distinct_recording_artists", "video_count",
                      "official_channel_video_count", "cover_video_count", "spotify_status", "youtube_status"]
    charts.append(chart_spec("Song-level answers (ISRCs & videos per song)", overview, "table",
                             table_params(answer_columns, "isrc_count"), 12, 90, 4))
    return charts


def pipeline_health_charts() -> list[dict]:
    runs = "marts.mon_pipeline_runs"
    charts = [
        chart_spec("Hours since last successful run", runs, "big_number_total",
                   big_number_params(sql_metric("dateDiff('hour', maxIf(finished_at, status = 'success' "
                                                "AND task_name = 'dq_checks'), now())", "Hours"),
                                     "since the last green dq_checks"), 3, 25, 1),
        chart_spec("Failed task runs (7 days)", runs, "big_number_total",
                   big_number_params(sql_metric("countIf(status = 'failed' AND started_at >= now() - INTERVAL 7 DAY)",
                                                "Failed"), "task attempts that failed"), 3, 25, 1),
        chart_spec("Songs waiting for YouTube quota", "marts.mart_song_overview", "big_number_total",
                   big_number_params(sql_metric("countIf(youtube_status = 'pending_quota')", "Pending"),
                                     "deferred to the next runs"), 3, 25, 1),
        chart_spec("E-mail alerts (7 days)", "marts.mon_alert_log", "big_number_total",
                   big_number_params(sql_metric("countIf(sent_at >= now() - INTERVAL 7 DAY)", "Alerts"),
                                     "failure + quarantine alerts"), 3, 25, 1),
        chart_spec("YouTube quota used per day", "marts.mon_api_quota_daily", "echarts_timeseries_bar",
                   daily_bar_params("quota_date", sql_metric("sum(units_used)", "Units"), ["endpoint"]), 6, 55, 2),
        chart_spec("Record changes captured (SCD Type 2)", "marts.mon_scd_changes", "echarts_timeseries_bar",
                   daily_bar_params("changed_at", sql_metric("count()", "Changes"), ["change_type"]), 6, 55, 2),
    ]
    dq_params = table_params(["check_name", "layer", "severity", "status", "metric_value", "threshold",
                              "checked_at"], "checked_at", 100)
    dq_params["adhoc_filters"] = [{"expressionType": "SQL", "sqlExpression": "is_latest", "clause": "WHERE"}]
    charts.append(chart_spec("Latest data-quality checks", "marts.mon_dq_results", "table", dq_params, 6, 60, 3))
    charts.append(chart_spec("E-mail alert log", "marts.mon_alert_log", "table",
                             table_params(["sent_at", "alert_type", "status", "subject", "error_message"],
                                          "sent_at", 200), 6, 60, 3))
    charts.append(chart_spec("Task runs", runs, "table", table_params(
        ["started_at", "task_name", "status", "duration_seconds", "rows_loaded", "api_calls", "api_errors",
         "batch_id", "error_message"], "started_at", 300), 12, 70, 4))
    return charts


def quarantine_charts() -> list[dict]:
    q = "marts.mon_quarantine_records"
    by_reason = bar_params("reject_reason", sql_metric("count()", "Records"), 25)
    by_reason["groupby"] = ["source"]
    by_reason["stack"] = "Stack"
    by_reason["show_legend"] = True
    return [
        chart_spec("Quarantined in the latest run", q, "big_number_total",
                   big_number_params(sql_metric("countIf(is_latest_batch)", "Records"),
                                     "records rejected by the latest run"), 3, 25, 1),
        chart_spec("Quarantined in the last 30 days", q, "big_number_total",
                   big_number_params(sql_metric("countIf(detected_at >= now() - INTERVAL 30 DAY)", "Records"),
                                     "records rejected (all runs)"), 3, 25, 1),
        chart_spec("Distinct records ever quarantined", q, "big_number_total",
                   big_number_params(sql_metric("uniqExact(source, entity, record_key)", "Records"),
                                     "distinct source keys"), 3, 25, 1),
        chart_spec("Runs with quarantined data", q, "big_number_total",
                   big_number_params(sql_metric("uniqExact(batch_id)", "Runs"), "pipeline runs"), 3, 25, 1),
        chart_spec("Quarantined records by reason", q, "echarts_timeseries_bar", by_reason, 8, 55, 2),
        chart_spec("Quarantined records by source", q, "pie",
                   pie_params("source", sql_metric("count()", "Records")), 4, 55, 2),
        chart_spec("Quarantined records per day", q, "echarts_timeseries_bar",
                   daily_bar_params("detected_at", sql_metric("count()", "Records"), ["source"]), 12, 50, 3),
        chart_spec("Quarantined records (latest first)", q, "table", table_params(
            ["detected_at", "source", "entity", "reject_reason", "record_key", "record_label", "severity",
             "batch_id", "record_json"], "detected_at", 2000), 12, 90, 4),
    ]


def dashboard_specs() -> list[dict]:
    source_note = "> ✅ **Live data** — Spotify Web API + YouTube Data API v3, refreshed daily and incrementally."
    return [
        {"title": "Song Catalog — Recordings & Videos", "slug": "song-catalog", "charts": song_catalog_charts(),
         "intro": ("## Song catalog: ISRCs on Spotify & videos on YouTube\n" + source_note + "\n\n"
                   "**Song** = composition (catalog row). **Recording** = one ISRC (original, live, acoustic, "
                   "covers...). Videos = matched videos in the top YouTube search results (`youtube_status` "
                   "explains every zero).")},
        {"title": "Pipeline Health", "slug": "pipeline-health", "charts": pipeline_health_charts(),
         "intro": ("## Pipeline health\n" + source_note + "\n\n"
                   "Task runs (`ops.pipeline_runs`), YouTube quota (Pacific-time days), SCD Type 2 changes, "
                   "data-quality checks and the e-mail alert log.")},
        {"title": "Data Quarantine", "slug": "data-quarantine", "charts": quarantine_charts(),
         "intro": ("## Data quarantine\nRecords rejected by validation (pandas) are **kept with a reason** in "
                   "`dq.quarantine_records` instead of being silently dropped or loaded. Each run that quarantines "
                   "data e-mails the data team. Fix the source (sheet / API data) and the next run reloads them.")},
    ]


def ensure_database(api: dict[str, Any], cfg: dict[str, Any]) -> int:
    warehouse = get_settings()["clickhouse"]          # same docker network as Superset
    uri = (f"clickhousedb://{quote(cfg['bi_user'], safe='')}:{quote(cfg['bi_password'], safe='')}"
           f"@{warehouse['host']}:{warehouse['port']}/marts")
    payload = {"database_name": SUPERSET_DATABASE_NAME, "sqlalchemy_uri": uri, "expose_in_sqllab": True,
               "allow_run_async": False, "allow_ctas": False, "allow_cvas": False, "allow_dml": False}
    existing = superset_find(api, "database", "database_name", SUPERSET_DATABASE_NAME)
    if existing:
        superset_call(api, "PUT", f"/api/v1/database/{existing['id']}", payload)   # rotates the password too
        return existing["id"]
    return superset_call(api, "POST", "/api/v1/database/", payload)["id"]


def ensure_dataset(api: dict[str, Any], database_id: int, qualified: str, cache: dict[str, int]) -> int:
    if qualified in cache:
        return cache[qualified]
    schema, table = qualified.split(".")
    found = None
    for item in superset_call(api, "GET", "/api/v1/dataset/?q=(page_size:100)").get("result", []):
        if item.get("table_name") == table and item.get("schema") == schema:
            found = item
            break
    if found:
        dataset_id = found["id"]
        superset_call(api, "PUT", f"/api/v1/dataset/{dataset_id}/refresh")       # sync new dbt columns
    else:
        dataset_id = superset_call(api, "POST", "/api/v1/dataset/",
                                   {"database": database_id, "schema": schema, "table_name": table})["id"]
    cache[qualified] = dataset_id
    return dataset_id


def ensure_dashboard(api: dict[str, Any], spec: dict) -> int:
    existing = superset_find(api, "dashboard", "dashboard_title", spec["title"])
    if existing:
        return existing["id"]
    return superset_call(api, "POST", "/api/v1/dashboard/",
                         {"dashboard_title": spec["title"], "slug": spec["slug"], "published": True})["id"]


def ensure_chart(api: dict[str, Any], spec: dict, dataset_id: int, dashboard_id: int) -> int:
    params = {"viz_type": spec["viz_type"], "datasource": f"{dataset_id}__table"}
    params.update(spec["params"])
    payload = {"slice_name": spec["name"], "viz_type": spec["viz_type"], "datasource_id": dataset_id,
               "datasource_type": "table", "params": json.dumps(params), "dashboards": [dashboard_id]}
    existing = superset_find(api, "chart", "slice_name", spec["name"])
    if existing:
        superset_call(api, "PUT", f"/api/v1/chart/{existing['id']}", payload)
        return existing["id"]
    return superset_call(api, "POST", "/api/v1/chart/", payload)["id"]


def dashboard_position(spec: dict, chart_ids: list[int]) -> dict:
    """Grid layout: an intro markdown row, then the charts grouped by `row`."""
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": spec["title"]}},
    }
    rows = ["ROW-intro"]
    for chart in spec["charts"]:
        if f"ROW-{chart['row']}" not in rows:
            rows.append(f"ROW-{chart['row']}")
    for row_id in rows:
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    position["ROW-intro"]["children"].append("MARKDOWN-intro")
    position["MARKDOWN-intro"] = {"type": "MARKDOWN", "id": "MARKDOWN-intro", "children": [],
                                  "parents": ["ROOT_ID", "GRID_ID", "ROW-intro"],
                                  "meta": {"width": 12, "height": 26, "code": spec["intro"]}}
    for chart, chart_id in zip(spec["charts"], chart_ids):
        row_id = f"ROW-{chart['row']}"
        key = f"CHART-{chart_id}"
        position[row_id]["children"].append(key)
        position[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                         "meta": {"chartId": chart_id, "width": chart["width"], "height": chart["height"],
                                  "sliceName": chart["name"]}}
    return position


def run_publish_dashboards() -> dict[str, Any]:
    cfg = get_settings()["superset"]
    api = superset_login(cfg)
    database_id = ensure_database(api, cfg)
    datasets = {}
    summary = {"database_id": database_id, "dashboards": {}}
    for spec in dashboard_specs():
        dashboard_id = ensure_dashboard(api, spec)
        chart_ids = []
        for chart in spec["charts"]:
            dataset_id = ensure_dataset(api, database_id, chart["dataset"], datasets)
            chart_ids.append(ensure_chart(api, chart, dataset_id, dashboard_id))
        metadata = {"refresh_frequency": 0, "color_scheme": "supersetColors", "timed_refresh_immune_slices": [],
                    "expanded_slices": {}, "label_colors": {}, "default_filters": "{}"}
        superset_call(api, "PUT", f"/api/v1/dashboard/{dashboard_id}", {
            "dashboard_title": spec["title"], "slug": spec["slug"], "published": True,
            "position_json": json.dumps(dashboard_position(spec, chart_ids)), "json_metadata": json.dumps(metadata)})
        summary["dashboards"][spec["slug"]] = {"id": dashboard_id, "charts": len(chart_ids),
                                               "url": f"{cfg['public_url']}/superset/dashboard/{spec['slug']}/"}
        log.info("published dashboard %r (%d charts)", spec["title"], len(chart_ids))
    return summary


# =============================================================================
# SECTION 20 — the DAG
# =============================================================================
def run_preflight() -> dict[str, Any]:
    """Fail fast: config sanity, warehouse reachable, DDL applied, Spotify credentials valid."""
    settings = get_settings()
    warnings = validate_settings(settings)
    for warning in warnings:
        log.warning("preflight: %s", warning)
    client = get_client()
    version = client.command("SELECT version()")
    statements = apply_ddl(client)
    spotify_refresh_token(new_spotify_client(settings["spotify"]))      # 401 here = wrong credentials
    return {"clickhouse": version, "ddl_statements": statements, "warnings": warnings,
            "alert_recipients": settings["email"]["to"]}


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(hours=1),      # a hung task is killed (and e-mailed)
    "on_failure_callback": on_task_failure,       # e-mail ALERT_EMAIL_TO after the last retry
}


@dag(
    dag_id=DAG_ID,
    description="Catalog + Spotify ISRCs + YouTube videos -> pandas -> ClickHouse (upsert, SCD2) -> dbt -> Superset",
    schedule="15 15 * * *",          # 15:15 WIB = 08:15 UTC: just after the YouTube quota reset (00:00 PT)
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,               # runs never overlap (watermarks + SCD2 assume a single writer)
    dagrun_timeout=timedelta(hours=4),
    default_args=default_args,
    params={"max_songs": Param(0, type="integer", minimum=0,
                               description="Cap songs searched per API in this run (0 = MAX_SONGS_PER_RUN)")},
    tags=["spotify", "youtube", "pandas", "clickhouse", "scd2", "dbt", "superset"],
    doc_md=__doc__,
)
def music_catalog_elt():

    @task
    def preflight() -> dict:
        return run_preflight()

    @task
    def load_catalog() -> dict:
        return run_load_catalog(batch_id_from_context())

    @task(execution_timeout=timedelta(hours=2))
    def extract_spotify() -> dict:
        return run_extract_spotify(batch_id_from_context(), max_songs_from_context())

    @task
    def preprocess_spotify() -> dict:
        return run_preprocess_spotify(batch_id_from_context())

    @task(execution_timeout=timedelta(hours=2))
    def extract_youtube() -> dict:
        return run_extract_youtube(batch_id_from_context(), max_songs_from_context())

    @task
    def preprocess_youtube() -> dict:
        return run_preprocess_youtube(batch_id_from_context())

    @task(trigger_rule="all_done", retries=1)
    def notify_quarantine() -> dict:
        """Runs even when an upstream task failed: quarantine found so far is still reported."""
        return run_notify_quarantine(batch_id_from_context())

    # Cosmos renders every dbt model as its own task followed by its tests: a
    # failing test stops only the downstream models, each model retries alone.
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=ProfileConfig(profile_name="music_dw", target_name="prod",
                                     profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml"),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        # emit_datasets=False (+ AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS=false in
        # compose): no Airflow Assets per dbt model. This DAG is time-scheduled,
        # and on Airflow 3 two dbt tasks finishing together can race to register
        # the same new Asset (unique-key violation in the metadata DB), which
        # marks a successful dbt model as failed. (Cosmos reads this flag from
        # RenderConfig; a value in operator_args is overwritten.)
        render_config=RenderConfig(load_method=LoadMode.DBT_LS, test_behavior=TestBehavior.AFTER_EACH,
                                   should_detach_multiple_parents_tests=True, dbt_executable_path=DBT_EXECUTABLE,
                                   emit_datasets=False),
        operator_args={"install_deps": False, "retries": 1},
    )

    @task
    def dq_checks() -> dict:
        return run_dq_checks(batch_id_from_context())

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def publish_dashboards() -> dict:
        return run_publish_dashboards()

    catalog = load_catalog()
    spotify_clean = preprocess_spotify()
    youtube_clean = preprocess_youtube()
    preflight() >> catalog
    catalog >> extract_spotify() >> spotify_clean
    catalog >> extract_youtube() >> youtube_clean
    [spotify_clean, youtube_clean] >> dbt_transform >> dq_checks() >> publish_dashboards()
    [spotify_clean, youtube_clean] >> notify_quarantine()


music_catalog_elt_dag = music_catalog_elt()
