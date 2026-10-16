# Massive Music — Song Metadata Pipeline

Automated pipeline that enriches the internal song catalog with **real** Spotify and YouTube metadata and answers, for every song:

1. **How many videos does the song have on YouTube?**
2. **How many ISRCs (recordings) does the song have on Spotify?**

**Stack:** Airflow 3.3 · pandas · ClickHouse 26.8 · dbt 1.11 (+ Cosmos) · Superset 6 · PostgreSQL 16 — all on Docker Compose.

```
                       ┌──────────── Airflow: ONE DAG file (airflow/dags/music_catalog_elt.py) ────────────┐
Google Sheet ──────────┤ load_catalog ── pandas: standardise · validate · de-duplicate ─┐                    │
Spotify Web API ───────┤ extract_spotify ─► raw (JSON) ─► preprocess_spotify (pandas) ─┼─► clean: UPSERT     │
YouTube Data API v3 ───┤ extract_youtube ─► raw (JSON) ─► preprocess_youtube (pandas) ─┘   + SCD Type 2     │
                       │            rejected rows ─► dq.quarantine_records ─► e-mail + Superset dashboard   │
                       │ dbt_transform (staging ► intermediate ► marts) ─► dq_checks ─► publish_dashboards │
                       └──────────────── any task failing after its retries ─► e-mail alert ───────────────┘
```

## Quick start

```bash
cp .env.example .env        # then fill it in (or use the .env that ships with this copy)
docker compose up -d --build
```

* Every credential lives in **`.env`** — nothing secret is hard-coded in compose, the DAG, dbt or Superset config.
* The DAG is unpaused at creation, so the first run starts by itself (incremental: only songs that are new, stale or changed in the catalog are sent to the APIs).
* Both APIs have **daily quotas** (YouTube: 10,000 units ≈ 89 songs/day; Spotify Development Mode: ≈ 600 calls/day, then `429 QUOTA_EXCEEDED` for ~23 h). A run that reaches a quota ends as `partial`; the next daily run continues where it stopped, so the whole catalog is filled in automatically over successive days.
* E-mail alerts go to `ALERT_EMAIL_TO` (fachrifa1898@gmail.com). With Gmail, put an **App Password** in `SMTP_PASSWORD`; until then alerts are recorded as `skipped` in `ops.alert_log`.

| UI | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Superset | http://localhost:8088 | `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` from `.env` |
| ClickHouse | http://localhost:8123/play | `CLICKHOUSE_ADMIN_USER` / `CLICKHOUSE_ADMIN_PASSWORD` from `.env` |

Useful commands (Git Bash on Windows: prefix with `MSYS_NO_PATHCONV=1`):

```bash
docker compose exec airflow-scheduler airflow dags trigger music_catalog_elt                         # another run
docker compose exec airflow-scheduler airflow dags trigger music_catalog_elt --conf '{"max_songs": 20}' # smoke run
docker compose exec airflow-scheduler airflow dags list-runs music_catalog_elt                         # status
docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/dags/tests -p no:cacheprovider  # unit tests
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dags/dbt/music_dw && dbt build --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"   # dbt outside Airflow
```

> If `docker compose` complains that a variable from `.env` is missing although it is in the file, your terminal exports an older copy of it (some editors inject `.env` into new terminals): open a fresh terminal or `unset` that variable — shell variables take precedence over `.env`.

## Where the answers live

| Question | Table | Columns |
|---|---|---|
| Videos per song | `marts.mart_song_overview` | `video_count`, `official_channel_video_count`, `cover_video_count`, `youtube_status` |
| ISRCs per song | `marts.mart_song_overview` | `isrc_count`, `isrc_count_original_artist`, `isrc_count_other_artists`, `spotify_status` |
| The rows behind the counts | `marts.bridge_song_recording`, `marts.bridge_song_video` | match type, confidence, version label … |
| History of changes (SCD2) | `marts.dim_song_history`, `dim_spotify_track_history`, `dim_youtube_video_history` | `version_no`, `valid_from`, `valid_to`, `is_current_version` |
| Rejected records | `dq.quarantine_records` (dashboard **Data Quarantine**) | `source`, `reject_reason`, `record_json` |

## Repository layout

```
├── docker-compose.yml          all services; every credential is read from .env
├── .env.example                template of every setting / credential
├── airflow/dags/
│   ├── music_catalog_elt.py    THE pipeline: config, e-mail, extract, pandas preprocessing,
│   │                           upsert + SCD2, quarantine, DQ checks, Superset, DAG (one file)
│   ├── dbt/music_dw/           dbt project: staging → intermediate → marts (+ tests, unit test)
│   └── tests/                  pytest unit tests of the DAG file
├── docker/                     Dockerfiles + ClickHouse / Postgres / Superset config & first-boot scripts
├── data/catalog/               export of the "[DE TS] Songs Data" sheet
```

The Python code is written without classes, comprehensions or lambdas: state lives in plain dicts and every step is an explicit loop or `if`, so the whole pipeline can be followed line by line in a debugger — and it is all in one file.
