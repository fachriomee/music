"""
Unit tests for airflow/dags/music_catalog_elt.py — pure functions only
(no network, no ClickHouse), plus a DAG integrity test.

Run inside the scheduler container:
    docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/dags/tests -p no:cacheprovider
"""
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import music_catalog_elt as elt

NOW = datetime(2026, 10, 15, 8, 15, tzinfo=timezone.utc)
EARLIER = datetime(2026, 10, 1, 8, 15, tzinfo=timezone.utc)


def no_sleep(seconds):
    return None


# ----------------------------------------------------------------------- DAG --
def test_dag_has_every_task_and_emails_on_failure():
    dag = elt.music_catalog_elt_dag
    for task_id in ("preflight", "load_catalog", "extract_spotify", "preprocess_spotify", "extract_youtube",
                    "preprocess_youtube", "notify_quarantine", "dq_checks", "publish_dashboards"):
        assert task_id in dag.task_ids
    dbt_tasks = []
    for task in dag.tasks:
        if task.task_id.startswith("dbt_transform."):
            dbt_tasks.append(task.task_id)
            # parallel dbt tasks registering Airflow Assets collide on Airflow 3
            assert task.emit_datasets is False, task.task_id
    assert len(dbt_tasks) > 10                      # one task per dbt model + tests
    for task in dag.tasks:
        assert task.on_failure_callback, f"{task.task_id} would fail silently"
    assert dag.get_task("notify_quarantine").trigger_rule == "all_done"
    assert "full_refresh" not in dag.params          # incremental only
    assert dag.max_active_runs == 1


def test_ddl_is_one_create_table_per_statement():
    statements = elt.split_sql_statements(elt.DDL_SQL)
    assert len(statements) == 17
    for statement in statements:
        assert statement.startswith("CREATE TABLE IF NOT EXISTS")


# ---------------------------------------------------------------------- text --
def test_text_helpers():
    assert elt.clean_text("⁠SEROTONIN  6 AM ") == "SEROTONIN 6 AM"
    assert elt.query_title("LEBIH BAIK (REMASTERED)") == "LEBIH BAIK"
    assert elt.query_title("KANGEN - PIANO VERSION") == "KANGEN"
    assert elt.split_artists("ALSA, CHRIS ANDRIAN YANG") == ["ALSA", "CHRIS ANDRIAN YANG"]
    assert elt.youtube_query("KEMANA", "KOES PLUS") == "KEMANA KOES PLUS"
    assert elt.normalize_isrc("id-a12-23-00001") == "IDA122300001"
    assert elt.is_valid_isrc("IDA122300001")
    assert not elt.is_valid_isrc("IDA12230001")
    assert elt.iso_duration_seconds("PT1H2M3S") == 3723
    assert elt.parse_release_date("1974", "year") == date(1974, 1, 1)
    assert elt.parse_release_date("2016-02", "month") == date(2016, 2, 1)
    assert elt.clean_html_text("Rock &amp; Roll &#39;77") == "Rock & Roll '77"


def test_spotify_query_plan():
    cfg = {"artist_query_pages": 2, "max_pages": 3}
    plan = elt.spotify_plan_queries({"code": 1, "title": "LEBIH BAIK (REMASTERED)", "artist": "ALSA, CHRIS"}, cfg)
    assert plan == [("title_artist", 'track:"LEBIH BAIK" artist:"ALSA"', 2), ("title_only", 'track:"LEBIH BAIK"', 3)]
    plan = elt.spotify_plan_queries({"code": 2, "title": "DUSTA", "artist": None}, cfg)
    assert plan == [("title_only", 'track:"DUSTA"', 3)]


# ------------------------------------------------------------------- catalog --
def test_preprocess_catalog_standardises_validates_and_deduplicates():
    sheet = pd.DataFrame({
        "code ": [2197.0, 2197.0, None, "ABC", 13809.0, None],
        "Original Artist": ["KOES PLUS", "KOES PLUS", "NOAH", "UNGU", "8 BALL", None],
        "SONG TITLE": ["⁠KEMANA ", "KEMANA", "DOA", "DOA", "  ", None],
    }, dtype=object)
    result = elt.preprocess_catalog(sheet)
    assert result["rows_read"] == 5                                   # the fully empty row is dropped
    assert result["clean"]["song_code"].tolist() == [2197]
    assert result["clean"]["song_title"].tolist() == ["KEMANA"]       # invisible U+2060 removed
    reasons = dict(zip(result["rejected"]["source_row_number"].tolist(),
                       result["rejected"]["reject_reason"].tolist()))
    assert reasons == {2: "duplicate_code", 3: "missing_code", 4: "non_numeric_code", 5: "missing_title"}


def test_catalog_schema_contract_fails_loudly():
    with pytest.raises(ValueError, match="missing required columns"):
        elt.preprocess_catalog(pd.DataFrame({"CODE": [1], "TITLE": ["x"]}, dtype=object))


@pytest.mark.skipif(not Path(elt.get_settings()["catalog"]["file_path"]).exists(), reason="catalog not mounted")
def test_real_catalog_has_699_valid_songs():
    sheet, source_ref = elt.read_catalog_sheet(elt.get_settings()["catalog"])
    result = elt.preprocess_catalog(sheet)
    assert source_ref.endswith("#Data")
    assert len(result["clean"]) == 699
    assert len(result["rejected"]) == 0
    assert result["clean"]["original_artist"].isna().sum() == 342   # blank artist is allowed (NULL)


# ------------------------------------------------------------------- spotify --
def spotify_page(items):
    return json.dumps({"tracks": {"items": items, "limit": 10, "offset": 0, "next": None, "total": 0}})


def spotify_track(track_id, name, artist, isrc, release="1974-07-01", precision="day"):
    item = {"id": track_id, "name": name, "artists": [{"id": "1Wcaqn6Bwfs5XqSZJ9Kdrh", "name": artist}],
            "album": {"id": "44ze0ovgP3rI6Bn72QduAJ", "name": "Volume 11", "album_type": "album",
                      "release_date": release, "release_date_precision": precision},
            "duration_ms": 180000, "explicit": False, "track_number": 3, "disc_number": 1}
    if isrc is not None:
        item["external_ids"] = {"isrc": isrc}
    return item


def spotify_searches():
    return pd.DataFrame([
        {"request_id": "r1", "batch_id": "b1", "ingested_at": NOW, "song_code": 2197, "query_type": "title_artist",
         "page_offset": 0, "http_status": 200, "response_json": spotify_page([
             spotify_track("6sD4GrpYhnxfpI8dhQF1Fl", "Kemana", "Koes Plus", "qzda5-25-61958"),
             spotify_track("3CQH3xdTeXiiJ4LEp4ZQnJ", "Kemana", "Koes Plus", None, "1976", "year")])},
        {"request_id": "r2", "batch_id": "b1", "ingested_at": NOW, "song_code": 2197, "query_type": "title_only",
         "page_offset": 0, "http_status": 200, "response_json": spotify_page([
             spotify_track("6sD4GrpYhnxfpI8dhQF1Fl", "Kemana", "Koes Plus", "QZDA52561958"),
             spotify_track("not-a-track-id", "Kemana", "Andmesh", "SGB502243982"),
             spotify_track("6AVBDYXyuzy6qEqiqOIifQ", "Kemana Saja Kau Pergi", "Koes Plus", "IDA5118"),
             spotify_track("6vD1XuK9YrKjifyWMXXYll", "Kemana-Mana", "Koes Plus", None)])},
        {"request_id": "r3", "batch_id": "b1", "ingested_at": NOW, "song_code": 2197, "query_type": "title_only",
         "page_offset": 10, "http_status": 429, "response_json": ""},
    ], dtype=object)


def test_spotify_status_and_hits():
    searches = spotify_searches()
    items = elt.spotify_search_items(searches)
    assert len(items) == 6
    status = elt.spotify_search_status(searches, items).iloc[0]
    assert (int(status["calls"]), int(status["failed_calls"]), int(status["is_success"]),
            int(status["hit_count"])) == (3, 1, 1, 6)
    hits = elt.spotify_hits(items, elt.latest_batch_by_song(searches)).set_index("spotify_track_id")
    assert hits.loc["6sD4GrpYhnxfpI8dhQF1Fl", "found_by_queries"] == ["title_artist", "title_only"]
    assert hits.loc["6sD4GrpYhnxfpI8dhQF1Fl", "found_by_artist_query"] == 1
    assert hits.loc["6sD4GrpYhnxfpI8dhQF1Fl", "best_search_rank"] == 1


def test_spotify_tracks_standardised_and_quarantined():
    items = elt.spotify_search_items(spotify_searches())
    tracks = elt.spotify_tracks_frame(items, {"3CQH3xdTeXiiJ4LEp4ZQnJ": "QT3F62579438"}).set_index("spotify_track_id")
    assert tracks.loc["6sD4GrpYhnxfpI8dhQF1Fl", "isrc"] == "QZDA52561958"         # upper-cased, dashes removed
    assert tracks.loc["3CQH3xdTeXiiJ4LEp4ZQnJ", "isrc_source"] == "track_lookup"  # fallback GET /tracks/{id}
    assert tracks.loc["3CQH3xdTeXiiJ4LEp4ZQnJ", "release_date"] == date(1976, 1, 1)
    assert tracks.loc["6sD4GrpYhnxfpI8dhQF1Fl", "artist_names"] == ["Koes Plus"]
    assert pd.isna(tracks.loc["6sD4GrpYhnxfpI8dhQF1Fl", "reject_reason"])
    assert tracks.loc["not-a-track-id", "reject_reason"] == "invalid_track_id"
    assert tracks.loc["6AVBDYXyuzy6qEqiqOIifQ", "reject_reason"] == "invalid_isrc_format"
    assert tracks.loc["6vD1XuK9YrKjifyWMXXYll", "reject_reason"] == "missing_isrc"
    quarantine = elt.quarantine_frame(tracks.reset_index()[tracks.reset_index()["reject_reason"].notna()],
                                      "spotify", "spotify_track", "spotify_track_id", "record_label", "b1")
    assert sorted(quarantine["reject_reason"].tolist()) == ["invalid_isrc_format", "invalid_track_id", "missing_isrc"]
    assert json.loads(quarantine["record_json"].iloc[0])["song_codes"] == [2197]


# ------------------------------------------------------------------- youtube --
def youtube_search_page(video_ids):
    items = []
    for video_id in video_ids:
        items.append({"id": {"kind": "youtube#video", "videoId": video_id},
                      "snippet": {"title": "Koes Plus - Kemana &amp; Live", "channelId": "UCkoesplus0000000000001",
                                  "channelTitle": "Koes Plus - Topic", "publishedAt": "2019-05-01T10:00:00Z"}})
    return json.dumps({"items": items, "pageInfo": {"totalResults": len(items)}})


def test_youtube_preprocessing():
    searches = pd.DataFrame([{"batch_id": "b1", "ingested_at": NOW, "song_code": 2197, "query_text": "KEMANA KOES PLUS",
                              "http_status": 200,
                              "response_json": youtube_search_page(["KemanaVid01", "KemanaVid-2", "KemanaVid01"])}],
                            dtype=object)
    frames = elt.youtube_search_frames(searches)
    assert int(frames["status"].iloc[0]["hit_count"]) == 3
    assert frames["hits"]["video_id"].tolist() == ["KemanaVid01", "KemanaVid-2"]     # de-duplicated
    assert frames["hits"].iloc[0]["video_title"] == "Koes Plus - Kemana & Live"        # HTML unescaped
    video_rows = pd.DataFrame([{
        "batch_id": "b1", "ingested_at": NOW, "video_ids": ["KemanaVid01", "KemanaVid-2"], "http_status": 200,
        "response_json": json.dumps({"items": [{
            "id": "KemanaVid01",
            "snippet": {"title": "Kemana", "channelId": "UCkoesplus0000000000001", "channelTitle": "Koes Plus",
                        "publishedAt": "2019-05-01T10:00:00Z", "categoryId": "10"},
            "contentDetails": {"duration": "PT4M13S", "licensedContent": True},
            "statistics": {"viewCount": "1234", "likeCount": "10"}}]})}], dtype=object)
    result = elt.youtube_videos_frame(video_rows, frames["hits"])
    video = result["videos"].set_index("video_id").loc["KemanaVid01"]
    assert int(video["duration_seconds"]) == 253
    assert int(video["is_licensed_content"]) == 1
    assert int(video["view_count"]) == 1234
    assert pd.isna(video["comment_count"])
    assert result["unavailable"]["video_id"].tolist() == ["KemanaVid-2"]     # asked for, not returned


# ---------------------------------------------------------------------- SCD2 --
def catalog_version(song_code, title, artist, row_number, version_no=1):
    row = {"song_code": song_code, "song_title": title, "original_artist": artist}
    return {"song_code": song_code, "version_no": version_no, "song_title": title, "original_artist": artist,
            "source_row_number": row_number,
            "record_hash": elt.tracked_hash(pd.Series(row), elt.CATALOG_SCD["tracked"]),
            "valid_from": EARLIER, "valid_to": elt.OPEN_END, "is_current": 1, "is_deleted": 0,
            "batch_id": "b0", "updated_at": EARLIER}


def test_scd2_plan_covers_insert_update_type1_delete_and_return():
    current = pd.DataFrame([catalog_version(1, "KEMANA", "KOES PLUS", 1),
                            catalog_version(2, "BE FREE", None, 2),
                            catalog_version(3, "DUSTA", None, 3)], dtype=object)
    last_versions = pd.DataFrame({"song_code": [1, 2, 3, 5], "last_version_no": [1, 1, 1, 2]}, dtype=object)
    incoming = pd.DataFrame({"song_code": [1, 2, 4, 5],
                             "song_title": ["KEMANA", "BE FREE (LIVE)", "BAHASA CINTA", "RINDU MAMA"],
                             "original_artist": ["KOES PLUS", None, None, None],
                             "source_row_number": [7, 2, 4, 5]})
    plan = elt.plan_scd2(current, last_versions, incoming, elt.CATALOG_SCD, NOW, "b1")
    assert (plan["inserted"], plan["updated"], plan["type1_updated"], plan["deleted"], plan["unchanged"]) == (2, 1, 1, 1, 0)
    rows = plan["rows"]
    changed = rows[rows["song_code"] == 2].sort_values("version_no")
    assert changed["version_no"].tolist() == [1, 2]                  # v1 closed, v2 opened
    assert changed["is_current"].tolist() == [0, 1]
    assert changed["valid_to"].tolist()[0] == NOW
    assert changed["song_title"].tolist() == ["BE FREE", "BE FREE (LIVE)"]
    assert rows[rows["song_code"] == 1]["source_row_number"].tolist() == [7]    # type-1 overwrite, same version
    assert rows[rows["song_code"] == 1]["version_no"].tolist() == [1]
    assert rows[rows["song_code"] == 3]["is_deleted"].tolist() == [1]           # gone from the sheet
    assert rows[rows["song_code"] == 5]["version_no"].tolist() == [3]           # came back after deletion
    assert rows[rows["song_code"] == 4]["version_no"].tolist() == [1]


def test_scd2_unchanged_data_writes_nothing():
    current = pd.DataFrame([catalog_version(1, "KEMANA", "KOES PLUS", 1)], dtype=object)
    last_versions = pd.DataFrame({"song_code": [1], "last_version_no": [1]}, dtype=object)
    incoming = pd.DataFrame({"song_code": [1], "song_title": ["KEMANA"], "original_artist": ["KOES PLUS"],
                             "source_row_number": [1]})
    plan = elt.plan_scd2(current, last_versions, incoming, elt.CATALOG_SCD, NOW, "b1")
    assert len(plan["rows"]) == 0
    assert plan["unchanged"] == 1


def clickhouse_reachable():
    try:
        return elt.get_client().command("SELECT 1") == 1
    except Exception:
        return False


@pytest.mark.skipif(not clickhouse_reachable(), reason="ClickHouse not reachable")
def test_scd2_upsert_round_trip_on_clickhouse():
    """Integration test on the real engine: ReplacingMergeTree + FINAL semantics,
    in a throw-away copy of clean.catalog_songs (dropped at the end)."""
    client = elt.get_client()
    table = "dq.it_scd2_" + uuid.uuid4().hex[:10]
    spec = dict(elt.CATALOG_SCD)
    spec["table"] = table
    codes = [1, 2, 3, 4, 5]
    titles = ["KEMANA", "DUSTA", "BE FREE", "RINDU MAMA", "BAHASA CINTA"]

    def snapshot(changed_titles, keep_codes):
        rows = {"song_code": [], "song_title": [], "original_artist": [], "source_row_number": []}
        for index in range(len(codes)):
            if codes[index] in keep_codes:
                rows["song_code"].append(codes[index])
                rows["song_title"].append(changed_titles.get(codes[index], titles[index]))
                rows["original_artist"].append(None)
                rows["source_row_number"].append(index + 1)
        return pd.DataFrame(rows)

    client.command(f"CREATE TABLE {table} AS clean.catalog_songs")
    try:
        first = elt.scd2_upsert(client, spec, snapshot({}, codes), "it_b1")
        again = elt.scd2_upsert(client, spec, snapshot({}, codes), "it_b2")
        edited = elt.scd2_upsert(client, spec, snapshot({1: "KEMANA (LIVE)"}, codes), "it_b3")
        removed = elt.scd2_upsert(client, spec, snapshot({1: "KEMANA (LIVE)"}, [1, 2, 3, 4]), "it_b4")
        back = elt.scd2_upsert(client, spec, snapshot({1: "KEMANA (LIVE)"}, codes), "it_b5")
        assert (first["inserted"], again["rows_written"], edited["updated"], removed["deleted"], back["inserted"]) == (5, 0, 1, 1, 1)
        history = elt.query_df(client, f"FROM {table} FINAL ORDER BY song_code, version_no",
                               ["song_code", "version_no", "song_title", "is_current", "is_deleted"])
        rows = []
        for row in history.itertuples(index=False):
            rows.append((int(row.song_code), int(row.version_no), row.song_title, int(row.is_current), int(row.is_deleted)))
        assert rows[:2] == [(1, 1, "KEMANA", 0, 0), (1, 2, "KEMANA (LIVE)", 1, 0)]       # update = close + new version
        assert (5, 1, "BAHASA CINTA", 0, 1) in rows and (5, 2, "BAHASA CINTA", 1, 0) in rows  # deleted, then back
        assert len(rows) == 7                                                              # unchanged runs wrote nothing
    finally:
        client.command(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.skipif(not clickhouse_reachable(), reason="ClickHouse not reachable")
def test_quarantine_email_flow_on_clickhouse(monkeypatch):
    """Defective sheet rows -> quarantine rows (throw-away copy of dq.quarantine_records)
    -> notify_quarantine builds the e-mail with counts by reason."""
    client = elt.get_client()
    table = "dq.it_quarantine_" + uuid.uuid4().hex[:10]
    sheet = pd.DataFrame({"CODE": [2197.0, 2197.0, None, 13809.0],
                          "ORIGINAL ARTIST": ["KOES PLUS", "KOES PLUS", "NOAH", "8 BALL"],
                          "SONG TITLE": ["KEMANA", "KEMANA LAGI", "DOA", " "]}, dtype=object)
    rejected = elt.preprocess_catalog(sheet)["rejected"]
    frame = elt.quarantine_frame(rejected, "catalog", "catalog_song", "record_key", "record_label", "it_q1")
    sent = []

    def fake_send(subject, text_body, html_body, alert_type, batch_id):
        sent.append({"subject": subject, "text": text_body, "type": alert_type})
        return "sent"

    monkeypatch.setattr(elt, "send_email", fake_send)
    client.command(f"CREATE TABLE {table} AS dq.quarantine_records")
    try:
        elt.insert_df(client, table, frame, elt.QUARANTINE_COLUMNS)
        elt.insert_df(client, table, frame, elt.QUARANTINE_COLUMNS)      # a retry must not duplicate
        result = elt.run_notify_quarantine("it_q1", table=table)
        assert result == {"quarantined": 3, "new": 3, "email": "sent"}
        assert sent[0]["subject"] == "[Massive Music ELT] QUARANTINE: 3 record(s) rejected (3 new)"
        assert "duplicate_code" in sent[0]["text"] and "missing_title" in sent[0]["text"]
        assert elt.run_notify_quarantine("no_such_batch", table=table)["email"] == "not needed"
    finally:
        client.command(f"DROP TABLE IF EXISTS {table}")


def test_scd2_mass_delete_guard():
    current = pd.DataFrame([catalog_version(1, "A", None, 1), catalog_version(2, "B", None, 2),
                            catalog_version(3, "C", None, 3)], dtype=object)
    last_versions = pd.DataFrame({"song_code": [1, 2, 3], "last_version_no": [1, 1, 1]}, dtype=object)
    incoming = pd.DataFrame({"song_code": [1], "song_title": ["A"], "original_artist": [None], "source_row_number": [1]})
    with pytest.raises(RuntimeError, match="SCD2 guard"):
        elt.plan_scd2(current, last_versions, incoming, elt.CATALOG_SCD, NOW, "b1", None, 0.2)


# ---------------------------------------------------------------------- HTTP --
def fake_session(statuses, headers=None):
    calls = []
    if headers is None:
        headers = {}

    def request(method, url, timeout, **kwargs):
        calls.append(url)
        return SimpleNamespace(status_code=statuses[len(calls) - 1], text='{"ok": true}', headers=headers)

    return SimpleNamespace(request=request, calls=calls)


def test_http_gives_up_at_once_on_a_daily_quota(monkeypatch):
    """Spotify Development Mode: 429 QUOTA_EXCEEDED with Retry-After ~23 h -> no waiting."""
    monkeypatch.setattr(elt.time, "sleep", no_sleep)
    session = fake_session([429, 200], {"Retry-After": "82029"})
    result = elt.http_request(session, "GET", "https://api.spotify.com/v1/search")
    assert result["status"] == 429 and result["retry_after"] == 82029
    assert len(session.calls) == 1


def test_spotify_page_info_detects_irrelevant_pages():
    page = spotify_page([spotify_track("6sD4GrpYhnxfpI8dhQF1Fl", "Kemana - Live", "Koes Plus", "QZDA52561958"),
                         spotify_track("6vD1XuK9YrKjifyWMXXYll", "Kemana-Mana", "Koes Plus", None)])
    info = elt.spotify_page_info(page, elt.compare_key(elt.query_title("KEMANA")))
    assert (info["items"], info["title_hits"], info["missing_isrc"]) == (2, 2, ["6vD1XuK9YrKjifyWMXXYll"])
    noise = elt.spotify_page_info(page, elt.compare_key("BERTAHAN HIDUP"))
    assert noise["title_hits"] == 0                       # -> paging stops, quota saved


def test_http_retries_server_errors(monkeypatch):
    monkeypatch.setattr(elt.time, "sleep", no_sleep)
    session = fake_session([503, 429, 200])
    result = elt.http_request(session, "GET", "https://api.spotify.com/v1/search")
    assert result["ok"]
    assert len(session.calls) == 3


def test_http_does_not_retry_client_errors(monkeypatch):
    monkeypatch.setattr(elt.time, "sleep", no_sleep)
    session = fake_session([400])
    result = elt.http_request(session, "GET", "https://api.spotify.com/v1/search")
    assert result["status"] == 400 and not result["ok"]
    assert len(session.calls) == 1


def test_circuit_breaker_trips_after_consecutive_failures():
    breaker = elt.new_circuit_breaker("spotify", threshold=3)
    elt.circuit_breaker_record(breaker, False)
    elt.circuit_breaker_record(breaker, True)            # a success resets the count
    elt.circuit_breaker_record(breaker, False)
    elt.circuit_breaker_record(breaker, False)
    with pytest.raises(RuntimeError, match="circuit breaker"):
        elt.circuit_breaker_record(breaker, False)


def test_quota_budget_reserves_search_and_details():
    ledger = {"budget": 9000, "spent": 8899}
    assert elt.quota_can_spend(ledger, elt.SEARCH_COST + elt.VIDEOS_COST)
    ledger["spent"] = 8900
    assert not elt.quota_can_spend(ledger, elt.SEARCH_COST + elt.VIDEOS_COST)
    quota_error = {"status": 403, "text": json.dumps({"error": {"errors": [{"reason": "quotaExceeded"}]}})}
    assert elt.youtube_is_quota_exceeded(quota_error)


# --------------------------------------------------------------------- email --
def test_email_needs_credentials_except_on_a_local_relay():
    cfg = {"smtp_host": "smtp.gmail.com", "to": ["fachrifa1898@gmail.com"], "security": "starttls",
           "user": "fachrifa1898@gmail.com", "password": ""}
    assert not elt.email_can_send(cfg)
    cfg["password"] = "app-password"
    assert elt.email_can_send(cfg)
    relay = {"smtp_host": "mailpit", "to": ["fachrifa1898@gmail.com"], "security": "none", "user": "", "password": ""}
    assert elt.email_can_send(relay)


def test_failure_email_names_task_error_and_log_link(monkeypatch):
    sent = []

    def fake_send(subject, text_body, html_body, alert_type, batch_id):
        sent.append({"subject": subject, "text": text_body, "html": html_body, "type": alert_type})
        return "sent"

    monkeypatch.setattr(elt, "send_email", fake_send)
    context = {"ti": SimpleNamespace(dag_id="music_catalog_elt", task_id="extract_spotify",
                                     run_id="manual__2026-10-15T08:15:00+00:00", try_number=3),
               "exception": RuntimeError("Spotify token request failed: HTTP 401")}
    elt.notify_task_failure(context)
    assert sent[0]["subject"] == "[Massive Music ELT] FAILED: music_catalog_elt.extract_spotify"
    assert sent[0]["type"] == "task_failure"
    assert "HTTP 401" in sent[0]["text"]
    assert "/runs/manual__2026-10-15T08%3A15%3A00%2B00%3A00/tasks/extract_spotify" in sent[0]["html"]


def test_dashboards_include_data_quarantine():
    slugs = []
    for spec in elt.dashboard_specs():
        slugs.append(spec["slug"])
        for chart in spec["charts"]:
            assert chart["dataset"].startswith("marts.")          # the BI role only reads marts/ops/dq
    assert slugs == ["song-catalog", "pipeline-health", "data-quarantine"]
