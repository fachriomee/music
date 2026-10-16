# =============================================================================
# Superset 6 + drivers
# -----------------------------------------------------------------------------
# The official image ships WITHOUT database drivers. We add:
#   * clickhouse-connect -> ClickHouse SQLAlchemy dialect (clickhousedb://)
#   * psycopg2-binary    -> Superset's own metadata DB lives in Postgres
# The app runs from /app/.venv, so we install into that interpreter with uv.
# =============================================================================
FROM apache/superset:6.0.0

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python \
        "clickhouse-connect==1.10.0" \
        "psycopg2-binary>=2.9,<3"

COPY superset-init.sh /app/docker/custom/superset-init.sh
RUN chmod +x /app/docker/custom/superset-init.sh

USER superset
