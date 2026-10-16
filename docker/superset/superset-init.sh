#!/bin/bash
# =============================================================================
# One-shot Superset bootstrap (runs before the web server starts)
#   1. migrate the metadata DB schema
#   2. create the admin user (no-op if it already exists)
#   3. sync roles/permissions
# Dashboards/datasets are NOT created here: the marts they point at only exist
# after the first pipeline run, so Airflow's `publish_dashboards` task creates
# them idempotently via the Superset REST API (SECTION 19 of the DAG file).
# =============================================================================
set -euo pipefail

echo "[superset-init] upgrading metadata database ..."
superset db upgrade

echo "[superset-init] ensuring admin user '${SUPERSET_ADMIN_USER}' ..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --firstname Admin --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true   # exits non-zero if user exists

echo "[superset-init] syncing roles and permissions ..."
superset init

echo "[superset-init] done."
