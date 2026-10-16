#!/bin/bash
# =============================================================================
# Postgres first-boot: create a dedicated role + database for Superset.
# Airflow uses the default POSTGRES_DB created by the image itself.
# One Postgres server, two isolated databases = less RAM, same separation.
# =============================================================================
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE ${SUPERSET_DB_USER} WITH LOGIN PASSWORD '${SUPERSET_DB_PASSWORD}';
  CREATE DATABASE ${SUPERSET_DB_NAME} OWNER ${SUPERSET_DB_USER};
EOSQL

echo "[postgres-init] superset database '${SUPERSET_DB_NAME}' created."
