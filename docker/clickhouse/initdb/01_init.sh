#!/bin/bash
# =============================================================================
# ClickHouse first-boot initialisation (runs ONCE, when the data dir is empty)
# -----------------------------------------------------------------------------
# Creates the layered databases and the least-privilege principals.
# Table DDL is NOT here on purpose: it lives in the DAG file (SECTION 5 of
# airflow/dags/music_catalog_elt.py) and is applied idempotently by the
# preflight task, so schema changes ship with the code, not with a volume reset.
# =============================================================================
set -euo pipefail

echo "[clickhouse-init] creating databases, profiles, users and grants ..."

clickhouse client \
  --user "${CLICKHOUSE_USER}" \
  --password "${CLICKHOUSE_PASSWORD}" \
  --multiquery <<SQL
-- ---------------------------------------------------------------------------
-- Layered databases (ClickHouse "database" == dbt "schema")
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS raw          COMMENT 'Bronze: API responses exactly as received (append-only)';
CREATE DATABASE IF NOT EXISTS clean        COMMENT 'Silver: pandas-validated rows, upserted (ReplacingMergeTree), SCD Type 2';
CREATE DATABASE IF NOT EXISTS dq           COMMENT 'Quarantined records + stored dbt test failures';
CREATE DATABASE IF NOT EXISTS ops          COMMENT 'Run log, API quota ledger, DQ results, watermarks, alert log';
CREATE DATABASE IF NOT EXISTS staging      COMMENT 'dbt: current SCD2 versions + matching keys';
CREATE DATABASE IF NOT EXISTS intermediate COMMENT 'dbt: song <-> recording / video matching';
CREATE DATABASE IF NOT EXISTS marts        COMMENT 'dbt: dimensional model, history, analytics marts (BI-facing)';

-- ---------------------------------------------------------------------------
-- Settings profiles: guard-rails per workload
-- ---------------------------------------------------------------------------
CREATE SETTINGS PROFILE IF NOT EXISTS etl_profile
  SETTINGS max_execution_time = 900,
           max_memory_usage   = 1200000000;

CREATE SETTINGS PROFILE IF NOT EXISTS bi_profile
  SETTINGS max_execution_time = 60,          -- dashboards must stay snappy
           max_memory_usage   = 600000000,
           max_result_rows    = 1000000,
           result_overflow_mode = 'break';

-- ---------------------------------------------------------------------------
-- etl: Airflow EL tasks + dbt
-- ---------------------------------------------------------------------------
CREATE USER IF NOT EXISTS ${CLICKHOUSE_ETL_USER}
  IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_ETL_PASSWORD}'
  SETTINGS PROFILE 'etl_profile';

GRANT ALL ON raw.*          TO ${CLICKHOUSE_ETL_USER};
GRANT ALL ON clean.*        TO ${CLICKHOUSE_ETL_USER};
GRANT ALL ON ops.*          TO ${CLICKHOUSE_ETL_USER};
GRANT ALL ON staging.*      TO ${CLICKHOUSE_ETL_USER};
GRANT ALL ON intermediate.* TO ${CLICKHOUSE_ETL_USER};
GRANT ALL ON marts.*        TO ${CLICKHOUSE_ETL_USER};
GRANT ALL ON dq.*           TO ${CLICKHOUSE_ETL_USER};
-- dbt-clickhouse uses temporary tables for some materialisations
GRANT CREATE TEMPORARY TABLE ON *.* TO ${CLICKHOUSE_ETL_USER};

-- ---------------------------------------------------------------------------
-- superset_ro: BI, read-only - curated marts + monitoring (ops) + quarantine (dq).
-- No access to raw / clean: analysts only see modelled, validated data.
-- ---------------------------------------------------------------------------
CREATE USER IF NOT EXISTS ${CLICKHOUSE_BI_USER}
  IDENTIFIED WITH sha256_password BY '${CLICKHOUSE_BI_PASSWORD}'
  SETTINGS PROFILE 'bi_profile';

GRANT SELECT ON marts.* TO ${CLICKHOUSE_BI_USER};
GRANT SELECT ON ops.*   TO ${CLICKHOUSE_BI_USER};
GRANT SELECT ON dq.*    TO ${CLICKHOUSE_BI_USER};
SQL

echo "[clickhouse-init] done."
