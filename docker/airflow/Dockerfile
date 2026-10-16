# =============================================================================
# Custom Airflow image = official Airflow 3.3.2 + EL dependencies + isolated dbt
# -----------------------------------------------------------------------------
# Two Python environments live in this image ON PURPOSE:
#
#   1. Airflow's own env  -> Airflow, Cosmos, clickhouse-connect, requests ...
#   2. /opt/dbt_venv      -> dbt-core + dbt-clickhouse ONLY
#
# dbt and Airflow pin overlapping libraries (protobuf, jinja2, pydantic ...).
# Installing them in the same env is the #1 source of "works on my machine"
# breakage, so Cosmos is told to call the dbt binary from the separate venv
# (ExecutionConfig(dbt_executable_path="/opt/dbt_venv/bin/dbt")).
# =============================================================================
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION=3.3.2

# -- 1) Airflow-side dependencies --------------------------------------------
# Pinning apache-airflow to the image version in the same command stops pip
# from silently upgrading/downgrading Airflow while resolving our requirements.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        -r /tmp/requirements-airflow.txt

# -- 2) dbt in its own virtualenv ---------------------------------------------
# /opt is root-owned and the base image runs as `airflow`: pre-create the
# venv dir as root and hand it over (group 0 = OpenShift-style arbitrary UID).
USER root
RUN mkdir -p /opt/dbt_venv && chown -R airflow:0 /opt/dbt_venv && chmod -R g+rwX /opt/dbt_venv
USER airflow

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt_venv \
 && /opt/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && /opt/dbt_venv/bin/dbt --version

# Handy on the CLI: `docker compose exec airflow-scheduler dbt ...`
ENV PATH="${PATH}:/opt/dbt_venv/bin"
