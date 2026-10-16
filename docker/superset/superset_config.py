"""
superset_config.py — mounted at /app/pythonpath (on Superset's PYTHONPATH).

Everything sensitive comes from environment variables injected by
docker-compose; nothing secret is hard-coded here.
"""
import os

# ---------------------------------------------------------------------------
# Core security
# ---------------------------------------------------------------------------
# Signs session cookies and encrypts stored DB passwords. Superset refuses to
# start in production mode with the default key, so we always require one.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata DB (users, charts, dashboards) -> Postgres, NOT ClickHouse.
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]

# ---------------------------------------------------------------------------
# Local-dev conveniences
# ---------------------------------------------------------------------------
# CSRF stays ON. The Airflow `publish_dashboards` task authenticates with a JWT
# and fetches a CSRF token from /api/v1/security/csrf_token/ like any client.
WTF_CSRF_ENABLED = True

# Simple in-process cache is enough for a single-container deployment.
# Production: point these at Redis.
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 600,   # dashboards refresh at most every 10 min
}

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": False,
}

# Default row limit for SQL Lab / charts (the marts are small, this is a guard)
ROW_LIMIT = 50000
SQL_MAX_ROW = 100000

# Show times in the business timezone in the UI
DEFAULT_TIMEZONE = "Asia/Jakarta"
