"""Make the DAG file importable as `music_catalog_elt` (Airflow puts the dags
folder on sys.path at runtime; pytest does not)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
