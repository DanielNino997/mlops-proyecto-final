import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "services" / "api"))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))     # para la prueba de paridad con el DAG
os.environ["ADMIN_TOKEN"] = "token-de-prueba"
