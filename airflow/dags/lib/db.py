"""Conexiones a RAW_DATA / CLEAN_DATA y creación idempotente del esquema."""
import os
from pathlib import Path

import psycopg2

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def connect(env_var):
    """Abre una conexión usando la URL guardada en la variable de entorno indicada."""
    return psycopg2.connect(os.environ[env_var])


def ensure_schema():
    """Crea tablas de raw_data y clean_data si no existen (se puede ejecutar siempre)."""
    for env_var, sql_file in (("RAW_DB_URL", "raw_data.sql"), ("CLEAN_DB_URL", "clean_data.sql")):
        conn = connect(env_var)
        try:
            with conn, conn.cursor() as cur:
                cur.execute((SQL_DIR / sql_file).read_text())
        finally:
            conn.close()
