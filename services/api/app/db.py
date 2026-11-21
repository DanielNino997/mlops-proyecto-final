"""Registro de cada inferencia en raw_data.inference_events (RF8). Nunca tumba la predicción."""
import json
import logging
import os

from psycopg2.pool import ThreadedConnectionPool

log = logging.getLogger(__name__)
_pool = None


def _get_pool():
    global _pool
    if _pool is None and os.environ.get("RAW_DB_URL"):
        _pool = ThreadedConnectionPool(1, 5, os.environ["RAW_DB_URL"])
    return _pool


def log_inference(payload, prediction, model_name, model_version, status, error, latency_ms):
    try:
        pool = _get_pool()
        if pool is None:
            return
        conn = pool.getconn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO inference_events (input, prediction, model_name, model_version, status, error, latency_ms)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (json.dumps(payload, default=str), prediction, model_name, model_version, status, error, latency_ms))
        finally:
            pool.putconn(conn)
    except Exception as exc:  # noqa: BLE001
        log.warning("No se pudo registrar la inferencia: %s", exc)
