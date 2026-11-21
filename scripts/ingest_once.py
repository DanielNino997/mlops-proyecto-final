"""Prueba local de B2: pide N lotes a la API de datos y los guarda en RAW_DATA."""
import argparse
import logging
import os
import sys
import time

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
from lib.api_client import NoMoreBatches, fetch_batch  # noqa: E402
from lib.db import connect, ensure_schema  # noqa: E402
from lib.storage import store_raw_batch  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument("-n", type=int, default=1, help="cuántos lotes pedir")
parser.add_argument("--restart", action="store_true", help="reiniciar la API local antes")
args = parser.parse_args()

api, group = os.environ["DATA_API_URL"], int(os.environ["GROUP_NUMBER"])
if args.restart:
    print(requests.get(f"{api}/restart_data_generation", params={"group_number": group}).json())

ensure_schema()
conn = connect("RAW_DB_URL")
try:
    for _ in range(args.n):
        try:
            body = fetch_batch(api, group)
        except NoMoreBatches as exc:
            print(f"Fin de datos: {exc}")
            break
        t0 = time.time()
        result = store_raw_batch(conn, body, airflow_run_id="manual-ingest_once")
        print(f"{result}  ({time.time() - t0:.1f} s)")
finally:
    conn.close()
