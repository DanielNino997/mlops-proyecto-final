"""Escritura del historial de cada corrida en clean_data.audit.batch_history (lo lee Streamlit)."""
from psycopg2.extras import Json

from lib.db import connect

COLUMNS = ["batch_id", "group_number", "batch_number", "airflow_run_id", "n_rows_raw", "n_rows_clean",
           "validations", "new_categories", "drift", "trained", "train_reason", "mlflow_run_id",
           "model_version", "promoted", "decision_reason", "candidate_mae", "candidate_rmse",
           "champion_mae", "champion_rmse", "delta_mae_pct", "final_status", "alertas"]
JSON_COLUMNS = {"validations", "new_categories", "drift", "alertas"}


def write_history(row):
    cols = [c for c in COLUMNS if row.get(c) is not None]
    values = [Json(row[c]) if c in JSON_COLUMNS else row[c] for c in cols]
    conn = connect("CLEAN_DB_URL")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"INSERT INTO audit.batch_history ({', '.join(cols)}) "
                        f"VALUES ({', '.join(['%s'] * len(cols))})", values)
    finally:
        conn.close()
