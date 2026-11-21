"""Lectura/escritura en RAW y CLEAN + procesamiento completo de un lote."""
import io
import logging

import pandas as pd

from lib.db import connect
from lib.preprocess import CLEAN_COLUMNS, preprocess
from lib.validation import (CATEGORICAL, DRIFT_COLS, detect_drift, detect_new_categories,
                            validate_quality, validate_schema)

log = logging.getLogger(__name__)


def load_raw_batch(batch_id):
    conn = connect("RAW_DB_URL")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT group_number, batch_number, n_rows FROM batches WHERE id = %s", (batch_id,))
            meta = dict(zip(("group_number", "batch_number", "n_rows"), cur.fetchone()))
            cur.execute("SELECT raw_row_id, record FROM properties_raw WHERE batch_id = %s ORDER BY row_number",
                        (batch_id,))
            rows = cur.fetchall()
    finally:
        conn.close()
    df = pd.DataFrame([r[1] for r in rows])
    df.insert(0, "raw_row_id", [r[0] for r in rows])
    return meta, df


def load_known_categories(exclude_batch_id):
    conn = connect("CLEAN_DB_URL")
    known = {}
    try:
        with conn.cursor() as cur:
            for c in CATEGORICAL:
                cur.execute(f"SELECT DISTINCT {c} FROM properties_clean WHERE batch_id <> %s AND {c} IS NOT NULL",
                            (exclude_batch_id,))
                known[c] = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()
    return known


def load_reference_sample(exclude_batch_id, n=200_000):
    conn = connect("CLEAN_DB_URL")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {', '.join(DRIFT_COLS)} FROM properties_clean WHERE batch_id <> %s "
                        f"ORDER BY random() LIMIT {int(n)}", (exclude_batch_id,))
            return pd.DataFrame(cur.fetchall(), columns=DRIFT_COLS, dtype="float64")
    finally:
        conn.close()


def count_clean(exclude_batch_id=None):
    conn = connect("CLEAN_DB_URL")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM properties_clean WHERE batch_id <> %s", (exclude_batch_id or -1,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def write_clean(batch_id, clean_df):
    """Reemplaza las filas CLEAN del lote (reprocesar el mismo lote no duplica)."""
    df = clean_df.copy()
    df.insert(1, "batch_id", batch_id)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)
    conn = connect("CLEAN_DB_URL")
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM properties_clean WHERE batch_id = %s", (batch_id,))
            cols = ", ".join(["raw_row_id", "batch_id"] + CLEAN_COLUMNS[1:])
            cur.copy_expert(f"COPY properties_clean ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')", buffer)
    finally:
        conn.close()
    return len(df)


def process_batch(batch_id):
    """Valida, limpia y guarda un lote. Devuelve el resumen que usará la decisión de entrenamiento."""
    meta, raw = load_raw_batch(batch_id)
    features = raw.drop(columns=["raw_row_id"])
    summary = {"batch_id": batch_id, **meta, "n_rows_raw": len(raw),
               "schema": validate_schema(features), "quality": validate_quality(features)}

    if not (summary["schema"]["ok"] and summary["quality"]["ok"]):
        summary.update(status="invalido", n_rows_clean=0)
        log.warning("Lote %s inválido: no se escribe en CLEAN", meta["batch_number"])
        return summary

    clean = preprocess(raw)
    before = count_clean(exclude_batch_id=batch_id)
    summary["new_categories"] = detect_new_categories(clean, load_known_categories(batch_id))
    summary["drift"] = detect_drift(clean, load_reference_sample(batch_id) if before else None)
    summary["n_rows_clean"] = write_clean(batch_id, clean)
    summary["n_total_clean_before"] = before
    summary["n_total_clean_after"] = before + summary["n_rows_clean"]
    summary["volume_increase_pct"] = round(summary["n_rows_clean"] / before * 100, 2) if before else None
    summary["status"] = "valido"
    return summary
