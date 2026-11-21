"""Almacenamiento de lotes crudos en RAW_DATA, sin transformar e idempotente."""
import csv
import hashlib
import io
import json
import logging
import math

log = logging.getLogger(__name__)


def payload_hash(records):
    """Huella sha256 del contenido del lote (para auditoría)."""
    raw = json.dumps(records, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def _json_safe(record):
    """NaN no es JSON válido en Postgres: se guarda como null. Es la única transformación."""
    return {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in record.items()}


def store_raw_batch(conn, body, airflow_run_id=None):
    """Guarda el lote completo en una transacción. Si el lote ya existe, no inserta nada."""
    group, batch, records = body["group_number"], body["batch_number"], body["data"]
    digest = payload_hash(records)
    columns = sorted({key for rec in records for key in rec})

    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO batches (group_number, batch_number, n_rows, payload_hash, columns, airflow_run_id)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (group_number, batch_number) DO NOTHING
               RETURNING id""",
            (group, batch, len(records), digest, json.dumps(columns), airflow_run_id),
        )
        row = cur.fetchone()

        if row is None:
            cur.execute(
                "SELECT id, payload_hash FROM batches WHERE group_number = %s AND batch_number = %s",
                (group, batch),
            )
            batch_id, old_hash = cur.fetchone()
            log.warning("Lote %s del grupo %s ya estaba guardado (batch_id=%s): no se reinserta",
                        batch, group, batch_id)
            return {"batch_id": batch_id, "group_number": group, "batch_number": batch,
                    "n_rows": len(records), "duplicate": True, "same_payload": old_hash.strip() == digest}

        batch_id = row[0]
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        for i, rec in enumerate(records):
            writer.writerow([batch_id, i, json.dumps(_json_safe(rec))])
        buffer.seek(0)
        cur.copy_expert(
            "COPY properties_raw (batch_id, row_number, record) FROM STDIN WITH (FORMAT csv)", buffer
        )

    log.info("Lote %s del grupo %s guardado en RAW: batch_id=%s, %s filas",
             batch, group, batch_id, len(records))
    return {"batch_id": batch_id, "group_number": group, "batch_number": batch,
            "n_rows": len(records), "duplicate": False, "payload_hash": digest}
