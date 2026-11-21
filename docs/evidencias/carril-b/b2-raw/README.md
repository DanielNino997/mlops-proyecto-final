# B2 — Tablas RAW/CLEAN e ingesta del lote

## Qué se hizo
- `airflow/dags/sql/raw_data.sql` y `clean_data.sql`: tablas creadas con `IF NOT EXISTS` (idempotente).
- `lib/storage.py` → `store_raw_batch()`: guarda cada registro completo como JSONB (sin transformar, salvo NaN→null),
  con `COPY` y en una sola transacción. La llave (group_number, batch_number) es UNIQUE: un lote ya guardado no se reinserta.
- `scripts/ingest_once.py`: prueba sin Airflow contra la API local.

## Resultado
- 01: lotes 0, 1 y 2 guardados en 3,6 s / 4,3 s / 10,7 s.
- 02: RAW (batches, properties_raw, inference_events) y CLEAN (properties_clean, audit.batch_history) separadas (RF2).
- 03: filas declaradas = filas guardadas en cada lote (trazabilidad por batch_id).
- 04: repetir el lote 0 no inserta nada; total RAW se mantiene en 398.701 (RF1).
- 05: la fila guardada es idéntica al registro que entregó la API (RF2).

## Hallazgo
`same_payload: True`: tras reiniciar, la API devuelve exactamente los mismos datos → es determinista, y la corrida
real del grupo 2 debería producir los mismos lotes vistos en local.
