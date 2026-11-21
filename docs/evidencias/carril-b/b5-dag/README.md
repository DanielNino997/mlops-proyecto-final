# B5 — DAG `pipeline_lotes`

## Qué se hizo
- `airflow/dags/pipeline_lotes.py`: 19 tareas en el orden del enunciado, cada una llama una función de `lib/`.
  Dos bifurcaciones: `decide_training` (reglas de `lib/decision.py`) y `decide_promotion` (MAE −3 % / RMSE +1 %).
- Los lotes no viajan por XCom (hasta 60 MB): se guardan en STAGING_DIR; por XCom van solo resúmenes.
  Entrenar / evaluar / registrar son tareas separadas; el modelo pasa entre ellas como `model.joblib`.
- Robustez: reintentos en la tarea de red, `max_active_runs=1`, fin de datos = corrida `skipped` (no error),
  lote duplicado = no se reprocesa, y cualquier fallo real queda en el historial como `fallido` (callback).
- `notify_or_log_result` escribe cada corrida en `clean_data.audit.batch_history` (lo lee Streamlit).
- `airflow/Dockerfile` (Airflow 2.10.5 + mismas versiones de numpy/pandas/sklearn/scipy del entorno de validación)
  y workflow `.github/workflows/airflow-image.yml` que prueba `lib/` y publica `dnino97/mlops-pf-airflow`.

## Resultado (7 corridas en Airflow local, 01-corridas.txt y 02-audit-batch-history.txt)
| Lote | Estado final | Detalle |
|---|---|---|
| 0 | promovido v1 | línea base |
| 1 | promovido v2 | MAE −7,63 % |
| 2 | promovido v3 | MAE −7,67 % |
| 3 | no_entrena | lote insuficiente (+1,02 %) aunque hubo drift y estado nuevo |
| 4 | promovido v4 | MAE −44,77 % |
| 5 | promovido v5 | MAE −51,53 % |
| — | sin_datos | la API ya entregó todos los lotes |

Las decisiones coinciden exactamente con la simulación de B4: el pipeline es reproducible.
03-grilla-dag.png muestra las dos bifurcaciones en la grilla de Airflow.
