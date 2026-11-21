# B7 — Streamlit y Locust

## Streamlit (`services/streamlit/`) — RF9
- Pestaña **Inferencia**: formulario → `POST /predict`; muestra precio estimado, **versión del modelo usada** y
  latencia. Arriba siempre se ve qué versión tiene cargada la API (`/health`).
- Pestaña **Historial de entrenamiento y despliegue**: lee `clean_data.audit.batch_history` (lo escribe el DAG).
  Una frase por lote (`historial.py`, con pruebas), contadores de lotes / entrenamientos / promociones / rechazos,
  gráfica del MAE del candidato vs el campeón y tabla con versión, Δ MAE % y link al run de MLflow.
- Variables: `API_URL`, `CLEAN_DB_URL`, `MLFLOW_UI_URL`. Puerto 8501; health en `/_stcore/health`.
- 00: pruebas en verde. 01–02: capturas de las dos pestañas (inferencia con v5; historial de los 6 lotes + fin de datos).
- Acceso desde el computador por túnel SSH (`ssh -L 8501:localhost:8501 ...`): el firewall de la VM no expone 8501.

## Locust (`services/locust/`) — RF10
- `locustfile.py`: 9 de cada 10 peticiones a `/predict` con **500 propiedades reales** sacadas de CLEAN
  (`scripts/export_locust_payloads.py` → `payloads.json`), 1 de cada 10 a `/health`. Espera de 0,5–2 s por usuario.
- Imagen `locustio/locust` + locustfile; el destino se pasa con `LOCUST_HOST`; UI en el puerto 8089.

## Resultado de la prueba local (03-locust-local.txt) — 30 usuarios, 60 s, 1 réplica de la API (1 worker)
| Indicador | Valor |
|---|---|
| Peticiones a /predict | 1.118, **0 fallos** |
| Throughput | ~18,8 predicciones/s |
| Latencia (Locust) | mediana 120 ms, p95 480 ms, máx. 990 ms |
| `predict_requests_total{ok}` en Prometheus | 6 → 1.124 (+1.118, igual a Locust) |
| Latencia interna de inferencia bajo carga | ~141 ms promedio (vs ~25 ms con una sola petición) |

**Lectura:** con un solo proceso, las peticiones simultáneas compiten por la CPU y hacen cola: la latencia sube
aunque no hay errores. La API usa 1 worker a propósito (modelo y métricas consistentes en un proceso); para escalar
se agregan réplicas en Kubernetes. En el clúster se repite la prueba con 1 y con varias réplicas y se muestra el
efecto en Grafana (peticiones/s, latencia, errores y CPU/memoria del pod).
