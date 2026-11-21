# B6 — API de inferencia (FastAPI)

## Qué se hizo
- Endpoints: `/live` (liveness), `/health` (readiness: 503 si no hay modelo), `/predict`, `/metrics`,
  `POST /admin/reload` (header `X-Admin-Token`, valor en un Secret).
- El modelo sale de MLflow por alias: `models:/house-price-model@champion`. No hay rutas, archivos ni versiones
  escritas en el código.
- Recarga sin redespliegue (RF7): un hilo revisa el alias cada 30 s y `/admin/reload` fuerza la revisión.
  El modelo nuevo se carga aparte y se intercambia con un lock cuando está completo; las peticiones en curso
  terminan con el modelo con el que empezaron. Si la descarga falla, se mantiene el anterior (`last_error`).
- Cada inferencia (ok, invalid, error) se registra en `raw_data.inference_events` en segundo plano (RF8).
- Métricas: `predict_requests_total{status}`, `predict_latency_seconds`, `model_version`, `model_loaded`,
  `model_reload_total{result}`.
- `app/features.py` transforma igual que el DAG: `test_paridad_con_el_dag` lo verifica.

## Resultado
- 01: 8 pruebas en verde.  02–05: health (v5), predicción, entrada inválida → 422, métricas.
- 06 (RF7): alias v5→v4 con `/admin/reload` → la API pasa a v4; alias v4→v5 sin tocar la API → el sondeo la
  devuelve a v5. Mismo `StartedAt` y `RestartCount=0` al inicio y al final: nunca se reinició.
- 07 (RF8): inferencias ok e inválidas registradas con versión del modelo y latencia.

## Decisión: latencia (08-latencia.txt)
La primera versión tardaba ~230 ms por predicción. La medición por partes mostró que el 90 % era
`TargetEncoder.transform` de sklearn, que reconstruye en cada llamada un diccionario con todas las categorías
vistas (decenas de miles de ciudades y zips). `app/fast_model.py` (FastPredictor) construye esos diccionarios
una sola vez al cargar el modelo y usa los mismos parámetros aprendidos: misma predicción (581.314,59),
~25 ms por petición (9 veces menos). `test_fast_predictor_igual_al_modelo` lo verifica, incluidas categorías nuevas.
