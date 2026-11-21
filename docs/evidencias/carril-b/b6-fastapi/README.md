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
- `app/features.py` transforma la entrada igual que el DAG: `test_paridad_con_el_dag` lo verifica.

## Resultado
- 01: 8 pruebas en verde.  02–05: health (v5), predicción, entrada inválida → 422, métricas.
- 06 (RF7): alias v5→v4 con `/admin/reload` → la API pasa a v4; alias v4→v5 sin tocar la API → el sondeo la
  devuelve a v5. Mismo `StartedAt` y `RestartCount=0` al inicio y al final: nunca se reinició.
- 07 (RF8): inferencias ok e inválidas registradas con versión del modelo y latencia.

## Optimización de la latencia (08-latencia.txt, 09-perfil-latencia.txt)

### El problema
La primera versión tardaba **~230 ms por predicción de una sola fila**. Con eso la API se satura con muy poca
carga, y la prueba de Locust (RF10) mostraría un cuello de botella del código, no de la infraestructura.

### Primera hipótesis, descartada: hilos de OpenMP
HistGradientBoosting usa OpenMP, y con una fila el costo de coordinar hilos puede superar al cálculo. Se fijó
`OMP_NUM_THREADS=1` en la imagen: **la latencia no cambió** (siguió en ~220-250 ms). Se mantiene esa variable
porque evita que varias réplicas compitan por hilos bajo carga, pero no era la causa.

### Diagnóstico: medir cada paso por separado
| Paso | Tiempo |
|---|---|
| build_features (entrada → features) | ~14 ms |
| **sklearn: TargetEncoder + OneHot** | **~210 ms** |
| sklearn: HistGradientBoosting | ~4 ms |

El 90 % del tiempo era `TargetEncoder.transform` de scikit-learn. En cada llamada reconstruye un diccionario con
**todas** las categorías vistas en el entrenamiento (decenas de miles de ciudades, zips y agencias). Con lotes de
miles de filas ese costo se reparte, pero con una sola fila domina.

### Solución: FastPredictor (`app/fast_model.py`)
Construye esos diccionarios **una sola vez, al cargar el modelo**, usando los mismos parámetros aprendidos por el
encoder (`categories_`, `encodings_` y la media global para categorías nuevas). En cada petición solo hace la
búsqueda. No se reentrena nada ni cambia el modelo guardado en MLflow.

| Versión | Latencia por petición | Predicción (mismo input, v5) |
|---|---|---|
| sklearn original | ~230 ms | 581.314,59 |
| FastPredictor con `Series.map(dict)` | ~75 ms | 581.314,59 |
| FastPredictor con búsqueda directa en el diccionario | **~25 ms** | 581.314,59 |

El paso intermedio mostró un segundo costo escondido: `Series.map(dict)` de pandas convierte el diccionario entero
en una Serie en cada llamada. La versión final busca valor por valor solo en las filas de la petición.

### Cómo se garantiza que el resultado no cambió
- `test_fast_predictor_igual_al_modelo` compara FastPredictor contra `model.predict` (tolerancia 1e-9), incluidas
  una ciudad, un zip y un status nunca vistos. Esta prueba detectó un error real durante la optimización (una
  versión intermedia buscaba la fila completa en lugar de cada valor en el diccionario de su columna) y no dejó
  pasar el cambio hasta corregirlo.
- En la API real la predicción fue idéntica (581.314,59) antes y después; 09-perfil-latencia.txt lo confirma
  dentro del contenedor (`misma predicción: True`).
- El workflow de GitHub Actions corre estas pruebas antes de publicar la imagen: si fallan, no se publica.

**Resultado: 9 veces más rápido (~230 ms → ~25 ms) con exactamente la misma predicción.**
