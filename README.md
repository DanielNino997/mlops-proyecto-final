# Proyecto Final MLOps — Precio de vivienda (grupo 2)

Pontificia Universidad Javeriana · Operación de Aprendizaje de Máquinas · Nivel 4
**Equipo:** Daniel Niño (carril B: datos, ML y aplicación) · Ana María Merchán (carril A: infraestructura y plataforma)
**Video de sustentación:** _[enlace de YouTube]_

Un sistema MLOps que consume lotes de datos inmobiliarios uno por uno. En cada lote decide **si reentrenar**, y
promueve el modelo nuevo **solo si le gana al que está en producción**. La API de inferencia toma siempre el modelo
productivo desde MLflow y lo cambia sin redesplegarse. Todo corre en Kubernetes, desplegado con Argo CD a partir de
imágenes que construye GitHub Actions.

Este README es el informe del proyecto: cuenta qué hicimos, en qué orden, qué decidimos y por qué, qué problemas
aparecieron y qué observamos. Cada afirmación importante tiene su evidencia (captura o archivo con salidas reales).

---

## Contenido

1. [Arquitectura](#1-arquitectura)
2. [Resultado en una tabla](#2-resultado-en-una-tabla)
3. [Bitácora del desarrollo](#3-bitácora-del-desarrollo)
4. [Datos: lo que encontramos en la API](#4-datos-lo-que-encontramos-en-la-api)
5. [DAG de Airflow](#5-dag-de-airflow)
6. [Validación, drift y decisión de entrenamiento](#6-validación-drift-y-decisión-de-entrenamiento)
7. [Análisis por lote](#7-análisis-por-lote)
8. [MLflow y promoción de modelos](#8-mlflow-y-promoción-de-modelos)
9. [API de inferencia (FastAPI)](#9-api-de-inferencia-fastapi)
10. [Streamlit](#10-streamlit)
11. [CI/CD con GitHub Actions](#11-cicd-con-github-actions)
12. [Kubernetes y Argo CD](#12-kubernetes-y-argo-cd)
13. [Observabilidad y prueba de carga](#13-observabilidad-y-prueba-de-carga)
14. [Cómo ejecutarlo](#14-cómo-ejecutarlo)
15. [Estructura del repositorio](#15-estructura-del-repositorio)
16. [Requerimientos del enunciado y dónde se cumplen](#16-requerimientos-del-enunciado-y-dónde-se-cumplen)

---

## 1. Arquitectura

```mermaid
flowchart LR
    DATOS["API de datos del profesor<br/>(grupo 2)"]

    subgraph CICD["GitHub + CI/CD"]
        GH["Repositorio GitHub"] --> GA["GitHub Actions<br/>pruebas + build"] --> DH[("Docker Hub<br/>dnino97/mlops-pf-*")]
    end

    subgraph K8S["Kubernetes (kind)"]
        AF["Airflow<br/>DAG pipeline_lotes"]
        PG[("Postgres datos<br/>raw_data · clean_data")]
        MLF["MLflow<br/>alias champion"]
        PGM[("Postgres<br/>metadatos MLflow")]
        MINIO[("MinIO<br/>artefactos")]
        API["FastAPI<br/>inferencia"]
        ST["Streamlit"]
        LOC["Locust"]
        PROM["Prometheus"]
        GRAF["Grafana"]

        AF -->|RAW · CLEAN · auditoría| PG
        AF -->|runs · modelos · promoción| MLF
        MLF --> PGM
        MLF --> MINIO
        API -->|carga el champion| MLF
        API -->|inference_events| PG
        ST -->|/predict| API
        ST -->|historial| PG
        LOC -->|carga| API
        PROM -->|scrape /metrics| API
        GRAF --> PROM
    end

    DATOS -->|1 lote por corrida| AF
    GH -->|manifiestos k8s/| ARGO["Argo CD"] -->|sincroniza| K8S
    DH -->|imágenes| K8S
```

**Flujo:** GitHub guarda el código, los DAGs y los manifiestos. GitHub Actions prueba y publica las imágenes en Docker
Hub, y Argo CD sincroniza `k8s/` con el clúster. Dentro del clúster, Airflow pide un lote por corrida, lo guarda en
RAW, lo valida, lo limpia en CLEAN y decide si entrena. Si entrena, registra el candidato en MLflow, lo compara contra
el campeón y lo promueve o lo rechaza. La API carga siempre el modelo con alias `champion`, Streamlit muestra
predicciones e historial, y Prometheus/Grafana observan la API mientras Locust le aplica carga.

**Por qué estos componentes:** dos Postgres separados, uno para los datos del pipeline (bases `raw_data` y
`clean_data`) y otro para los metadatos de MLflow, para que los dominios no se mezclen. MinIO es compatible con S3 y
corre dentro del clúster. Airflow usa `LocalExecutor`, porque las tareas comparten un directorio de staging donde
viaja el lote (hasta 60 MB, demasiado para XCom).

---

## 2. Resultado en una tabla

Así quedó la corrida completa de los 6 lotes del grupo 2 (`audit.batch_history`):

| Lote | Filas | ¿Entrenó? | Por qué | ¿Promovido? | Resultado |
|---|---|---|---|---|---|
| 0 | 73.784 | Sí | Línea base: no existía modelo productivo | Sí | v1 |
| 1 | 94.551 | Sí | Drift en `house_size` (PSI 12,5), 5 % de ciudades nuevas, +128 % de volumen | Sí | v2 (MAE −7,6 %) |
| 2 | 230.366 | Sí | Volumen +137 % (sin drift) | Sí | v3 (MAE −7,7 %) |
| 3 | 4.055 | **No** | **Lote insuficiente (+1 %), aunque hubo drift y un estado nuevo** | — | alertas registradas |
| 4 | 320.696 | Sí | Drift en `price` (PSI 12,2); el campeón empeoró su MAE +316 % sobre el lote | Sí | v4 (MAE −44,8 %) |
| 5 | 361.457 | Sí | Drift en `price` (PSI 12,3); el campeón empeoró su MAE +1.414 % | Sí | v5 (MAE −51,5 %) |
| — | — | — | La API respondió que ya no hay más lotes | — | fin normal |

![Grilla de Airflow con las 7 corridas del DAG](docs/img/airflow-grilla-dag.png)
_Grilla de Airflow con las 7 corridas, una por lote más la de fin de datos. En verde, la rama que se ejecutó; en
rosado, la que no. En la 4.ª columna (lote 3) se ve `skip_training`, y en las demás `promote_model`._

La corrida en Airflow dio **exactamente** las mismas decisiones y métricas que una simulación previa por script: el
pipeline es reproducible.

---

## 3. Bitácora del desarrollo

El trabajo se dividió en dos carriles: **A**, la plataforma (clúster, Argo CD, bases, MinIO, MLflow, Airflow,
manifiestos y observabilidad), y **B**, lo que corre sobre ella (DAG, datos, modelo, API, Streamlit y Locust). El
carril B se desarrolló primero en un `docker compose` **solo de desarrollo** (`dev/`), para avanzar sin esperar el
clúster; ese compose no es el despliegue. Cada fase cerró con pruebas, un README de evidencia y un commit.

| Fase | Qué se hizo | Problema o hallazgo | Cómo se resolvió |
|---|---|---|---|
| B0 Entorno | Estructura del repo y compose de desarrollo (Postgres ×2, MinIO, MLflow, API de datos) | `minio/minio` y `minio/mc` **ya no existen en Docker Hub** | Se usó `coollabsio/minio` (probada en clase), y el bucket lo crea un script con boto3 |
| B1 API de datos | Exploración de la API y cliente con reintentos | Los lotes pesan hasta 360 mil filas; el fin de datos llega como HTTP 400 | Timeout de lectura de 120 s; el 400 se convierte en `NoMoreBatches` (fin normal) |
| B2 RAW | Tablas e ingesta idempotente | Insertar fila por fila tardaba minutos | `COPY` en una transacción: el lote de 230 mil filas entra en ~10 s |
| B3 Validación | Esquema, calidad, categorías nuevas, drift y CLEAN | Un PSI de ~12 parecía un error de cálculo | Diagnóstico por tramos: **la API corta los lotes por rangos**. Es drift real |
| B4 Modelo | Entrenamiento, MLflow, reglas de decisión y promoción | El lote 3 tiene drift pero solo 4 mil filas | Puerta de volumen mínimo: una alerta no obliga a reentrenar |
| B5 DAG | DAG de 19 tareas en Airflow | El lote no cabe en XCom | Se guarda en un directorio de staging y por XCom van solo resúmenes |
| B6 API | FastAPI con recarga desde MLflow | ~230 ms por predicción | Medición por partes y `FastPredictor`: ~25 ms con la misma predicción |
| B7 Streamlit y Locust | Interfaz de inferencia e historial, prueba de carga | La latencia sube bajo carga con 1 proceso | Se escala con réplicas en Kubernetes (ver §13) |
| B8 Alertas | Alertas de drift siempre registradas (variación y rango, además de PSI) | Hacía falta distinguir alerta de reentrenamiento | Campo `alertas` en la auditoría, `log.warning` en Airflow y aviso en Streamlit |
| A1 Clúster | Clúster `kind` de un nodo en la VM (4 CPU, 15 GB) | Un contenedor de Rancher de un taller anterior usaba 5,8 GB de RAM y `/var` estaba al 78 % | Se eliminó Rancher y se limpiaron imágenes, volúmenes y caché de Docker (42 GB) antes de crear el clúster |
| A2 Argo CD | Argo CD con Helm y app-of-apps (`k8s/apps/`) | La instalación por defecto trae Dex y Notifications, que no se usan y consumen memoria | `values.yaml` mínimo: sin Dex ni Notifications y con `requests`/`limits` en cada componente |
| A3 Secrets | Todas las credenciales como Secrets de Kubernetes | Una contraseña en un manifiesto queda versionada en Git | `scripts/crear-secrets.sh` crea los Secrets desde `k8s/.env` (ignorado por Git); los manifiestos solo usan `secretKeyRef`. Se agregó `.env.*` al `.gitignore` después de que un respaldo `.env.bak` quedó sin ignorar |
| A4 Bases y MinIO | Postgres de datos, de MLflow y de Airflow; MinIO con su bucket | El Job que crea el bucket (hook PostSync de Argo CD) quedó esperando para siempre y bloqueó la sincronización | El log mostró `lookup minio … no such host`: faltaba el Service de MinIO. Se agregó y se terminó la operación colgada en Argo CD |
| A5 MLflow | Imagen propia de MLflow construida por Actions, con bump automático del tag | La imagen oficial no trae `psycopg2` ni `boto3`, y el primer despliegue apuntaba a un tag que aún no existía (`ImagePullBackOff`) | Imagen propia con los drivers. Orden fijo: primero la imagen y su bump, después la Application |
| A6 Airflow | Chart oficial 1.16.0 (Airflow 2.10.5), LocalExecutor y git-sync | El Postgres del chart usa imágenes de Bitnami que ya no se publican; las llaves generadas por el chart cambian en cada render y dejan a Argo CD *OutOfSync* | Postgres propio (`postgres-airflow`), fernet key, webserver key y conexión como Secrets, y la migración como hook `Sync` de Argo CD |
| A6 Airflow (recursos) | Primera corrida del DAG en el clúster | El sidecar `scheduler-log-groomer` se reinició por **OOMKilled** con un límite de 128 Mi | Se identificó con el estado de los contenedores del pod y se subió el límite a 384 Mi en `k8s/airflow/values.yaml`; Argo CD aplicó el cambio desde Git |
| A7 API, Streamlit, Locust | Manifiestos y bump automático en los 5 workflows de imágenes | Los workflows compartían un grupo de `concurrency`: al dispararse juntos, GitHub cancelaba alguno | Un grupo de `concurrency` por workflow y reintento con `git pull --rebase` en el commit del bump. Un solo push dispara los 5 builds y los 5 bumps |
| A8 Observabilidad | Prometheus (API + cAdvisor), Grafana con dashboard en ConfigMap y metrics-server | Sin metrics-server no hay `kubectl top` ni CPU/memoria por pod | metrics-server con `--kubelet-insecure-tls` (necesario en kind) y Prometheus leyendo cAdvisor a través del kubelet |

**Otro problema de entorno:** la partición `/var` de la VM (15 GB) se llenó al construir la imagen de Airflow. Se
movió el almacenamiento de Docker a `/opt`, en la partición raíz.

Evidencia detallada de cada fase, con salidas reales:
[B0](docs/evidencias/carril-b/b0-entorno/README.md) ·
[B1](docs/evidencias/carril-b/b1-api-datos/README.md) ·
[B2](docs/evidencias/carril-b/b2-raw/README.md) ·
[B3](docs/evidencias/carril-b/b3-validacion-clean/README.md) ·
[B4](docs/evidencias/carril-b/b4-modelo-mlflow/README.md) ·
[B5](docs/evidencias/carril-b/b5-dag/README.md) ·
[B6](docs/evidencias/carril-b/b6-fastapi/README.md) ·
[B7](docs/evidencias/carril-b/b7-streamlit-locust/README.md) ·
capturas del carril A en [`docs/evidencias/carril-a/`](docs/evidencias/carril-a/)

---

## 4. Datos: lo que encontramos en la API

Antes de diseñar el pipeline exploramos la API ([evidencia B1](docs/evidencias/carril-b/b1-api-datos/README.md)):

- **6 lotes por grupo** (`batch_number` 0 a 5, ~1,08 millones de filas). Al acabarse, la API responde **HTTP 400**
  con *"Ya se recolectó toda la información minima necesaria"*. El DAG lo trata como **fin normal**, no como error.
- **La API es determinista:** después de reiniciar devuelve exactamente los mismos datos.
- **Los lotes vienen cortados por rangos**, y eso genera drift real y extremo
  ([diagnóstico B3](docs/evidencias/carril-b/b3-validacion-clean/README.md)):
  - el lote 0 tiene `house_size` ≥ 1.500 y el lote 1, < 1.500;
  - los lotes 0–3 tienen precio ≤ 300 mil, el lote 4 entre 300 y 500 mil, y el lote 5 por encima.
- **Imagen usada:** `cristiandiaz13/mlops-puj:data-api-pf-v2`, la que indica el enunciado del proyecto.

**RAW y CLEAN están separados en dos bases:**

- `raw_data` guarda cada registro tal como llegó (JSONB), más los metadatos del lote: número, fecha, filas y hash.
  Un lote ya guardado no se vuelve a insertar (llave única `group_number` + `batch_number`).
- `clean_data` guarda los datos tipados y limpios. Cada fila conserva `raw_row_id` y `batch_id` para rastrear su
  origen.

![Conteo de filas por lote en RAW y en CLEAN, y una fila CLEAN con su origen en RAW](docs/img/datos-raw-clean.png)
_Filas por lote en RAW y en CLEAN, y la fila `raw_row_id = 1` en las dos bases: el registro de East Windsor
(Connecticut) se conserva sin cambios en RAW y tipado en CLEAN._

---

## 5. DAG de Airflow

`airflow/dags/pipeline_lotes.py` tiene **19 tareas en el orden del enunciado** y **dos bifurcaciones**:

```
start → fetch_batch_from_api → store_raw_batch → validate_schema → validate_data_quality
      → detect_new_categories → detect_data_drift → preprocess_data → decide_training
          ├─ skip_training ───────────────────────────────────────────────┐
          └─ train_candidate_model → evaluate_candidate_model             │
             → register_candidate_in_mlflow → compare_with_production     │
             → decide_promotion ─┬─ promote_model ──────────────────────┤
                                  └─ reject_model ───────────────────────┤
                                                    notify_or_log_result → end
```

- **Una petición = un lote = una corrida completa.** No se descargan todos los lotes de una vez.
- Cada tarea llama **una sola función** de `airflow/dags/lib/`, que tiene pruebas con pytest.
- **Cliente de la API** (`lib/api_client.py`): timeout, reintentos con espera creciente solo para errores
  transitorios, validación de la respuesta, lote vacío y fin de datos.
- **Robustez:** reintentos en la tarea de red; `max_active_runs=1`; un lote duplicado no se reprocesa; el fin de
  datos deja la corrida en `skipped`; y cualquier error real queda en el historial como `fallido` (callback de fallo),
  nunca disfrazado de éxito.

![Grafo del DAG pipeline_lotes](docs/img/airflow-grafo.png)
_Grafo del DAG con las 19 tareas y las dos bifurcaciones (`decide_training` y `decide_promotion`)._

---

## 6. Validación, drift y decisión de entrenamiento

Detalle completo y justificación de cada umbral: **[docs/decisiones.md](docs/decisiones.md)**.

**Validaciones de cada lote:** esquema (columnas faltantes o extra, tipos), calidad (nulos, duplicados, rangos,
precio fuera de [1.000, 100.000.000]), volumen frente al acumulado, categorías nuevas en `state`, `city` y `status`,
y drift contra el histórico de CLEAN.

**Medidas de drift por variable** (`price`, `house_size`, `acre_lot`, `bed`, `bath`):

| Medida | Umbral de alerta | Por qué |
|---|---|---|
| PSI | > 0,2 | Convención: < 0,1 estable, 0,1–0,2 moderado, > 0,2 significativo |
| KS (estadístico) | solo se reporta | Con cientos de miles de filas, el p-valor casi siempre sale significativo |
| `std_ratio` (dispersión del lote / histórico) | > 2 o < 0,5 | Cambia los extremos aunque la mediana se mantenga |
| Fuera del rango p1–p99 del histórico | > 10 % del lote | Si el lote se parece al histórico se espera ~2 %; más obliga a extrapolar |

**Categorías nuevas:** el modelo nunca falla ante una ciudad, zip o estado no vistos. `TargetEncoder` les asigna la
media global y `OneHotEncoder(handle_unknown="ignore")` las ignora.

**Regla de entrenamiento** (`lib/decision.py`): las alertas se registran **siempre**. Para entrenar, el lote además
debe aportar al menos **5 % del acumulado**. Si lo aporta, entrena cuando hay al menos un disparador: drift,
categorías nuevas en ≥ 5 % de las filas, volumen ≥ 10 % o degradación del campeón ≥ 10 % en MAE. **Nunca se entrena
por calendario ni por número de lotes.** La razón queda en la auditoría y como tag `train_reason` del run de MLflow.

### Ejemplo: lote 3, una alerta no es lo mismo que reentrenar

El lote 3 tiene 4.054 filas (+1,02 % del acumulado), un PSI de 0,41 en `price` y **Massachusetts, un estado nuevo, en
el 100 % de sus filas**. Se generan las alertas y se registran en Airflow y en Streamlit, pero **no se entrena**: con
4 mil filas de un solo estado, un candidato sería casi igual al campeón y su test casi el mismo. Sus datos quedan en
CLEAN y entran en el siguiente entrenamiento (v4 se entrenó con los lotes 0-1-2-3-4).

![Log de decide_training del lote 3 con las alertas](docs/img/airflow-alerta-lote3.png)
_Log de `decide_training` en la corrida del lote 3: las líneas `ALERTA lote 3` (drift en `price`, estado nuevo) y
la decisión «no entrena: lote insuficiente»._

---

## 7. Análisis por lote

_[Completar con los valores de `audit.batch_history` de la corrida final: para cada lote, qué llegó, qué alertas
hubo, qué se decidió y las métricas frente al campeón.]_

**Lote 0.** 73.784 filas de 50 estados, todas con `house_size` ≥ 1.500. Sin referencia previa, así que no hay drift
que medir. Entrena la línea base (v1, MAE de test ~40 mil) y se promueve porque no existía modelo productivo.

**Lote 1.** 94.551 filas, **todas con `house_size` < 1.500**: el lote completo cae fuera del rango del histórico
(PSI 12,5). Además, 5,2 % de las filas son de ciudades nuevas y el volumen crece +128 %. Entrena v2 y, sobre el mismo
test, baja el MAE 7,6 % frente a v1 → se promueve.

**Lote 2.** 230.366 filas, sin drift (PSI máximo 0,004). Entrena solo por volumen (+137 %). v3 mejora el MAE 7,7 %
frente a v2 → se promueve. Más datos de la misma distribución siguen ayudando.

**Lote 3.** Ver el ejemplo de §6: alertas registradas y no entrena.

**Lote 4.** 320.696 filas con **precios entre 300 y 500 mil**, por encima de todo lo visto (PSI 12,2). El campeón v3
empeora su MAE **+316 %** sobre este lote: un modelo de árboles no extrapola a precios que nunca vio. v4 baja el MAE
44,8 % frente a v3 sobre el mismo test → se promueve.

**Lote 5.** 361.457 filas con precios **por encima de 500 mil** (mediana ~760 mil). El campeón v4 empeora +1.414 %.
v5 baja el MAE 51,5 % → se promueve.

**Fin de datos.** La 7.ª corrida recibe el HTTP 400 de la API, termina en `skipped` y queda registrada como
`sin_datos`.

![Historial de lotes en la tabla de auditoría](docs/img/audit-batch-history.png)
_`audit.batch_history` después de la corrida completa: estado final, si entrenó y se promovió, versión, Δ MAE y
alertas por lote._

---

## 8. MLflow y promoción de modelos

- **Modelo:** `TransformedTargetRegressor(log1p)` + `HistGradientBoostingRegressor`. El log reduce el efecto de los
  precios extremos, y el boosting de histogramas entrena rápido con un millón de filas.
- **Encoders:** `TargetEncoder` para `city`, `state`, `zip_code` y `brokered_by` (decenas de miles de categorías: un
  one-hot sería inviable) y `OneHotEncoder(handle_unknown="ignore")` para `status`. Se descarta `street`, que es casi
  un identificador único.
- **Cada run registra:** lotes usados, commit (`git_sha`), razón del entrenamiento, parámetros del modelo y del
  preprocesamiento, métricas de train/val/test (MAE, RMSE, R², MAPE), gráficos (real vs. predicho, residuales,
  importancia por permutación) y el modelo en formato MLflow con firma.
- **Infraestructura:** metadatos en Postgres (no SQLite) y artefactos en el bucket `mlflow-artifacts` de MinIO.
- **Producción:** el modelo registrado `house-price-model` con **alias `champion`**.

**Regla de promoción:** se promueve si el **MAE baja al menos 3 %** y el **RMSE no empeora más de 1 %**. El MAE es
la métrica principal porque se lee directo en dólares («nos equivocamos en $X en promedio»). El RMSE funciona como
freno: impide promover un modelo con errores grandes más frecuentes. El MAPE se descartó como criterio porque se
dispara con los precios bajos.

**Comparación justa:** el campeón se evalúa sobre **el mismo conjunto de test** del candidato. Además, el split es
determinista (`raw_row_id % 100`: 70 % train, 15 % val, 15 % test), así que una fila siempre cae en el mismo
conjunto y el campeón nunca vio el test del candidato.

### ¿Por qué el `test_mae` sube de v3 a v5 si cada versión se promueve?

| Versión | Lotes | test_mae |
|---|---|---|
| v3 | 0-1-2 | ~35 mil |
| v4 | 0-1-2-3-4 | ~47 mil |
| v5 | 0-…-5 | ~129 mil |

El test **crece con cada lote** e incluye casas cada vez más caras: el lote 5 trae precios por encima de 500 mil
(mediana ~760 mil). Un error de 129 mil sobre casas de 760 mil es proporcionalmente menor que uno de 35 mil sobre
casas de 200 mil. Por eso las cifras de distintas versiones no se comparan entre sí. **Lo que decide la promoción es
la comparación sobre el mismo test:** v5 tiene un MAE 51,5 % menor que v4 sobre ese mismo conjunto.

![Runs del experimento house-price en MLflow](docs/img/mlflow-runs.png)
_Runs del experimento `house-price`: un run por entrenamiento, con los lotes usados, la razón y las métricas de test._

![Detalle de un run con parámetros, tags y artefactos](docs/img/mlflow-run-detalle.png)
_Detalle de un run: tags `batch_numbers`, `git_sha` y `train_reason`, parámetros del modelo y gráficos guardados
en MinIO._

![Model Registry con las versiones y el alias champion](docs/img/mlflow-registry-champion.png)
_Model Registry de `house-price-model`: versiones 1 a 5, cada una con su tag `decision`, y el alias `champion` en la
versión productiva._

---

## 9. API de inferencia (FastAPI)

Endpoints: `/live`, `/health`, `/predict`, `/metrics` y `POST /admin/reload` (protegido con un token guardado en un
Secret). Entrada validada con Pydantic. [Evidencia B6](docs/evidencias/carril-b/b6-fastapi/README.md).

- **MLflow como fuente de verdad:** la API carga `models:/house-price-model@champion`. No hay rutas, archivos ni
  versiones escritas en el código.
- **Recarga sin redespliegue (RF7):** un hilo revisa el alias cada 30 s y `/admin/reload` fuerza la revisión. El
  modelo nuevo se carga aparte y se intercambia con un lock solo cuando está completo, así que una petición nunca
  usa un modelo a medio cargar; las peticiones en curso terminan con el modelo con el que empezaron. Si la descarga
  falla, se queda el modelo anterior y el error se ve en `/health` y en la métrica `model_reload_total{result="error"}`.
- **Registro de inferencias (RF8):** cada petición (ok, inválida o con error) queda en `raw_data.inference_events`
  con fecha, entrada, predicción, versión, estado, error y latencia.
- **Optimización:** la primera versión tardaba ~230 ms por predicción. Medimos cada paso y el 90 % era el
  `TargetEncoder` de scikit-learn, que reconstruye en cada llamada un diccionario con todas las categorías.
  `FastPredictor` arma esos diccionarios una sola vez al cargar el modelo: **~25 ms, con exactamente la misma
  predicción** (verificado con pruebas).

![Predicción en /docs de la API](docs/img/api-docs-predict.png)
_`/docs` de la API: una predicción con su `model_version`._

![Recarga del modelo sin redespliegue](docs/img/api-recarga-modelo.png)
_Al mover el alias en MLflow, la API pasa de v5 a v4 y de vuelta a v5. El pod mantiene la misma edad y no se
reinicia._

![Inferencias registradas en raw_data.inference_events](docs/img/api-inference-events.png)
_`inference_events`: predicciones con v5 y con v4, una petición inválida con su error de validación, y la latencia
de cada una._

---

## 10. Streamlit

Dos pestañas ([evidencia B7](docs/evidencias/carril-b/b7-streamlit-locust/README.md)):

- **Inferencia:** un formulario que llama a la API y muestra el precio estimado, **la versión del modelo usada** y
  la latencia.
- **Historial de entrenamiento y despliegue:** una frase por lote (si entrenó o no, por qué, si se promovió),
  contadores, alertas de drift, la gráfica del MAE del candidato contra el campeón y la tabla con los IDs de MLflow.

![Streamlit, pestaña de inferencia](docs/img/streamlit-inferencia.png)
_Predicción de una casa en Austin (Texas) con el modelo v5, servida por la API._

![Streamlit, historial con alertas](docs/img/streamlit-historial.png)
_Historial de los 6 lotes: 5 entrenamientos promovidos, el lote 3 con alertas y sin reentrenar, y el fin de datos._

---

## 11. CI/CD con GitHub Actions

Hay cinco workflows en `.github/workflows/`, uno por imagen propia. Cada uno **corre las pruebas antes de
construir**; si fallan, no se publica nada. Las imágenes se publican en Docker Hub con el **SHA del commit** (versión
exacta) y `latest`. Las credenciales de Docker Hub están en los secrets del repo.

| Imagen | Workflow | Pruebas previas |
|---|---|---|
| `dnino97/mlops-pf-airflow` | `airflow-image.yml` | `lib/` del DAG (cliente, validaciones, decisión) |
| `dnino97/mlops-pf-api` | `api-image.yml` | API, paridad de features con el DAG, FastPredictor |
| `dnino97/mlops-pf-streamlit` | `streamlit-image.yml` | narración del historial |
| `dnino97/mlops-pf-locust` | `locust-image.yml` | — |
| `dnino97/mlops-pf-mlflow` | `mlflow-image.yml` | — (servidor MLflow 2.17.2 + `psycopg2` + `boto3`) |

**De la imagen al clúster sin pasos manuales.** Después de publicar, un segundo job (`bump-manifest`) hace que
`github-actions[bot]` escriba el nuevo tag en el manifiesto del componente (`k8s/<componente>/deployment.yaml` o
`k8s/airflow/values.yaml`) y lo suba a la misma rama. Argo CD ve ese commit y redespliega. Los manifiestos nunca usan
`latest`: siempre apuntan al SHA que se construyó.

- La imagen de Airflow recibe `GIT_SHA` como *build-arg*, y el entrenamiento lo registra en MLflow como tag `git_sha` (RF5).
- Cada workflow tiene su propio grupo de `concurrency`, y el bump reintenta con `git pull --rebase`. Así varios
  workflows pueden terminar al tiempo sin pisarse.

![Workflow de Airflow con build-push y bump-manifest en verde](docs/evidencias/carril-a/09-actions-airflow.png)
_Workflow `Imagen Airflow`: `build-push` (pruebas, build y push) y luego `bump-manifest`._

![Commits del bot actualizando los tags de las cinco imágenes](docs/evidencias/carril-a/10-commits-bots-bump.png)
_Un solo commit (`9afa043`) disparó los cinco workflows; `github-actions[bot]` hizo un commit `deploy(...)` por
imagen con el tag nuevo._

![Imagen de MLflow en Docker Hub con el tag del SHA](docs/evidencias/carril-a/07-dockerhub-mlflow.png)
_Tags en Docker Hub: el SHA del commit que construyó la imagen y `latest`._

![Tags de imagen en los manifiestos de k8s/](docs/evidencias/carril-a/10-tags-sha-manifiestos.png)
_Los cinco manifiestos en Git apuntan a imágenes versionadas por SHA, ninguno a `latest`._

---

## 12. Kubernetes y Argo CD

**Clúster.** Un clúster `kind` de un nodo (`k8s/cluster/kind.yaml`) en la VM del curso (4 CPU, 15 GB de RAM). Un
solo nodo porque cada nodo de kind es un contenedor con su propio overhead. Los servicios no se exponen hacia fuera:
se accede con `kubectl port-forward` y un túnel SSH. Todo el proyecto vive en el namespace `mlops`.

**GitOps con app-of-apps.** Argo CD se instala una vez con Helm (`k8s/argocd/values.yaml`). Lo único que se aplica a
mano es `k8s/argocd/root.yaml`: una Application que lee `k8s/apps/` y crea una Application por componente. Desde ahí
**todo entra por Git**: un archivo nuevo en `k8s/apps/` crea un componente, y un cambio en un manifiesto se aplica
solo. Todas las Applications usan `syncPolicy.automated` con `prune` (borra del clúster lo que se quita de Git) y
`selfHeal` (revierte cambios hechos a mano).

| Application | Qué despliega | Origen |
|---|---|---|
| `postgres-data` | Postgres con las bases `raw_data` y `clean_data` (StatefulSet + PVC 10 Gi) | `k8s/postgres-data/` |
| `postgres-mlflow` | Postgres de metadatos de MLflow (no SQLite) | `k8s/postgres-mlflow/` |
| `postgres-airflow` | Postgres de metadatos de Airflow | `k8s/postgres-airflow/` |
| `minio` | MinIO (PVC 5 Gi) y el Job que crea el bucket `mlflow-artifacts` | `k8s/minio/` |
| `mlflow` | Servidor MLflow con `--serve-artifacts` | `k8s/mlflow/` |
| `data-api` | API de datos del curso (`cristiandiaz13/mlops-puj:data-api-pf-v2`) | `k8s/data-api/` |
| `airflow` | Chart oficial `apache-airflow/airflow` 1.16.0 + nuestros values (multi-source) | `k8s/airflow/values.yaml` |
| `api`, `streamlit`, `locust` | Inferencia, interfaz y prueba de carga | `k8s/api/`, `k8s/streamlit/`, `k8s/locust/` |
| `observability` | Prometheus, Grafana, su configuración y el dashboard | `k8s/observability/` |
| `metrics-server` | Métricas de CPU/memoria por pod (`kubectl top`) | chart oficial |

**Decisiones del despliegue**
- **Secrets fuera de Git.** `scripts/crear-secrets.sh` lee `k8s/.env` (no versionado; la plantilla es
  `k8s/.env.example`) y crea los Secrets del namespace. Los manifiestos solo los referencian con `secretKeyRef`. Las
  URIs de conexión completas, con su contraseña, también viven en el Secret, para que ningún manifiesto lleve
  credenciales.
- **Persistencia.** Los tres Postgres y MinIO son StatefulSets con PVC (`local-path` de kind): los datos sobreviven al
  reinicio de los pods. Los logs de las tareas de Airflow también van en un PVC.
- **Configuración como código.** El script de creación de las bases, la configuración de Prometheus y el dashboard de
  Grafana son ConfigMaps versionados.
- **Probes.** Cada servicio tiene `readinessProbe` (no recibe tráfico hasta estar listo) y `livenessProbe` (se reinicia
  si se cuelga). La API usa `/health` para readiness, que responde 503 mientras no tenga modelo cargado, y `/live` para
  liveness.
- **Airflow.** LocalExecutor: las tareas corren dentro del pod del scheduler, sin Redis ni workers de Celery, lo que
  ahorra memoria en una sola VM. Los DAGs llegan por git-sync cada 60 s, así que un cambio en el DAG no obliga a
  reconstruir la imagen. La migración de la base corre como hook `Sync` de Argo CD.
- **La API de datos también corre en el clúster**, y el DAG la llama por su nombre (`http://data-api`). Guarda en
  memoria el último lote entregado; si su pod se reinicia vuelve al lote 0, y el DAG no reprocesa lotes repetidos
  (llave única `group_number` + `batch_number` en RAW).

**Recursos.** Todos los contenedores (incluidos los *sidecars*, los *initContainers* y los Jobs) declaran
`requests` y `limits` de CPU y memoria. Los `requests` son lo que el scheduler de Kubernetes reserva y los `limits`
el techo: si un contenedor pasa su límite de memoria, Kubernetes lo reinicia (`OOMKilled`); si pasa el de CPU, lo frena.
Los valores se fijaron por el pico esperado de cada componente, no por su consumo en reposo.

| Componente | requests (CPU / mem) | limits (CPU / mem) | En reposo | Por qué |
|---|---|---|---|---|
| Airflow scheduler | 500m / 1 Gi | 2 / 5 Gi | 515 Mi | Con LocalExecutor aquí se cargan lotes de hasta 360 mil filas y se entrena el modelo |
| Airflow webserver | 250m / 768 Mi | 1 / 1,5 Gi | 778 Mi | UI con 2 workers de gunicorn |
| Postgres de datos | 250m / 512 Mi | 1 / 1,5 Gi | 27 Mi | Recibe los lotes completos con `COPY` |
| MLflow | 200m / 512 Mi | 1 / 1,5 Gi | 393 Mi | 2 workers; sube y baja los modelos hacia MinIO |
| API de datos | 100m / 512 Mi | 1 / 2 Gi | 222 Mi | Mantiene los lotes en memoria |
| API de inferencia | 250m / 512 Mi | 1 / 1 Gi | ~150 Mi | Modelo en memoria; se escala con réplicas |
| Postgres MLflow / Airflow, MinIO | 100m / 256 Mi | 500m / 1 Gi | < 100 Mi | Solo metadatos y artefactos pequeños |
| Streamlit · Locust · Prometheus · Grafana | 50–100m / 128–256 Mi | 0,5–1 / 0,5–1 Gi | — | Servicios livianos |
| Sidecars y Jobs (git-sync, log-groomer, bucket, migración) | 10–100m / 64–256 Mi | 0,1–0,5 / 0,25–1 Gi | — | Corren poco o hacen tareas cortas |

En reposo el nodo usa ~19 % de CPU y ~30 % de memoria, así que queda margen para los picos de entrenamiento y de la
prueba de carga.

![Argo CD con las 12 aplicaciones Synced y Healthy](docs/evidencias/carril-a/10-argocd-12-apps.png)
_Las Applications del proyecto en Argo CD, todas `Synced` y `Healthy`, leyendo los manifiestos de Git._

![Sincronización de Argo CD disparada por un commit del bot](docs/evidencias/carril-a/07-argocd-mlflow.png)
_Application `mlflow` sincronizada al commit `dd3ec72`, que no hizo una persona: lo hizo `github-actions[bot]` al
cambiar el tag de la imagen (`pendiente` → `2ab8d62…`). Argo CD lo aplicó sin ningún `kubectl apply` manual._

![Diff del commit del bot que cambió el tag de la imagen](docs/evidencias/carril-a/07-commit-bot-bump.png)
_El cambio en Git que disparó esa sincronización: una sola línea, el tag de la imagen en `k8s/mlflow/deployment.yaml`._

![Árbol de la Application minio en Argo CD](docs/evidencias/carril-a/06-argocd-arbol-minio.png)
_Recursos que crea la Application `minio`: StatefulSet, pod, PVC, Service y el Job PostSync (⚓) que creó el bucket._

![Pods, Services y PVC del namespace mlops](docs/evidencias/carril-a/06-kubectl-recursos.png)
_Pods en `Running`, Services internos y PVC en `Bound`; abajo, las bases `raw_data`, `clean_data` y `mlflow` separadas._

![Requests y limits de cada pod](docs/evidencias/carril-a/06-kubectl-limits.png)
_Requests y limits de CPU y memoria por contenedor, incluido el Job que crea el bucket._

![Uso real de recursos con kubectl top](docs/evidencias/carril-a/09-kubectl-top.png)
_`kubectl top pods` en reposo, con Airflow y MLflow corriendo._

---

## 13. Observabilidad y prueba de carga

La API expone estas métricas: `predict_requests_total{status}` (ok, invalid, error), `predict_latency_seconds`
(histograma), `model_version`, `model_loaded` y `model_reload_total`.

**Prueba local previa** (30 usuarios, 60 s, 1 réplica): 1.118 predicciones, **0 fallos**, ~19 predicciones/s,
mediana de 120 ms y p95 de 480 ms. El contador de Prometheus subió exactamente lo mismo que Locust (+1.118). Con un
solo proceso, las peticiones simultáneas hacen cola: la latencia sube aunque no haya errores. La API usa 1 worker a
propósito, para que el modelo y las métricas vivan en un solo proceso, y se escala con **réplicas en Kubernetes**.

**En el clúster.** Prometheus descubre las réplicas de la API por la anotación `prometheus.io/scrape` del pod (si se
escala a 3 réplicas, aparecen 3 targets sin tocar la configuración) y lee su `/metrics` cada 5 s. También lee de
cAdvisor, a través del kubelet, la CPU y la memoria reales de los contenedores. Grafana arranca con el datasource y
el dashboard ya provisionados desde ConfigMaps (`k8s/observability/`); cambiar el dashboard es un commit, no un clic.

El dashboard **"API de inferencia — precio de vivienda"** muestra: versión del modelo cargado, réplicas activas,
peticiones/s por estado, tasa de errores, latencia p50/p95, CPU y memoria por réplica, *throttling* de CPU (cuánto
frena el límite a la API) y recargas del modelo.

![Targets de Prometheus activos](docs/evidencias/carril-a/11-prometheus-targets.png)
_Targets en UP: la réplica de la API (descubierta por anotación) y cAdvisor del nodo._

![Dashboard de Grafana en reposo](docs/evidencias/carril-a/11-grafana-dashboard-reposo.png)
_El dashboard antes de la prueba de carga: versión 1 del modelo, una réplica y las primeras predicciones desde Streamlit._

<!-- PENDIENTE (carril A): prueba de carga en el clúster con 1 réplica y luego con 3. Agregar aquí las capturas de
Locust y de Grafana durante la prueba, y el análisis: peticiones/s, latencia p50/p95, errores, CPU y memoria del
pod frente a sus limits, throttling y qué cambió al escalar réplicas. -->

---

## 14. Cómo ejecutarlo

**En Kubernetes** (el despliegue del proyecto). Requisitos: Docker, `kind`, `kubectl` y `helm`. Desde la raíz del repo:

```bash
# 1. Clúster
kind create cluster --config k8s/cluster/kind.yaml

# 2. Argo CD
helm repo add argo https://argoproj.github.io/argo-helm && helm repo update
helm install argocd argo/argo-cd -n argocd --create-namespace -f k8s/argocd/values.yaml --wait

# 3. Secrets (las credenciales nunca van a Git)
cp k8s/.env.example k8s/.env        # poner valores reales; generar con: openssl rand -hex 16
./scripts/crear-secrets.sh

# 4. App-of-apps: desde aquí Argo CD despliega todo lo que está en k8s/apps/
kubectl apply -f k8s/argocd/root.yaml
kubectl -n argocd get applications  # esperar a que todas queden Synced / Healthy

# 5. Usuario admin de Airflow (una vez, con el webserver arriba)
./scripts/crear-usuario-airflow.sh

# 6. Acceso a las interfaces (en otra terminal o con túnel SSH)
kubectl -n argocd port-forward svc/argocd-server     8081:80   &
kubectl -n mlops  port-forward svc/airflow-webserver 8080:8080 &
kubectl -n mlops  port-forward svc/mlflow            5000:5000 &
kubectl -n mlops  port-forward svc/api               8001:8000 &
kubectl -n mlops  port-forward svc/streamlit         8501:8501 &
kubectl -n mlops  port-forward svc/locust            8089:8089 &
kubectl -n mlops  port-forward svc/grafana           3000:3000 &
kubectl -n mlops  port-forward svc/prometheus        9090:9090 &
```

La contraseña inicial de Argo CD está en el Secret `argocd-initial-admin-secret`, y las de Airflow y Grafana en `k8s/.env`.
Cada corrida del DAG (`pipeline_lotes`, botón ▶ en Airflow) consume un lote de la API de datos.

**Entorno local de desarrollo** (solo para desarrollar y probar el carril B; no es el despliegue):

```bash
cp dev/.env.example dev/.env          # completar contraseñas
docker compose -f dev/docker-compose.dev.yml up -d
# Airflow :8080 · MLflow :5000 · API :8001 · Streamlit :8501 · Locust :8089 · API de datos :8000
docker compose -f dev/docker-compose.dev.yml exec airflow airflow dags trigger pipeline_lotes
```

Pruebas unitarias:

```bash
pip install -r requirements-dev.txt
python -m pytest airflow/tests services/api/tests services/streamlit/tests -q
```

---

## 15. Estructura del repositorio

```
.github/workflows/      CI: un workflow por imagen (pruebas + build + push + bump del tag en k8s/)
airflow/
  dags/pipeline_lotes.py   DAG principal (19 tareas, 2 bifurcaciones)
  dags/lib/                lógica probada: cliente API, almacenamiento, validación, drift, entrenamiento, promoción
  dags/sql/                esquemas de raw_data y clean_data
  Dockerfile, tests/
services/
  api/                  FastAPI (inferencia, recarga, métricas, FastPredictor)
  streamlit/            interfaz (inferencia + historial)
  locust/               prueba de carga con 500 propiedades reales
  mlflow/               imagen del servidor MLflow (psycopg2 + boto3)
k8s/                    manifiestos sincronizados por Argo CD (carril A)
  cluster/kind.yaml       clúster de un nodo
  argocd/                 values de Argo CD y root.yaml (app-of-apps)
  apps/                   una Application por componente
  airflow/values.yaml     values del chart oficial de Airflow
  postgres-*/ minio/ mlflow/ data-api/ api/ streamlit/ locust/ observability/
dev/                    docker compose solo para desarrollo local
scripts/                crear-secrets.sh, crear-usuario-airflow.sh y utilidades de prueba del carril B
docs/
  decisiones.md         justificación de umbrales y reglas
  evidencias/carril-a/  capturas de la plataforma (clúster, Argo CD, CI/CD, observabilidad)
  evidencias/carril-b/  evidencia por fase (README + salidas reales)
  img/                  capturas usadas en este README
```

---

## 16. Requerimientos del enunciado y dónde se cumplen

| Requerimiento | Dónde |
|---|---|
| RF1 Recolección incremental | DAG, un lote por corrida, sin reprocesar (§4, §5) |
| RF2 RAW y CLEAN separados | Bases `raw_data` y `clean_data`, trazabilidad por `raw_row_id` (§4) |
| RF3 Validación y categorías nuevas | `lib/validation.py`, `TargetEncoder` y `handle_unknown` (§6) |
| RF4 Decisión automática de entrenamiento | `lib/decision.py`, bifurcación `decide_training` (§6, §7) |
| RF5 Registro en MLflow | Runs con lotes, commit, parámetros, métricas, gráficos y modelo (§8) |
| RF6 Comparación con producción | Mismo test, regla MAE −3 % / RMSE +1 % (§8) |
| RF7 Recarga sin redespliegue | Alias `champion`, sondeo + `/admin/reload` (§9) |
| RF8 Registro de inferencias | `raw_data.inference_events` (§9) |
| RF9 Streamlit | Inferencia + historial (§10) |
| RF10 Observabilidad y carga | `/metrics`, Prometheus, Grafana, Locust (§13) |
| CI/CD | 5 workflows: pruebas, imagen con tag SHA y bump automático del manifiesto (§11) |
| GitOps y Kubernetes | Argo CD app-of-apps con `automated`; `requests`/`limits`, probes, Secrets y PVC en todo (§12) |
