# B1 — Exploración de la API de datos y de los lotes

## API (01-openapi-*.json)
- La API real (10.43.97.110:8000) y la local (imagen cristiandiaz13/mlops-puj:data-api-pf-v2) exponen lo mismo.
- `GET /data?group_number=2` entrega el siguiente lote; `GET /restart_data_generation?group_number=2` reinicia.
- El parámetro `day` (Tuesday/Wednesday) es solo compatibilidad con la v1: no se usa.
- Cada respuesta trae `group_number`, `batch_number` (desde 0) y `data` (lista de registros con las 12 columnas).
- Al acabarse los lotes responde **HTTP 400** con `"Ya se recolectó toda la información minima necesaria"`.

## Lotes del grupo 2 (03-recorrido-lotes.txt, API local)
| batch | filas | estados | ciudades nuevas | mediana precio | top estados |
|---|---|---|---|---|---|
| 0 | 73.784 | 50 | 7.747 | 234.900 | Texas, Ohio, Pennsylvania |
| 1 | 94.551 | 51 | 2.400 | 179.900 | Florida, Pennsylvania, Ohio |
| 2 | 230.366 | 50 | 2.126 | 200.000 | Texas, Florida, Ohio |
| 3 | 4.055 | 1 | 103 | 249.000 | Massachusetts |
| 4 | 320.696 | 53 | 1.606 | 395.000 | Florida, Texas, California |
| 5 | 361.457 | 53 | 1.121 | 759.900 | California, Florida, Washington |

Total: 6 lotes, ~1,085 millones de filas. `price` sin nulos en ningún lote.

## Decisiones de diseño que salen de esta exploración
- **Identificador de lote** = (`group_number`, `batch_number`): llave única para no reprocesar (RF1).
- **Fin de datos**: el HTTP 400 con ese mensaje se trata como fin normal (`NoMoreBatches`), no como error del DAG.
- **Lotes de hasta 360 mil filas**: inserción masiva en Postgres (COPY / execute_values), timeout de lectura ~120 s,
  y límites de memoria generosos para los pods de Airflow.
- **Ciudades nuevas en todos los lotes**: el encoder debe tolerar categorías desconocidas (`handle_unknown`) (RF3).
- **Lote 3 (4 mil filas, un solo estado)**: caso esperado de *no entrenar* por volumen insuficiente.
- **Lotes 4 y 5 (mediana de precio x2 y x3)**: caso esperado de *entrenar* por drift en el target.
