# B3 — Validaciones, categorías nuevas, drift y CLEAN

## Qué se hizo
- `lib/validation.py`: validate_schema (columnas faltantes/extra, tipos), validate_quality (nulos, duplicados,
  rangos, precio fuera de [1.000, 100.000.000], outliers), detect_new_categories (state/city/status vs CLEAN),
  detect_drift (PSI + estadístico KS vs muestra de CLEAN).
- `lib/preprocess.py`: tipos, negativos→nulo, zip de 5 dígitos, years_since_prev_sale, quita duplicados,
  conserva raw_row_id y batch_id (trazabilidad).
- `lib/batch_processing.py`: procesa un lote RAW→CLEAN y devuelve el resumen para la decisión de entrenamiento.
  Reprocesar un lote reemplaza sus filas CLEAN (no duplica).

## Decisiones
- Categorías nuevas y drift se calculan sobre el lote ya limpio, para que datos basura no generen drift falso.
- El drift se decide por PSI (> 0,2), no por el p-valor del KS: con lotes de 10^5 filas cualquier diferencia da p<0,01.

## Resultado (03-procesamiento-lotes.txt, 6 lotes, ~1,08 M filas)
| batch | raw → clean | +volumen | estados nuevos (% filas) | PSI máx |
|---|---|---|---|---|
| 0 | 73.784 → 73.626 | — (referencia) | 50 (100 %) | — |
| 1 | 94.551 → 94.383 | 128 % | 1 (0 %) | house_size 12,5 |
| 2 | 230.366 → 230.013 | 137 % | 1 (0 %) | acre_lot 0,004 |
| 3 | 4.055 → 4.054 | 1 % | 1 (100 %) | price 0,41 |
| 4 | 320.696 → 320.351 | 80 % | 0 | price 12,2 |
| 5 | 361.457 → 361.278 | 50 % | 0 | price 12,3 |

## Hallazgo clave (05-diagnostico-drift.txt)
La API corta el dataset por rangos: el lote 0 tiene house_size ≥ 1.500 y el lote 1 < 1.500; los lotes 0–3 tienen
precio ≤ 300 mil, el 4 entre 300 y 500 mil y el 5 por encima. Por eso el PSI llega a ~12: el lote entero cae en un
solo tramo de la referencia. Es drift real y diseñado, no un error de cálculo.

## Implicaciones para la decisión de entrenamiento (B4/B5)
- Puerta de volumen mínimo antes de cualquier regla: el lote 3 (1 % de volumen) no entrena aunque tenga drift y un
  estado nuevo; sus datos quedan en CLEAN para el próximo entrenamiento.
- Lotes 1, 4 y 5: drift fuerte → entrenar; el campeón no puede extrapolar a rangos que no vio → se espera promoción.
- Lote 2: sin drift, entrena por volumen → candidato natural a "entrena pero no promueve" si no mejora el MAE ≥ 3 %.
