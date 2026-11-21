# B4 — Modelo, MLflow, decisión de entrenamiento y promoción

## Qué se hizo
- `lib/train.py`: TransformedTargetRegressor(log1p) + HistGradientBoosting. TargetEncoder para city, state,
  zip_code y brokered_by (alta cardinalidad; categorías no vistas → media global, RF3); OneHot(handle_unknown=ignore)
  para status. División determinista por raw_row_id % 100 (test 15 %, val 15 %): una fila siempre cae en el mismo
  conjunto, así el campeón nunca vio el test del candidato. Registra en MLflow: lotes usados, git sha, razón,
  parámetros, métricas train/val/test, gráficos (real vs predicho, residuales, importancia por permutación) y modelo.
- `lib/promote.py`: evalúa al campeón (alias `champion`) sobre el mismo test del candidato; promueve si el MAE baja
  ≥ 3 % y el RMSE no empeora más de 1 %. Deja tags decision / decision_reason en cada versión.
- `lib/decision.py`: puerta de volumen mínimo (≥ 5 % del acumulado) y disparadores: PSI > 0,2, categorías nuevas
  ≥ 5 % de filas, volumen ≥ 10 %, degradación del campeón ≥ 10 %.

## Resultado (02-simulacion-pipeline.txt, 6 lotes en orden de llegada)
| Lote | Decisión | Motivo | Resultado |
|---|---|---|---|
| 0 | entrena | línea base | v1 promovido |
| 1 | entrena | drift house_size (PSI 12,5), ciudades nuevas 5,2 %, volumen | v2 promovido (MAE −7,6 %) |
| 2 | entrena | volumen +137 % | v3 promovido (MAE −7,7 %) |
| 3 | no entrena | lote insuficiente (+1 %) aunque hubo drift y estado nuevo | datos quedan en CLEAN |
| 4 | entrena | drift price (PSI 12,2), campeón degradado +316 % | v4 promovido (MAE −44,8 %) |
| 5 | entrena | drift price (PSI 12,3), campeón degradado +1.414 % | v5 promovido (MAE −51,5 %) |

## Notas
- El MAE absoluto crece con los lotes porque el test incluye casas más caras; la comparación es justa porque
  candidato y campeón se evalúan siempre sobre el mismo test.
- El caso "no entrena por decisión técnica" lo cubre el lote 3; no se forzó ningún rechazo.
