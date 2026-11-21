# Decisiones técnicas del pipeline

## Alertas de drift y decisión de reentrenamiento

### Idea central: alerta ≠ reentrenar
Cada lote pasa por las mismas verificaciones y **todas las alertas se registran siempre** en
`audit.batch_history.alertas`, en el log de Airflow (`log.warning "ALERTA lote N: …"`) y en el
historial de Streamlit. Que haya una alerta no obliga a entrenar: para entrenar el lote también
tiene que ser **suficiente** (aportar al menos 5 % del acumulado). Un lote pequeño con drift se
guarda en CLEAN y entra en el siguiente entrenamiento, pero no justifica por sí solo un modelo nuevo
entrenado con casi los mismos datos.

### Qué se mide por columna (`price`, `house_size`, `acre_lot`, `bed`, `bath`)
Lo calcula `detect_drift` en `airflow/dags/lib/validation.py`, comparando el lote contra una
muestra de hasta 200 000 filas del histórico en CLEAN.

| Medida | Qué detecta | Umbral de alerta | Por qué ese umbral |
|---|---|---|---|
| **PSI** (10 tramos por cuantiles del histórico) | Cambio en la forma de la distribución | > 0.2 | Convención usada en riesgo de crédito: < 0.1 estable, 0.1–0.2 cambio moderado, > 0.2 cambio significativo. |
| **KS** (estadístico) | Distancia máxima entre distribuciones acumuladas | Solo se reporta | Con cientos de miles de filas el p-valor de KS sale significativo casi siempre, así que no sirve como regla. |
| **std_ref, std_lote, std_ratio** | Lote mucho más disperso o mucho más concentrado que el histórico | std_ratio > 2 o < 0.5 | Duplicar o reducir a la mitad la dispersión cambia lo que el modelo ve en los extremos aunque la mediana se mantenga. El PSI puede no notarlo si los cuantiles centrales se parecen. |
| **p1_ref, p99_ref, fuera_rango_pct** | Valores fuera del rango conocido por el modelo | > 10 % del lote fuera de [p1, p99] | Si el lote viene de la misma distribución, se espera ~2 % fuera de ese rango. Superar 10 % (5 veces lo esperado) indica que el modelo tendrá que extrapolar. |

Además, se emite una alerta por **categorías nuevas** cuando cubren al menos 5 % de las filas del lote en
`state` o `city`, y por **degradación** cuando el MAE del campeón sobre el lote empeora al menos 10 %
frente al MAE con el que se registró.

### Cuándo se entrena (`decide_training` en `airflow/dags/lib/decision.py`)
1. Lote inválido (esquema o calidad) → no entrena.
2. No hay modelo productivo → entrena (línea base).
3. El lote aporta < 5 % del acumulado → **no entrena**, aunque haya alertas. Las alertas quedan registradas.
4. Si no, entrena cuando hay al menos un disparador: drift (cualquiera de las tres medidas de la
   tabla), categorías nuevas, volumen ≥ 10 % del acumulado o degradación del campeón ≥ 10 %.

Las mismas funciones (`drift_findings`, `new_category_findings`) alimentan la alerta y la decisión,
así que una alerta y un disparador siempre significan lo mismo.

### Ejemplo: lote 3
El lote 3 trae **4 054 filas**, solo **+1.02 %** del acumulado. Todas son de **Massachusetts**, un
estado que no aparecía en el histórico.

| Señal (de `docs/evidencias/carril-b/b3-validacion-clean/reporte-lote-3.json`) | Valor | ¿Alerta? |
|---|---|---|
| PSI de `price` | 0.41 (mediana 249 000 frente a 200 000 en el histórico) | Sí |
| PSI de `bath` | 0.19 | No (por debajo de 0.2) |
| Estado nuevo (`state`) | Massachusetts, 100 % de las filas | Sí |
| Ciudades nuevas (`city`) | 103 ciudades, 27 % de las filas | Sí |
| Volumen | +1.02 % del acumulado | Por debajo del mínimo de 5 % |

**Decisión:** no entrena. Hay drift real y categorías nuevas, pero 4 054 filas sobre ~400 000 no
cambiarían el modelo de forma medible, y con tan pocas filas el test del candidato sería casi
igual al del campeón. Las alertas quedan en el historial y los datos de Massachusetts entran en
CLEAN. Por eso aparecen en el entrenamiento del lote 4 (v4 se entrenó con los lotes 0-1-2-3-4).

*(Las columnas std_ratio y fuera_rango_pct del lote 3 aparecerán en `batch_history` cuando se
vuelva a correr el DAG con esta versión. Completar esta tabla con esos valores.)*
