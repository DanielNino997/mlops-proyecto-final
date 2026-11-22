#!/usr/bin/env bash
# Deja el sistema listo para una corrida limpia de todos los lotes:
#   1. pausa el DAG
#   2. vacía raw_data, clean_data y el registro de inferencias
#   3. borra el modelo registrado y los runs de entrenamiento en MLflow
#   4. borra el historial de corridas del DAG en Airflow
#   5. reinicia la API de inferencia (arranca sin modelo hasta que exista un champion nuevo)
#   6. reinicia el contador de lotes del grupo en la API de datos
# No cambia nada de lo que está en Git: solo limpia datos y estado.
set -euo pipefail
NS=mlops
DATA_API_URL=${DATA_API_URL:-http://10.43.97.110:8000}
GROUP=${GROUP_NUMBER:-2}

read -r -p "Esto BORRA datos, modelos e historial de corridas. Escribe SI para continuar: " ok
[ "$ok" = "SI" ] || { echo "Cancelado"; exit 1; }

echo "== 1/6 Pausar el DAG"
kubectl -n "$NS" exec airflow-scheduler-0 -c scheduler -- airflow dags pause pipeline_lotes

echo "== 2/6 Vaciar raw_data y clean_data"
kubectl -n "$NS" exec postgres-data-0 -- sh -c \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d raw_data -c "TRUNCATE batches, properties_raw, inference_events RESTART IDENTITY CASCADE;"'
kubectl -n "$NS" exec postgres-data-0 -- sh -c \
  'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d clean_data -c "TRUNCATE properties_clean, audit.batch_history RESTART IDENTITY;"'

echo "== 3/6 Borrar el modelo registrado y los runs en MLflow"
kubectl -n "$NS" exec -i deploy/mlflow -- python - <<'PY'
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
c = MlflowClient("http://localhost:5000")
try:
    c.delete_registered_model("house-price-model"); print("modelo house-price-model borrado")
except MlflowException as e:
    print("modelo:", e.error_code)
exp = c.get_experiment_by_name("house-price")
if exp:
    runs = c.search_runs([exp.experiment_id], max_results=1000)
    for r in runs:
        c.delete_run(r.info.run_id)
    print(f"{len(runs)} runs de house-price borrados")
prueba = c.get_experiment_by_name("prueba-plataforma")
if prueba and prueba.lifecycle_stage == "active":
    c.delete_experiment(prueba.experiment_id); print("experimento prueba-plataforma borrado")
PY

echo "== 4/6 Borrar el historial de corridas del DAG (Airflow lo vuelve a leer de git-sync, pausado)"
kubectl -n "$NS" exec airflow-scheduler-0 -c scheduler -- airflow dags delete pipeline_lotes -y

echo "== 5/6 Reiniciar la API de inferencia"
kubectl -n "$NS" delete pod -l app=api --wait=false

echo "== 6/6 Reiniciar el contador de lotes del grupo $GROUP en $DATA_API_URL"
kubectl -n "$NS" run reinicio-lotes --rm -i --restart=Never --image=curlimages/curl:8.11.1 -- \
  curl -s "$DATA_API_URL/restart_data_generation?group_number=$GROUP"
echo
echo "Listo. Activa el DAG en Airflow y dispara una corrida por lote."
