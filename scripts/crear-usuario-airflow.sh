#!/usr/bin/env bash
# Crea el usuario administrador de la UI de Airflow con las credenciales de k8s/.env.
# Se corre una vez, cuando el webserver de Airflow está arriba. Si el usuario ya existe, no hace nada.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source k8s/.env; set +a

kubectl -n mlops exec deploy/airflow-webserver -c webserver -- \
  airflow users create \
    --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Admin --lastname MLOps --role Admin --email admin@mlops.local
