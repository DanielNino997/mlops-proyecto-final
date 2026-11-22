#!/usr/bin/env bash
# Crea (o actualiza) los Secrets del proyecto en el namespace mlops a partir de k8s/.env.
# Las credenciales nunca se versionan: k8s/.env está en .gitignore y los manifiestos
# solo referencian estos Secrets por nombre (secretKeyRef).
set -euo pipefail
cd "$(dirname "$0")/.."

ENV_FILE=k8s/.env
[ -f "$ENV_FILE" ] || { echo "Falta $ENV_FILE: copia k8s/.env.example y pon valores reales"; exit 1; }
set -a; source "$ENV_FILE"; set +a

NS=mlops
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

secret() {
  local name=$1; shift
  kubectl -n "$NS" create secret generic "$name" "$@" --dry-run=client -o yaml | kubectl apply -f -
}

secret data-db-credentials \
  --from-literal=POSTGRES_USER="$DATA_DB_USER" \
  --from-literal=POSTGRES_PASSWORD="$DATA_DB_PASSWORD" \
  --from-literal=RAW_DB_URL="postgresql://$DATA_DB_USER:$DATA_DB_PASSWORD@postgres-data:5432/raw_data" \
  --from-literal=CLEAN_DB_URL="postgresql://$DATA_DB_USER:$DATA_DB_PASSWORD@postgres-data:5432/clean_data"

secret mlflow-db-credentials \
  --from-literal=POSTGRES_USER="$MLFLOW_DB_USER" \
  --from-literal=POSTGRES_PASSWORD="$MLFLOW_DB_PASSWORD" \
  --from-literal=POSTGRES_DB=mlflow \
  --from-literal=MLFLOW_BACKEND_URI="postgresql://$MLFLOW_DB_USER:$MLFLOW_DB_PASSWORD@postgres-mlflow:5432/mlflow"

secret minio-credentials \
  --from-literal=MINIO_ROOT_USER="$MINIO_ROOT_USER" \
  --from-literal=MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
  --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"

secret api-admin \
  --from-literal=ADMIN_TOKEN="$API_ADMIN_TOKEN"


secret airflow-db-credentials \
  --from-literal=POSTGRES_USER="$AIRFLOW_DB_USER" \
  --from-literal=POSTGRES_PASSWORD="$AIRFLOW_DB_PASSWORD" \
  --from-literal=POSTGRES_DB=airflow

secret airflow-metadata \
  --from-literal=connection="postgresql://$AIRFLOW_DB_USER:$AIRFLOW_DB_PASSWORD@postgres-airflow:5432/airflow"

secret airflow-fernet-key \
  --from-literal=fernet-key="$AIRFLOW_FERNET_KEY"

secret airflow-webserver-secret \
  --from-literal=webserver-secret-key="$AIRFLOW_WEBSERVER_SECRET_KEY"

echo "Secrets en $NS:"; kubectl -n "$NS" get secrets
