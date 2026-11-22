# Servidor MLflow con los drivers que necesita: Postgres (metadatos) y S3/MinIO (artefactos).
# Se construye en GitHub Actions; nunca en la máquina de despliegue.
FROM ghcr.io/mlflow/mlflow:v2.17.2
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 boto3==1.35.99
EXPOSE 5000
