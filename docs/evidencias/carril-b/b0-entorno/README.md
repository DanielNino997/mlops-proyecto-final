# B0 — Repo y entorno local de desarrollo

## Qué se hizo
Se creó la estructura del repo y un docker compose solo para desarrollo (`dev/docker-compose.dev.yml`)
que imita los servicios del clúster: Postgres de datos (bases `raw_data` y `clean_data` separadas),
Postgres para metadatos de MLflow, MinIO para artefactos, servidor MLflow y la API de datos del profesor.
Este compose no se despliega; el despliegue real lo hace Argo CD en Kubernetes.

## Cómo se probó
- `docker compose ps -a` → 01-compose-ps.txt
- `psql -c '\l'` en postgres-data → 02-psql-bases.txt
- logs de mlflow → 03-mlflow-bucket.txt
- `curl` a MLflow y a la API de datos → 04-healthchecks.txt
- capturas de MLflow, MinIO y Swagger → 05, 06, 07

## Resultado
- Los 5 servicios están arriba; Postgres y MinIO pasan su healthcheck.
- `raw_data` y `clean_data` existen como bases separadas (RF2).
- MLflow guarda metadatos en Postgres (no SQLite) y artefactos en el bucket `mlflow-artifacts` de MinIO.
- MLflow y la API de datos responden HTTP 200.

## Decisiones / problemas encontrados
- `minio/minio` y `minio/mc` ya no existen en Docker Hub (MinIO los retiró en sep 2026).
  Se usó `coollabsio/minio:RELEASE.2025-04-22T22-12-26Z`, imagen probada en clase.
  Como tampoco hay `mc`, el bucket lo crea `dev/create_bucket.py` (boto3) al arrancar MLflow.
- Los Postgres se exponen en 5433 y 5434 para no chocar con otros Postgres de la VM.
