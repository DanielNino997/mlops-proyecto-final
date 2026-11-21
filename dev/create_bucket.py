"""Crea el bucket de artefactos de MLflow en MinIO si no existe (solo entorno dev)."""
import os
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKET = os.environ.get("MLFLOW_BUCKET", "mlflow-artifacts")
s3 = boto3.client("s3", endpoint_url=os.environ["MLFLOW_S3_ENDPOINT_URL"])

for intento in range(30):
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"bucket '{BUCKET}' ya existe")
        break
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"bucket '{BUCKET}' creado")
        break
    except EndpointConnectionError:
        print(f"esperando a MinIO... (intento {intento + 1})")
        time.sleep(2)
else:
    raise SystemExit("MinIO no respondió: no se pudo crear el bucket")
