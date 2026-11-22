FROM apache/airflow:2.10.5-python3.11
# Commit del código con el que se construyó la imagen: lib/train.py lo registra en MLflow (RF5).
ARG GIT_SHA=desconocido
ENV GIT_SHA=${GIT_SHA}
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && pip check || true
