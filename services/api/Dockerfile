FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
ENV OMP_NUM_THREADS=1
EXPOSE 8000
# 1 worker: el modelo cargado y las métricas viven en un solo proceso (consistentes);
# para escalar se agregan réplicas en Kubernetes, no workers.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
