"""Prueba de carga de la API de inferencia con propiedades reales (RF10)."""
import json
import random
from pathlib import Path

from locust import HttpUser, between, task

PAYLOADS = json.loads((Path(__file__).parent / "payloads.json").read_text())


class UsuarioInferencia(HttpUser):
    wait_time = between(0.5, 2)

    @task(9)
    def predecir(self):
        self.client.post("/predict", json=random.choice(PAYLOADS), name="/predict")

    @task(1)
    def salud(self):
        self.client.get("/health", name="/health")
