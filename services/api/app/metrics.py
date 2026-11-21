"""Métricas para Prometheus."""
from prometheus_client import Counter, Gauge, Histogram

REQUESTS = Counter("predict_requests_total", "Peticiones a /predict por estado", ["status"])
LATENCY = Histogram("predict_latency_seconds", "Latencia de inferencia (solo modelo y features)",
                    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
MODEL_VERSION = Gauge("model_version", "Versión del modelo cargado (0 = ninguno)")
MODEL_LOADED = Gauge("model_loaded", "1 si hay un modelo cargado")
RELOADS = Counter("model_reload_total", "Intentos de recarga del modelo por resultado", ["result"])
