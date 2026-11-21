"""Carga el modelo productivo desde MLflow (alias) y lo recarga sin redesplegar la API."""
import logging
import threading
import time
from datetime import datetime, timezone

import mlflow
from mlflow import MlflowClient

from app.fast_model import FastPredictor
from app.metrics import MODEL_LOADED, MODEL_VERSION, RELOADS

log = logging.getLogger(__name__)


class ModelManager:
    def __init__(self, model_name, alias, interval_seconds):
        self.model_name, self.alias, self.interval = model_name, alias, interval_seconds
        self._lock = threading.Lock()          # protege el cambio de referencia del modelo
        self._refresh_lock = threading.Lock()  # evita dos descargas al mismo tiempo
        self._model, self._version, self._run_id, self._loaded_at = None, None, None, None
        self.last_error = None
        self._stop = threading.Event()

    def current(self):
        """(modelo, versión) leídos juntos: una petición nunca mezcla modelo y versión."""
        with self._lock:
            return self._model, self._version

    def info(self):
        with self._lock:
            return {"model_name": self.model_name, "alias": self.alias, "model_version": self._version,
                    "run_id": self._run_id, "loaded_at": self._loaded_at, "last_error": self.last_error}

    def refresh(self, force=False):
        """Consulta el alias en MLflow; si cambió (o force), carga el modelo nuevo y lo intercambia."""
        with self._refresh_lock:
            previous = self._version
            try:
                mv = MlflowClient().get_model_version_by_alias(self.model_name, self.alias)
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"no se pudo consultar el alias: {exc}"
                RELOADS.labels("error").inc()
                log.warning(self.last_error)
                return {"changed": False, "model_version": previous, "error": self.last_error}

            if mv.version == previous and not force:
                return {"changed": False, "model_version": previous}

            try:
                model = FastPredictor(mlflow.sklearn.load_model(f"models:/{self.model_name}/{mv.version}"))
            except Exception as exc:  # noqa: BLE001
                self.last_error = f"falló la descarga de v{mv.version}; se mantiene v{previous}: {exc}"
                RELOADS.labels("error").inc()
                log.error(self.last_error)
                return {"changed": False, "model_version": previous, "error": self.last_error}

            with self._lock:   # intercambio atómico: el modelo nuevo ya está completo en memoria
                self._model, self._version, self._run_id = model, mv.version, mv.run_id
                self._loaded_at = datetime.now(timezone.utc).isoformat()
                self.last_error = None
            MODEL_VERSION.set(int(mv.version))
            MODEL_LOADED.set(1)
            RELOADS.labels("ok").inc()
            log.info("Modelo %s cargado: v%s (antes v%s)", self.model_name, mv.version, previous)
            return {"changed": True, "previous_version": previous, "model_version": mv.version}

    def start_polling(self):
        def loop():
            while not self._stop.wait(self.interval):
                self.refresh()
        threading.Thread(target=loop, name="model-poller", daemon=True).start()

    def stop(self):
        self._stop.set()
