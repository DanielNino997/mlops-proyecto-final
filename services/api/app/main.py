"""API de inferencia del precio de vivienda. El modelo productivo sale de MLflow (alias champion)."""
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.db import log_inference
from app.features import build_features
from app.metrics import LATENCY, REQUESTS
from app.model_manager import ModelManager
from app.schemas import PredictionOut, PropertyIn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

MODEL_NAME = os.environ.get("MODEL_NAME", "house-price-model")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
manager = ModelManager(MODEL_NAME, os.environ.get("MODEL_ALIAS", "champion"),
                       int(os.environ.get("RELOAD_INTERVAL_SECONDS", "30")))


@asynccontextmanager
async def lifespan(_app):
    manager.refresh()          # si MLflow no responde, la API arranca igual y /health da 503
    manager.start_polling()
    yield
    manager.stop()


app = FastAPI(title="API de inferencia — precio de vivienda", version="1.0", lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def invalid_input(request: Request, exc: RequestValidationError):
    REQUESTS.labels("invalid").inc()
    log_inference(None, None, MODEL_NAME, manager.current()[1], "invalid", str(exc.errors())[:500], None)
    return await request_validation_exception_handler(request, exc)


@app.get("/live")
def live():
    return {"status": "alive"}


@app.get("/health")
def health():
    info = manager.info()
    ok = info["model_version"] is not None
    return JSONResponse({"status": "ok" if ok else "sin_modelo", **info}, status_code=200 if ok else 503)


@app.post("/predict", response_model=PredictionOut)
def predict(item: PropertyIn, background: BackgroundTasks):
    payload = item.model_dump(mode="json")
    model, version = manager.current()
    if model is None:
        REQUESTS.labels("error").inc()
        background.add_task(log_inference, payload, None, MODEL_NAME, None, "error", "sin modelo cargado", None)
        raise HTTPException(status_code=503, detail="No hay modelo productivo cargado")
    t0 = time.perf_counter()
    try:
        price = float(model.predict(build_features([payload]))[0])
    except Exception as exc:  # noqa: BLE001
        REQUESTS.labels("error").inc()
        background.add_task(log_inference, payload, None, MODEL_NAME, version, "error", str(exc)[:500], None)
        raise HTTPException(status_code=500, detail=f"Error de inferencia: {exc}") from exc
    elapsed = time.perf_counter() - t0
    LATENCY.observe(elapsed)
    REQUESTS.labels("ok").inc()
    latency_ms = round(elapsed * 1000, 2)
    background.add_task(log_inference, payload, price, MODEL_NAME, version, "ok", None, latency_ms)
    return PredictionOut(predicted_price=round(price, 2), model_name=MODEL_NAME, model_version=version,
                         latency_ms=latency_ms)


@app.post("/admin/reload")
def admin_reload(force: bool = False, x_admin_token: Optional[str] = Header(default=None)):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Token de administración inválido")
    return manager.refresh(force=force)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
