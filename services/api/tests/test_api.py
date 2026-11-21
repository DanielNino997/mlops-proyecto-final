import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.features import build_features

EJEMPLO = {"bed": 4, "bath": 2, "acre_lot": 0.38, "house_size": 1617, "city": "East Windsor",
           "state": "Connecticut", "zip_code": "06016", "status": "for_sale", "brokered_by": "101640",
           "prev_sold_date": "1999-09-30"}


class ModeloFalso:
    def predict(self, X):
        assert list(X.columns) == ["bed", "bath", "acre_lot", "house_size", "years_since_prev_sale",
                                   "city", "state", "zip_code", "brokered_by", "status"]
        return np.full(len(X), 250000.0)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main.manager, "refresh", lambda force=False: {"changed": False})
    monkeypatch.setattr(main.manager, "start_polling", lambda: None)
    monkeypatch.setattr(main, "log_inference", lambda *a, **k: None)
    monkeypatch.setattr(main, "ADMIN_TOKEN", "token-de-prueba")
    main.manager._model, main.manager._version = ModeloFalso(), "7"
    with TestClient(main.app) as c:
        yield c
    main.manager._model, main.manager._version = None, None


def test_health_muestra_version(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["model_version"] == "7"


def test_predict_ok(client):
    r = client.post("/predict", json=EJEMPLO)
    assert r.status_code == 200
    assert r.json()["predicted_price"] == 250000.0 and r.json()["model_version"] == "7"


def test_predict_invalido_da_422(client):
    r = client.post("/predict", json={**EJEMPLO, "bed": -1})
    assert r.status_code == 422


def test_reload_sin_token_da_401(client):
    assert client.post("/admin/reload").status_code == 401
    assert client.post("/admin/reload", headers={"X-Admin-Token": "token-de-prueba"}).status_code == 200


def test_metricas_expuestas(client):
    client.post("/predict", json=EJEMPLO)
    body = client.get("/metrics").text
    assert "predict_requests_total" in body and "predict_latency_seconds" in body and "model_version" in body


def test_sin_modelo_health_503(client):
    main.manager._model, main.manager._version = None, None
    assert client.get("/health").status_code == 503
    assert client.post("/predict", json=EJEMPLO).status_code == 503


def test_paridad_con_el_dag():
    """La API transforma la entrada exactamente igual que el DAG antes de entrenar."""
    from lib.preprocess import preprocess
    from lib.train import prepare_features
    raw = pd.DataFrame([{"raw_row_id": 1, "brokered_by": 101640.0, "status": "for_sale", "price": 289900.0,
                         "bed": 4.0, "bath": 2.0, "acre_lot": 0.38, "street": 1.0, "city": "East Windsor",
                         "state": "Connecticut", "zip_code": 6016.0, "house_size": 1617.0,
                         "prev_sold_date": "1999-09-30"}])
    dag = prepare_features(preprocess(raw, reference_date="2026-11-21")).reset_index(drop=True)
    api = build_features([EJEMPLO], today="2026-11-21")
    pd.testing.assert_frame_equal(dag, api, check_dtype=False)


def test_fast_predictor_igual_al_modelo():
    """Mismo resultado que model.predict, incluidas categorías nunca vistas."""
    from app.fast_model import FastPredictor
    from lib.train import build_model
    rng = np.random.default_rng(0)
    n = 400
    train = pd.DataFrame({
        "bed": rng.integers(1, 6, n).astype(float), "bath": rng.integers(1, 4, n).astype(float),
        "acre_lot": rng.random(n), "house_size": rng.integers(800, 4000, n).astype(float),
        "years_since_prev_sale": rng.random(n) * 20,
        "city": rng.choice(["A", "B", "C", "D"], n), "state": rng.choice(["TX", "FL"], n),
        "zip_code": rng.choice(["00001", "00002", "00003"], n), "brokered_by": rng.choice(["1", "2"], n),
        "status": rng.choice(["for_sale", "sold"], n)})
    y = 100000 + train["house_size"] * 150 + rng.normal(0, 5000, n)
    model = build_model().fit(train, y)
    nuevo = train.head(5).copy()
    nuevo.loc[0, "city"], nuevo.loc[1, "zip_code"], nuevo.loc[2, "status"] = "Nunca-vista", "99999", "otro"
    np.testing.assert_allclose(FastPredictor(model).predict(nuevo), model.predict(nuevo), rtol=1e-9)
