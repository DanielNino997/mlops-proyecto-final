import numpy as np
import pandas as pd

from lib.preprocess import preprocess
from lib.validation import detect_new_categories, psi, validate_quality, validate_schema

FILA = {"brokered_by": 101640.0, "status": "for_sale", "price": 289900.0, "bed": 4.0, "bath": 2.0,
        "acre_lot": 0.38, "street": 1758218.0, "city": "East Windsor", "state": "Connecticut",
        "zip_code": 6016.0, "house_size": 1617.0, "prev_sold_date": "1999-09-30"}


def test_columna_extra_se_ignora_y_faltante_se_reporta():
    df = pd.DataFrame([{**FILA, "nueva_col": 1}]).drop(columns=["street"])
    r = validate_schema(df)
    assert r["ok"] is True                      # street no es requerida
    assert r["extra_ignored"] == ["nueva_col"] and r["missing"] == ["street"]


def test_falta_columna_requerida_invalida_el_lote():
    r = validate_schema(pd.DataFrame([FILA]).drop(columns=["price"]))
    assert r["ok"] is False and r["missing_required"] == ["price"]


def test_precio_invalido_se_cuenta():
    df = pd.DataFrame([FILA, {**FILA, "price": -5.0}, {**FILA, "price": None}])
    r = validate_quality(df)
    assert r["target_invalido"] == 2 and r["ok"] is False      # 66 % > 20 %


def test_detecta_categoria_nueva():
    df = pd.DataFrame([FILA, {**FILA, "state": "Ohio"}])
    r = detect_new_categories(df, {"state": {"Connecticut"}, "city": {"East Windsor"}, "status": {"for_sale"}})
    assert r["state"]["n_nuevas"] == 1 and r["state"]["filas_con_nuevas_pct"] == 50.0
    assert r["city"]["n_nuevas"] == 0


def test_psi_detecta_cambio_de_distribucion():
    rng = np.random.default_rng(0)
    base = pd.Series(rng.normal(0, 1, 5000))
    assert psi(base, pd.Series(rng.normal(0, 1, 5000))) < 0.1
    assert psi(base, pd.Series(rng.normal(1.5, 1, 5000))) > 0.2


def test_preprocess_limpia_y_conserva_trazabilidad():
    df = pd.DataFrame([FILA, {**FILA, "price": -5.0}, {**FILA, "bed": -1.0, "city": "Hartford"}, FILA])
    df.insert(0, "raw_row_id", [10, 11, 12, 13])
    out = preprocess(df, reference_date="2026-01-01")
    assert list(out["raw_row_id"]) == [10, 12]                 # sin precio negativo ni duplicado
    assert out.loc[0, "zip_code"] == "06016"
    assert pd.isna(out.loc[1, "bed"])                          # negativo -> nulo
    assert out.loc[0, "years_since_prev_sale"] > 26
