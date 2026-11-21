"""Misma transformación que preprocess() + prepare_features() del DAG (ver tests/test_api.py::test_paridad)."""
import pandas as pd

NUM_COLS = ["bed", "bath", "acre_lot", "house_size", "years_since_prev_sale"]
TE_COLS = ["city", "state", "zip_code", "brokered_by"]
OH_COLS = ["status"]


def _num(df, col):
    return pd.to_numeric(df[col], errors="coerce").astype("float64")


def _code(df, col):
    return pd.to_numeric(df[col], errors="coerce").round().astype("Int64").astype("string")


def build_features(items, today=None):
    df = pd.DataFrame(items)
    today = pd.Timestamp(today) if today else pd.Timestamp.today().normalize()
    X = pd.DataFrame(index=df.index)
    for c in ("bed", "bath", "acre_lot"):
        v = _num(df, c)
        X[c] = v.where(v >= 0)
    hs = _num(df, "house_size")
    X["house_size"] = hs.where(hs > 0)
    sold = pd.to_datetime(df["prev_sold_date"], errors="coerce")
    X["years_since_prev_sale"] = ((today - sold).dt.days / 365.25).round(2)
    X["city"] = df["city"].astype("string").str.strip()
    X["state"] = df["state"].astype("string").str.strip()
    X["zip_code"] = _code(df, "zip_code").str.zfill(5)
    X["brokered_by"] = _code(df, "brokered_by")
    X["status"] = df["status"].astype("string").str.strip()
    for c in TE_COLS + OH_COLS:
        X[c] = X[c].astype("object").where(X[c].notna(), "desconocido").astype(str)
    return X[NUM_COLS + TE_COLS + OH_COLS]
