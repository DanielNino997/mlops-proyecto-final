"""Transformación de un lote RAW a la forma de CLEAN (pura: no toca la base)."""
import numpy as np
import pandas as pd

from lib.validation import MAX_PRICE, MIN_PRICE, num

CLEAN_COLUMNS = ["raw_row_id", "brokered_by", "status", "price", "bed", "bath", "acre_lot",
                 "city", "state", "zip_code", "house_size", "prev_sold_date", "years_since_prev_sale"]


def _text(df, col):
    if col not in df:
        return pd.Series(pd.NA, index=df.index, dtype="string")
    return df[col].astype("string").str.strip()


def _code(df, col):
    """Códigos numéricos (zip, broker) como texto sin '.0'."""
    return num(df, col).round().astype("Int64").astype("string")


def preprocess(df, reference_date=None):
    """df trae raw_row_id + las columnas del registro. Devuelve el lote limpio."""
    today = pd.Timestamp(reference_date) if reference_date else pd.Timestamp.today().normalize()
    out = pd.DataFrame({"raw_row_id": df["raw_row_id"].astype("int64")})
    out["brokered_by"] = _code(df, "brokered_by")
    out["status"] = _text(df, "status")
    out["price"] = num(df, "price")
    for c in ("bed", "bath", "acre_lot"):
        v = num(df, c)
        out[c] = v.where(v >= 0)                     # negativos -> nulo
    hs = num(df, "house_size")
    out["house_size"] = hs.where(hs > 0)
    out["city"] = _text(df, "city")
    out["state"] = _text(df, "state")
    out["zip_code"] = _code(df, "zip_code").str.zfill(5)
    sold = pd.to_datetime(df["prev_sold_date"], errors="coerce") if "prev_sold_date" in df \
        else pd.Series(pd.NaT, index=df.index)
    out["prev_sold_date"] = sold.dt.date
    out["years_since_prev_sale"] = ((today - sold).dt.days / 365.25).round(2)

    out = out[out["price"].between(MIN_PRICE, MAX_PRICE)]   # target válido
    features = [c for c in CLEAN_COLUMNS if c != "raw_row_id"]
    out = out.drop_duplicates(subset=features)
    return out[CLEAN_COLUMNS].reset_index(drop=True)
