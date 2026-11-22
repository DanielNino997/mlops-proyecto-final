"""Validaciones de un lote: esquema, calidad, categorías nuevas y drift."""
import numpy as np
import pandas as pd
from scipy.stats import ks_2samp

NUMERIC = ["brokered_by", "price", "bed", "bath", "acre_lot", "street", "zip_code", "house_size"]
TEXT = ["status", "city", "state"]
EXPECTED = NUMERIC + TEXT + ["prev_sold_date"]
REQUIRED = ["price", "bed", "bath", "acre_lot", "house_size", "status", "city", "state", "zip_code"]
CATEGORICAL = ["status", "state", "city"]
DRIFT_COLS = ["price", "house_size", "acre_lot", "bed", "bath"]

MIN_PRICE, MAX_PRICE = 1_000, 100_000_000
MAX_INVALID_TARGET_PCT = 20.0


def num(df, col):
    """Columna como número (lo que no se pueda convertir queda NaN)."""
    if col not in df:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def validate_schema(df):
    cols = set(df.columns)
    missing = [c for c in EXPECTED if c not in cols]
    type_issues = {}
    for c in NUMERIC:
        if c in cols:
            s = df[c].dropna()
            bad = int(pd.to_numeric(s, errors="coerce").isna().sum())
            if bad:
                type_issues[c] = bad
    if "prev_sold_date" in cols:
        s = df["prev_sold_date"].dropna()
        bad = int(pd.to_datetime(s, errors="coerce").isna().sum())
        if bad:
            type_issues["prev_sold_date"] = bad
    missing_required = [c for c in missing if c in REQUIRED]
    return {
        "ok": not missing_required,
        "missing": missing,
        "missing_required": missing_required,
        "extra_ignored": sorted(cols - set(EXPECTED)),
        "type_issues": type_issues,
    }


def validate_quality(df):
    n = len(df)
    price = num(df, "price")
    invalid_target = price.isna() | (price < MIN_PRICE) | (price > MAX_PRICE)
    valid_log_price = np.log(price[~invalid_target])
    outliers = 0
    if len(valid_log_price) > 0:
        q1, q3 = valid_log_price.quantile([0.25, 0.75])
        iqr = q3 - q1
        outliers = int(((valid_log_price < q1 - 3 * iqr) | (valid_log_price > q3 + 3 * iqr)).sum())
    feature_cols = [c for c in df.columns if c in EXPECTED]
    invalid_pct = round(float(invalid_target.mean() * 100), 2) if n else 100.0
    return {
        "ok": n > 0 and invalid_pct <= MAX_INVALID_TARGET_PCT,
        "n_rows": n,
        "null_pct": {c: round(float(df[c].isna().mean() * 100), 2) for c in feature_cols},
        "duplicados": int(df.duplicated(subset=feature_cols).sum()),
        "target_invalido": int(invalid_target.sum()),
        "target_invalido_pct": invalid_pct,
        "rangos_invalidos": {
            "bed_negativo": int((num(df, "bed") < 0).sum()),
            "bath_negativo": int((num(df, "bath") < 0).sum()),
            "acre_lot_negativo": int((num(df, "acre_lot") < 0).sum()),
            "house_size_no_positivo": int((num(df, "house_size") <= 0).sum()),
        },
        "outliers_log_precio_iqr3": outliers,
    }


def detect_new_categories(df, known):
    """known: {columna: set(valores ya vistos en CLEAN)}."""
    n = len(df)
    result = {}
    for c in CATEGORICAL:
        if c not in df:
            continue
        values = df[c].dropna().astype(str)
        is_new = ~values.isin(known.get(c, set()))
        counts = values[is_new].value_counts()
        result[c] = {
            "n_nuevas": int(len(counts)),
            "filas_con_nuevas_pct": round(float(is_new.sum() / n * 100), 2) if n else 0.0,
            "ejemplos": {str(k): int(v) for k, v in counts.head(5).items()},
        }
    return result


def psi(reference, current, bins=10):
    """Population Stability Index con cortes por cuantiles de la referencia."""
    reference, current = reference.dropna(), current.dropna()
    if len(reference) < 50 or len(current) < 50:
        return None
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    ref_pct = np.clip(np.histogram(reference, edges)[0] / len(reference), 1e-6, None)
    cur_pct = np.clip(np.histogram(current, edges)[0] / len(current), 1e-6, None)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def _round(x, nd=4):
    return None if x is None or not np.isfinite(x) else round(float(x), nd)


def variation_and_range(old, new):
    """Dispersión del lote frente al histórico y fracción del lote fuera del rango p1–p99 del histórico.

    Si el lote viene de la misma distribución, std_ratio ≈ 1 y ~2 % de sus filas cae fuera de [p1, p99].
    """
    if old.empty or new.empty:
        return {"std_ref": None, "std_lote": None, "std_ratio": None,
                "p1_ref": None, "p99_ref": None, "fuera_rango_pct": None}
    std_ref, std_lote = float(old.std()), float(new.std())
    p1, p99 = (float(v) for v in old.quantile([0.01, 0.99]))
    fuera = float(((new < p1) | (new > p99)).mean() * 100)
    return {"std_ref": _round(std_ref, 2), "std_lote": _round(std_lote, 2),
            "std_ratio": _round(std_lote / std_ref, 3) if std_ref > 0 else None,
            "p1_ref": _round(p1, 2), "p99_ref": _round(p99, 2), "fuera_rango_pct": round(fuera, 2)}


def detect_drift(df, reference):
    if reference is None or reference.empty:
        return {"referencia": False, "columnas": {}}
    columns = {}
    for c in DRIFT_COLS:
        new, old = num(df, c).dropna(), num(reference, c).dropna()
        value = psi(old, new)
        ks = ks_2samp(old, new).statistic if len(old) and len(new) else None
        columns[c] = {
            "psi": None if value is None else round(value, 4),
            "ks_stat": None if ks is None else round(float(ks), 4),
            "mediana_ref": None if old.empty else float(old.median()),
            "mediana_lote": None if new.empty else float(new.median()),
            **variation_and_range(old, new),
        }
    return {"referencia": True, "columnas": columns}
