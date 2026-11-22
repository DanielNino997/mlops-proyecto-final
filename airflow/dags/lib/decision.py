"""Decisión de entrenamiento y alertas de cada lote (B3) a partir del resumen y la degradación del campeón.

Alerta ≠ reentrenar: las alertas se calculan y registran SIEMPRE; entrenar exige además que el lote
sea suficiente (puerta de volumen). Ver docs/decisiones.md.
"""
RULES = {
    "min_volume_pct": 5.0,       # puerta: el lote debe aportar al menos este % del acumulado
    "volume_trigger_pct": 10.0,  # entrena si el lote aumenta el acumulado al menos este %
    "psi_threshold": 0.2,        # drift si alguna variable supera este PSI
    "std_ratio_max": 2.0,        # drift si la desviación del lote es > 2x o < 1/2 de la del histórico
    "out_of_range_pct": 10.0,    # drift si más de este % del lote cae fuera de [p1, p99] del histórico (~2 % esperado)
    "new_cat_pct": 5.0,          # alerta/entrena si las categorías nuevas cubren este % de filas
    "degradation_pct": 10.0,     # alerta/entrena si el campeón empeora su MAE este % sobre el lote
}


def drift_findings(drift, rules=RULES):
    """{columna: [motivos]} con las columnas que cruzan algún umbral de drift."""
    found = {}
    for col, v in (drift or {}).get("columnas", {}).items():
        reasons = []
        if v.get("psi") is not None and v["psi"] > rules["psi_threshold"]:
            reasons.append(f"PSI {v['psi']:.2f}")
        ratio = v.get("std_ratio")
        if ratio is not None and (ratio > rules["std_ratio_max"] or ratio < 1 / rules["std_ratio_max"]):
            reasons.append(f"std x{ratio:.2f}")
        out = v.get("fuera_rango_pct")
        if out is not None and out > rules["out_of_range_pct"]:
            reasons.append(f"{out:.0f} % fuera de p1–p99")
        if reasons:
            found[col] = reasons
    return found


def new_category_findings(new_categories, rules=RULES):
    return {c: v["filas_con_nuevas_pct"] for c, v in (new_categories or {}).items()
            if c in ("state", "city") and v.get("filas_con_nuevas_pct", 0) >= rules["new_cat_pct"]}


def build_alerts(summary, degradation=None, rules=RULES):
    """Lista legible de alertas del lote. Se llena se entrene o no."""
    alerts = [f"drift: {c} " + ", ".join(r) for c, r in drift_findings(summary.get("drift"), rules).items()]
    alerts += [f"categorías nuevas: {c} {p:.0f} % filas"
               for c, p in new_category_findings(summary.get("new_categories"), rules).items()]
    if degradation and degradation["degradacion_pct"] >= rules["degradation_pct"]:
        alerts.append(f"degradación: campeón v{degradation['champion_version']} "
                      f"{degradation['degradacion_pct']:+.1f} % MAE sobre el lote")
    return alerts


def decide_training(summary, has_champion, degradation=None, rules=RULES):
    """Devuelve (entrenar: bool, razón legible, disparadores: list)."""
    if summary.get("status") != "valido":
        return False, "no entrena: lote inválido (esquema o calidad)", []
    if not has_champion:
        return True, "entrena: línea base inicial, no existe modelo productivo", ["linea_base"]

    vol = summary.get("volume_increase_pct") or 0.0
    drift = drift_findings(summary.get("drift"), rules)
    cats = new_category_findings(summary.get("new_categories"), rules)
    observado = []
    if drift:
        observado.append("drift en " + ", ".join(f"{c} ({'; '.join(r)})" for c, r in drift.items()))
    if cats:
        observado.append("categorías nuevas en " + ", ".join(f"{c} ({p:.1f} % filas)" for c, p in cats.items()))

    if vol < rules["min_volume_pct"]:
        extra = f"; se observó {' y '.join(observado)}" if observado else ""
        return (False, f"no entrena: lote insuficiente (+{vol:.2f} % del acumulado, mínimo "
                       f"{rules['min_volume_pct']} %){extra}; sus datos quedan en CLEAN para el próximo entrenamiento",
                [])

    triggers = list(observado)
    if vol >= rules["volume_trigger_pct"]:
        triggers.append(f"volumen +{vol:.1f} % del acumulado")
    if degradation and degradation["degradacion_pct"] >= rules["degradation_pct"]:
        triggers.append(f"campeón v{degradation['champion_version']} degradado "
                        f"{degradation['degradacion_pct']:+.1f} % en MAE sobre el lote")
    if triggers:
        return True, "entrena: " + "; ".join(triggers), triggers
    return False, "no entrena: sin drift, categorías nuevas, volumen ni degradación relevantes", []
