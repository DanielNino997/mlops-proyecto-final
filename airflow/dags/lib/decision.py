"""Decisión de entrenamiento a partir del resumen del lote (B3) y la degradación del campeón."""
RULES = {
    "min_volume_pct": 5.0,       # puerta: el lote debe aportar al menos este % del acumulado
    "volume_trigger_pct": 10.0,  # entrena si el lote aumenta el acumulado al menos este %
    "psi_threshold": 0.2,        # entrena si alguna variable supera este PSI
    "new_cat_pct": 5.0,          # entrena si las categorías nuevas cubren este % de filas
    "degradation_pct": 10.0,     # entrena si el campeón empeora su MAE este % sobre el lote
}


def decide_training(summary, has_champion, degradation=None, rules=RULES):
    """Devuelve (entrenar: bool, razón legible, disparadores: list)."""
    if summary.get("status") != "valido":
        return False, "no entrena: lote inválido (esquema o calidad)", []
    if not has_champion:
        return True, "entrena: línea base inicial, no existe modelo productivo", ["linea_base"]

    vol = summary.get("volume_increase_pct") or 0.0
    drift = {c: v["psi"] for c, v in summary.get("drift", {}).get("columnas", {}).items()
             if v.get("psi") is not None and v["psi"] > rules["psi_threshold"]}
    cats = {c: v["filas_con_nuevas_pct"] for c, v in summary.get("new_categories", {}).items()
            if c in ("state", "city") and v["filas_con_nuevas_pct"] >= rules["new_cat_pct"]}
    observado = []
    if drift:
        observado.append("drift en " + ", ".join(f"{c} (PSI {p:.2f})" for c, p in drift.items()))
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
