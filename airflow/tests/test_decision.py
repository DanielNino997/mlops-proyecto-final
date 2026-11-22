from lib.decision import build_alerts, decide_training

BASE = {"status": "valido", "volume_increase_pct": 50.0,
        "drift": {"columnas": {"price": {"psi": 0.01}}},
        "new_categories": {"state": {"filas_con_nuevas_pct": 0.0}, "city": {"filas_con_nuevas_pct": 1.0}}}


def test_sin_campeon_entrena_linea_base():
    ok, reason, _ = decide_training(BASE, has_champion=False)
    assert ok and "línea base" in reason


def test_lote_invalido_no_entrena():
    ok, _, _ = decide_training({**BASE, "status": "invalido"}, has_champion=True)
    assert not ok


def test_lote_pequeno_no_entrena_aunque_haya_drift():
    s = {**BASE, "volume_increase_pct": 1.0, "drift": {"columnas": {"price": {"psi": 0.41}}}}
    ok, reason, _ = decide_training(s, has_champion=True)
    assert not ok and "insuficiente" in reason and "drift" in reason


def test_drift_dispara_entrenamiento():
    s = {**BASE, "volume_increase_pct": 8.0, "drift": {"columnas": {"price": {"psi": 12.2}}}}
    ok, reason, _ = decide_training(s, has_champion=True)
    assert ok and "price" in reason


def test_sin_cambios_no_entrena():
    ok, _, _ = decide_training({**BASE, "volume_increase_pct": 6.0}, has_champion=True)
    assert not ok


def test_alertas_se_llenan_aunque_no_entrene():
    # Caso del lote 3: drift de precio y estado nuevo, pero solo +1 % de volumen
    s = {**BASE, "volume_increase_pct": 1.02,
         "drift": {"columnas": {"price": {"psi": 0.41, "std_ratio": 1.1, "fuera_rango_pct": 3.0}}},
         "new_categories": {"state": {"filas_con_nuevas_pct": 100.0}, "city": {"filas_con_nuevas_pct": 27.1}}}
    ok, _, _ = decide_training(s, has_champion=True)
    alerts = build_alerts(s)
    assert not ok
    assert "drift: price PSI 0.41" in alerts
    assert "categorías nuevas: state 100 % filas" in alerts and "categorías nuevas: city 27 % filas" in alerts


def test_rango_y_dispersion_disparan_drift_sin_psi():
    s = {**BASE, "volume_increase_pct": 8.0,
         "drift": {"columnas": {"house_size": {"psi": 0.05, "std_ratio": 0.3, "fuera_rango_pct": 95.0}}}}
    ok, reason, _ = decide_training(s, has_champion=True)
    assert ok and "house_size" in reason and "std x0.30" in reason and "95 % fuera de p1–p99" in reason


def test_sin_hallazgos_no_hay_alertas():
    assert build_alerts(BASE) == []
