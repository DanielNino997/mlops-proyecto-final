from lib.decision import decide_training

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
