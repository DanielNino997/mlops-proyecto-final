from historial import narrar


def test_lote_promovido():
    t = narrar({"batch_number": 4, "final_status": "promovido", "trained": True, "model_version": "4",
                "train_reason": "entrena: drift en price (PSI 12.19)",
                "decision_reason": "se promueve: MAE -44.77 % vs campeón v3"})
    assert "Lote 4" in t and "entrenó por drift" in t and "v4" in t and "se promovió" in t


def test_lote_no_entrena():
    t = narrar({"batch_number": 3, "final_status": "no_entrena", "trained": False,
                "train_reason": "no entrena: lote insuficiente (+1.02 %)"})
    assert "no entrenó porque lote insuficiente" in t and "Candidato" not in t


def test_fin_de_datos():
    assert "Sin lote nuevo" in narrar({"final_status": "sin_datos", "decision_reason": "la API ya entregó todo"})
