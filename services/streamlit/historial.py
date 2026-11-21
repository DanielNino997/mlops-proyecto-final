"""Convierte una fila de audit.batch_history en una frase legible (RF9)."""


def _verbo(texto):
    if not texto:
        return ""
    for antes, despues in (("no entrena: ", "no entrenó porque "), ("entrena: ", "entrenó por "),
                           ("se promueve: ", "se promovió: "), ("se rechaza: ", "se rechazó: ")):
        if texto.startswith(antes):
            return despues + texto[len(antes):]
    return texto


def narrar(fila):
    lote = fila.get("batch_number")
    estado = fila.get("final_status")
    if estado == "sin_datos":
        return f"**Sin lote nuevo:** {fila.get('decision_reason')}."
    if estado == "fallido":
        return f"**Lote {lote}: la corrida falló.** {fila.get('decision_reason')}"
    if estado == "duplicado":
        return f"**Lote {lote}: duplicado,** no se reprocesó."
    if estado == "invalido":
        return f"**Lote {lote}: inválido,** no entrenó. {_verbo(fila.get('train_reason'))}"
    texto = f"**Lote {lote}:** {_verbo(fila.get('train_reason'))}."
    if fila.get("trained"):
        texto += f" Candidato v{fila.get('model_version')}: {_verbo(fila.get('decision_reason'))}."
    return texto
