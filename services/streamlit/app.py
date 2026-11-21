"""Interfaz del proyecto final: inferencia contra la API y historial de lotes del DAG (RF9)."""
import os
from datetime import date

import pandas as pd
import psycopg2
import requests
import streamlit as st

from historial import narrar

API_URL = os.environ.get("API_URL", "http://localhost:8001").rstrip("/")
CLEAN_DB_URL = os.environ.get("CLEAN_DB_URL")
MLFLOW_UI_URL = os.environ.get("MLFLOW_UI_URL", "").rstrip("/")

st.set_page_config(page_title="Precio de vivienda — MLOps", layout="wide")
st.title("Precio de vivienda — Proyecto Final MLOps (grupo 2)")


def api_health():
    try:
        return requests.get(f"{API_URL}/health", timeout=5).json()
    except Exception as exc:  # noqa: BLE001
        return {"status": "sin_conexion", "error": str(exc)}


@st.cache_data(ttl=30)
def cargar_historial():
    conn = psycopg2.connect(CLEAN_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT id, processed_at, batch_number, final_status, trained, train_reason, promoted,
                                  decision_reason, model_version, mlflow_run_id, candidate_mae, champion_mae,
                                  delta_mae_pct, n_rows_raw, n_rows_clean, airflow_run_id,
                                  COALESCE(alertas, '[]'::jsonb) AS alertas
                           FROM audit.batch_history ORDER BY id""")
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


salud = api_health()
if salud.get("model_version"):
    st.success(f"Modelo productivo en la API: **{salud['model_name']} v{salud['model_version']}** "
               f"(alias `{salud.get('alias')}`, cargado {salud.get('loaded_at', '')[:19]} UTC)")
else:
    st.error(f"La API no tiene modelo cargado: {salud}")

tab_inf, tab_hist = st.tabs(["Inferencia", "Historial de entrenamiento y despliegue"])

with tab_inf:
    with st.form("prediccion"):
        c1, c2, c3 = st.columns(3)
        bed = c1.number_input("Habitaciones", min_value=0, max_value=50, value=3)
        bath = c2.number_input("Baños", min_value=0, max_value=50, value=2)
        house_size = c3.number_input("Área habitable (pies²)", min_value=100, max_value=50000, value=1600)
        acre_lot = c1.number_input("Terreno (acres)", min_value=0.0, max_value=1000.0, value=0.25, step=0.05)
        city = c2.text_input("Ciudad", "Austin")
        state = c3.text_input("Estado", "Texas")
        zip_code = c1.text_input("Código postal", "78701")
        status = c2.selectbox("Estado de la propiedad", ["for_sale", "ready_to_build", "sold"])
        prev = c3.date_input("Venta anterior (opcional)", value=None, min_value=date(1900, 1, 1))
        broker = c1.text_input("Agencia (código, opcional)", "")
        enviar = st.form_submit_button("Predecir precio", type="primary")

    if enviar:
        payload = {"bed": bed, "bath": bath, "acre_lot": acre_lot, "house_size": house_size, "city": city,
                   "state": state, "zip_code": zip_code, "status": status,
                   "brokered_by": broker or None, "prev_sold_date": prev.isoformat() if prev else None}
        try:
            r = requests.post(f"{API_URL}/predict", json=payload, timeout=15)
        except Exception as exc:  # noqa: BLE001
            st.error(f"No se pudo conectar con la API: {exc}")
        else:
            if r.ok:
                d = r.json()
                m1, m2, m3 = st.columns(3)
                m1.metric("Precio estimado", f"${d['predicted_price']:,.0f}")
                m2.metric("Versión del modelo usada", f"v{d['model_version']}")
                m3.metric("Latencia de inferencia", f"{d['latency_ms']} ms")
            else:
                st.error(f"La API respondió {r.status_code}: {r.text}")

with tab_hist:
    if not CLEAN_DB_URL:
        st.warning("Falta la variable CLEAN_DB_URL para leer el historial.")
    else:
        if st.button("Actualizar"):
            cargar_historial.clear()
        df = cargar_historial()
        if df.empty:
            st.info("Todavía no hay corridas del DAG registradas.")
        else:
            lotes = df[df["batch_number"].notna()]
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Lotes procesados", int(lotes["batch_number"].nunique()))
            c2.metric("Entrenamientos", int(df["trained"].fillna(False).sum()))
            c3.metric("Promociones", int((df["promoted"] == True).sum()))  # noqa: E712
            c4.metric("Rechazos", int((df["final_status"] == "rechazado").sum()))
            c5.metric("Lotes con alerta", int(df["alertas"].map(len).gt(0).sum()))

            st.subheader("Qué pasó en cada lote")
            for fila in df.to_dict("records"):
                st.markdown("- " + narrar(fila))
                if fila["alertas"]:
                    st.warning("⚠️ " + " · ".join(fila["alertas"])
                               + ("" if fila.get("trained") else "  —  alerta registrada, el lote NO reentrenó"))

            entrenados = df[df["trained"] == True].dropna(subset=["candidate_mae"])  # noqa: E712
            if not entrenados.empty:
                st.subheader("MAE en test: candidato vs campeón (USD, mismo conjunto de test)")
                graf = entrenados.set_index(entrenados["batch_number"].astype(int).astype(str))
                st.bar_chart(graf[["candidate_mae", "champion_mae"]].rename(
                    columns={"candidate_mae": "candidato", "champion_mae": "campeón anterior"}))

            st.subheader("Detalle")
            tabla = df.assign(alertas=df["alertas"].map(lambda a: " · ".join(a) if a else "")).rename(columns={"batch_number": "lote", "final_status": "estado", "trained": "entrenó",
                                       "promoted": "promovido", "model_version": "versión",
                                       "delta_mae_pct": "Δ MAE %", "mlflow_run_id": "run MLflow"})
            config = {}
            if MLFLOW_UI_URL:
                tabla["run MLflow"] = tabla["run MLflow"].map(
                    lambda r: f"{MLFLOW_UI_URL}/#/experiments/0/runs/{r}" if r else None)
                config["run MLflow"] = st.column_config.LinkColumn(display_text="abrir run")
            st.dataframe(tabla[["lote", "estado", "entrenó", "promovido", "versión", "Δ MAE %", "candidate_mae",
                                "champion_mae", "alertas", "n_rows_raw", "n_rows_clean", "run MLflow",
                                "processed_at"]],
                         column_config=config, hide_index=True, use_container_width=True)
