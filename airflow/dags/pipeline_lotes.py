"""
### pipeline_lotes — Proyecto Final MLOps (grupo 2)

Cada corrida consume **un** lote de la API de datos y lo lleva por:
ingesta → RAW → validaciones → CLEAN → decisión de entrenamiento → (entrenamiento → MLflow →
comparación con el campeón → promoción o rechazo) → historial en `audit.batch_history`.

- Bifurcación 1 (`decide_training`): reglas de `lib/decision.py` (volumen mínimo, drift PSI, categorías nuevas,
  volumen, degradación del campeón). Nunca por calendario ni por número de lotes.
- Bifurcación 2 (`decide_promotion`): promover si el MAE baja >= 3 % y el RMSE no empeora más de 1 %.
- Los lotes no viajan por XCom (pesan hasta 60 MB): se guardan en STAGING_DIR y por XCom van solo resúmenes.
"""
import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)

STAGING = Path(os.environ.get("STAGING_DIR", "/tmp/pipeline_lotes"))
GROUP = int(os.environ.get("GROUP_NUMBER", "2"))
API_URL = os.environ.get("DATA_API_URL", "http://data-api")
AFTER_BRANCH = "none_failed_min_one_success"


def _run_dir(ctx):
    d = STAGING / ctx["run_id"].replace(":", "_").replace("+", "_")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _on_task_failure(context):
    """Un error crítico queda registrado como 'fallido' (no se disfraza de éxito)."""
    from lib.audit import write_history
    ti = context["task_instance"]
    batch = ti.xcom_pull(task_ids="store_raw_batch") or ti.xcom_pull(task_ids="fetch_batch_from_api") or {}
    write_history({"batch_id": batch.get("batch_id"), "group_number": GROUP,
                   "batch_number": batch.get("batch_number"), "airflow_run_id": context["run_id"],
                   "final_status": "fallido",
                   "decision_reason": f"falló la tarea {ti.task_id}: {context.get('exception')}"[:1000]})


def _clean_in_memory(batch_id):
    from lib.batch_processing import load_raw_batch
    from lib.preprocess import preprocess
    return preprocess(load_raw_batch(batch_id)[1])


@dag(
    dag_id="pipeline_lotes",
    schedule=os.environ.get("PIPELINE_SCHEDULE") or None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "carril-b", "retries": 0, "on_failure_callback": _on_task_failure},
    tags=["mlops", "proyecto-final"],
    doc_md=__doc__,
)
def pipeline_lotes():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def fetch_batch_from_api():
        from lib.api_client import NoMoreBatches, fetch_batch
        from lib.audit import write_history
        from lib.db import ensure_schema
        ctx = get_current_context()
        ensure_schema()
        try:
            body = fetch_batch(API_URL, GROUP)
        except NoMoreBatches as exc:
            write_history({"group_number": GROUP, "airflow_run_id": ctx["run_id"], "final_status": "sin_datos",
                           "decision_reason": f"la API ya entregó todos los lotes: {exc}"})
            raise AirflowSkipException(f"Fin de datos: {exc}")
        path = _run_dir(ctx) / "lote.json"
        path.write_text(json.dumps(body))
        return {"path": str(path), "group_number": body["group_number"],
                "batch_number": body["batch_number"], "n_rows": len(body["data"])}

    @task
    def store_raw_batch(fetched):
        from lib.audit import write_history
        from lib.db import connect
        from lib.storage import store_raw_batch as store
        ctx = get_current_context()
        conn = connect("RAW_DB_URL")
        try:
            res = store(conn, json.loads(Path(fetched["path"]).read_text()), airflow_run_id=ctx["run_id"])
        finally:
            conn.close()
        if res["duplicate"]:
            write_history({"batch_id": res["batch_id"], "group_number": GROUP, "batch_number": res["batch_number"],
                           "airflow_run_id": ctx["run_id"], "final_status": "duplicado",
                           "decision_reason": "el lote ya estaba en RAW: no se reprocesa (RF1)"})
            raise AirflowSkipException("Lote duplicado")
        return {**fetched, "batch_id": res["batch_id"], "payload_hash": res["payload_hash"]}

    @task
    def validate_schema(batch):
        from lib.batch_processing import load_raw_batch
        from lib.validation import validate_schema as check
        return check(load_raw_batch(batch["batch_id"])[1].drop(columns=["raw_row_id"]))

    @task
    def validate_data_quality(batch):
        from lib.batch_processing import load_raw_batch
        from lib.validation import validate_quality
        return validate_quality(load_raw_batch(batch["batch_id"])[1].drop(columns=["raw_row_id"]))

    @task
    def detect_new_categories(batch, schema, quality):
        from lib.batch_processing import load_known_categories
        from lib.validation import detect_new_categories as detect
        if not (schema["ok"] and quality["ok"]):
            return {}
        return detect(_clean_in_memory(batch["batch_id"]), load_known_categories(batch["batch_id"]))

    @task
    def detect_data_drift(batch, schema, quality):
        from lib.batch_processing import count_clean, load_reference_sample
        from lib.validation import detect_drift
        if not (schema["ok"] and quality["ok"]):
            return {}
        bid = batch["batch_id"]
        reference = load_reference_sample(bid) if count_clean(bid) else None
        return detect_drift(_clean_in_memory(bid), reference)

    @task
    def preprocess_data(batch, schema, quality):
        from lib.batch_processing import count_clean, load_raw_batch, write_clean
        from lib.preprocess import preprocess
        raw = load_raw_batch(batch["batch_id"])[1]
        if not (schema["ok"] and quality["ok"]):
            return {"status": "invalido", "n_rows_raw": len(raw), "n_rows_clean": 0}
        before = count_clean(batch["batch_id"])
        n = write_clean(batch["batch_id"], preprocess(raw))
        return {"status": "valido", "n_rows_raw": len(raw), "n_rows_clean": n, "n_total_clean_before": before,
                "volume_increase_pct": round(n / before * 100, 2) if before else None}

    @task.branch
    def decide_training(batch, categories, drift, prep):
        from lib.decision import decide_training as decide
        from lib.promote import champion_degradation, get_champion
        from lib.train import load_clean
        has_champion = get_champion() is not None
        degradation = None
        if has_champion and prep["status"] == "valido":
            degradation = champion_degradation(load_clean([batch["batch_id"]]))
        summary = {"status": prep["status"], "volume_increase_pct": prep.get("volume_increase_pct"),
                   "new_categories": categories, "drift": drift}
        train, reason, _ = decide(summary, has_champion, degradation)
        get_current_context()["ti"].xcom_push(key="decision", value={"trained": train, "train_reason": reason,
                                                                     "degradation": degradation})
        log.info("Decisión: %s", reason)
        return "train_candidate_model" if train else "skip_training"

    @task
    def skip_training():
        ti = get_current_context()["ti"]
        reason = ti.xcom_pull(task_ids="decide_training", key="decision")["train_reason"]
        log.info("No se entrena. %s", reason)
        return reason

    @task
    def train_candidate_model(batch):
        from lib.batch_processing import training_batches
        from lib.train import fit_and_save
        ids, labels = training_batches(batch["batch_id"])
        return {"batch_ids": ids, "batch_labels": labels,
                **fit_and_save(ids, _run_dir(get_current_context()))}

    @task
    def evaluate_candidate_model(trained):
        from lib.train import evaluate_saved
        return {**trained, **evaluate_saved(trained["batch_ids"], trained["model_path"],
                                            _run_dir(get_current_context()))}

    @task
    def register_candidate_in_mlflow(evaluated):
        from lib.train import register_saved
        ctx = get_current_context()
        reason = ctx["ti"].xcom_pull(task_ids="decide_training", key="decision")["train_reason"]
        reg = register_saved(evaluated["batch_ids"], evaluated["batch_labels"], reason, evaluated["model_path"],
                             evaluated, extra_tags={"airflow_run_id": ctx["run_id"]})
        return {**reg, "batch_ids": evaluated["batch_ids"], "metrics": evaluated["metrics"]}

    @task
    def compare_with_production(registered):
        from lib.promote import compare_with_production as compare
        return compare(registered["batch_ids"], registered["version"], registered["run_id"], registered["metrics"])

    @task.branch
    def decide_promotion(comparison):
        return "promote_model" if comparison["promoted"] else "reject_model"

    @task
    def promote_model(comparison):
        from lib.promote import apply_decision
        return apply_decision(comparison)

    @task
    def reject_model(comparison):
        from lib.promote import apply_decision
        return apply_decision(comparison)

    @task(trigger_rule=AFTER_BRANCH)
    def notify_or_log_result():
        from lib.audit import write_history
        ctx = get_current_context()
        ti = ctx["ti"]
        batch = ti.xcom_pull(task_ids="store_raw_batch") or {}
        prep = ti.xcom_pull(task_ids="preprocess_data") or {}
        decision = ti.xcom_pull(task_ids="decide_training", key="decision") or {}
        cmp = ti.xcom_pull(task_ids="compare_with_production") or {}
        if prep.get("status") == "invalido":
            final = "invalido"
        elif not decision.get("trained"):
            final = "no_entrena"
        else:
            final = "promovido" if cmp.get("promoted") else "rechazado"
        row = {"batch_id": batch.get("batch_id"), "group_number": batch.get("group_number"),
               "batch_number": batch.get("batch_number"), "airflow_run_id": ctx["run_id"],
               "n_rows_raw": prep.get("n_rows_raw"), "n_rows_clean": prep.get("n_rows_clean"),
               "validations": {"schema": ti.xcom_pull(task_ids="validate_schema"),
                               "quality": ti.xcom_pull(task_ids="validate_data_quality"),
                               "degradacion_campeon": decision.get("degradation")},
               "new_categories": ti.xcom_pull(task_ids="detect_new_categories"),
               "drift": ti.xcom_pull(task_ids="detect_data_drift"),
               "trained": bool(decision.get("trained")), "train_reason": decision.get("train_reason"),
               "mlflow_run_id": cmp.get("run_id"), "model_version": cmp.get("candidate_version"),
               "promoted": cmp.get("promoted") if cmp else None,
               "decision_reason": cmp.get("decision_reason") or decision.get("train_reason"),
               "candidate_mae": cmp.get("candidate_mae"), "candidate_rmse": cmp.get("candidate_rmse"),
               "champion_mae": cmp.get("champion_mae"), "champion_rmse": cmp.get("champion_rmse"),
               "delta_mae_pct": cmp.get("delta_mae_pct"), "final_status": final}
        write_history(row)
        shutil.rmtree(_run_dir(ctx), ignore_errors=True)
        log.info("Lote %s -> %s: %s", row["batch_number"], final, row["decision_reason"])
        return {"batch_number": row["batch_number"], "final_status": final}

    fetched = fetch_batch_from_api()
    batch = store_raw_batch(fetched)
    schema = validate_schema(batch)
    quality = validate_data_quality(batch)
    categories = detect_new_categories(batch, schema, quality)
    drift = detect_data_drift(batch, schema, quality)
    prep = preprocess_data(batch, schema, quality)
    decision = decide_training(batch, categories, drift, prep)

    skip = skip_training()
    trained = train_candidate_model(batch)
    comparison = compare_with_production(register_candidate_in_mlflow(evaluate_candidate_model(trained)))
    promotion = decide_promotion(comparison)
    promote, reject = promote_model(comparison), reject_model(comparison)
    notify = notify_or_log_result()

    start >> fetched
    schema >> quality >> categories >> drift >> prep
    decision >> [skip, trained]
    promotion >> [promote, reject]
    [skip, promote, reject] >> notify >> end


pipeline_lotes()
