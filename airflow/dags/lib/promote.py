"""Comparación candidato vs campeón y promoción/rechazo por alias en MLflow."""
import logging

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from lib.train import MODEL_NAME, load_clean, metrics, prepare_features, split

log = logging.getLogger(__name__)

ALIAS = "champion"
RULE = {"min_mae_improvement_pct": 3.0, "max_rmse_worsening_pct": 1.0}


def get_champion(client=None):
    client = client or MlflowClient()
    try:
        return client.get_model_version_by_alias(MODEL_NAME, ALIAS)
    except MlflowException:
        return None


def load_champion_model():
    return mlflow.sklearn.load_model(f"models:/{MODEL_NAME}@{ALIAS}")


def champion_degradation(batch_df):
    """MAE del campeón sobre el lote nuevo vs el MAE de test con el que se registró."""
    client = MlflowClient()
    champ = get_champion(client)
    if champ is None or batch_df.empty:
        return None
    pred = load_champion_model().predict(prepare_features(batch_df))
    mae_batch = metrics(batch_df["price"], pred)["mae"]
    mae_reg = client.get_run(champ.run_id).data.metrics["test_mae"]
    return {"champion_version": champ.version, "mae_lote": round(mae_batch, 2),
            "mae_registro": round(mae_reg, 2), "degradacion_pct": round((mae_batch / mae_reg - 1) * 100, 2)}


def compare_with_production(batch_ids, candidate_version, run_id, cand_metrics):
    """Tarea compare_with_production: evalúa al campeón sobre el MISMO test del candidato y aplica la regla."""
    client = MlflowClient()
    champ = get_champion(client)
    cand_mae, cand_rmse = cand_metrics["test_mae"], cand_metrics["test_rmse"]
    result = {"candidate_version": candidate_version, "run_id": run_id, "candidate_mae": cand_mae,
              "candidate_rmse": cand_rmse, "champion_version": None, "champion_mae": None,
              "champion_rmse": None, "delta_mae_pct": None, "delta_rmse_pct": None}
    if champ is None:
        result.update(promoted=True, decision_reason="se promueve: no existía modelo productivo (línea base inicial)")
        return result

    test = split(load_clean(batch_ids))[2]
    champ_m = metrics(test["price"], load_champion_model().predict(prepare_features(test)))
    d_mae = (cand_mae - champ_m["mae"]) / champ_m["mae"] * 100
    d_rmse = (cand_rmse - champ_m["rmse"]) / champ_m["rmse"] * 100
    promote = d_mae <= -RULE["min_mae_improvement_pct"] and d_rmse <= RULE["max_rmse_worsening_pct"]
    verb = "se promueve" if promote else "se rechaza"
    reason = (f"{verb}: MAE {d_mae:+.2f} % (exige <= -{RULE['min_mae_improvement_pct']} %), "
              f"RMSE {d_rmse:+.2f} % (exige <= +{RULE['max_rmse_worsening_pct']} %) vs campeón v{champ.version}")
    client.log_metric(run_id, "champion_test_mae", champ_m["mae"])
    client.log_metric(run_id, "champion_test_rmse", champ_m["rmse"])
    result.update(champion_version=champ.version, champion_mae=champ_m["mae"], champion_rmse=champ_m["rmse"],
                  delta_mae_pct=round(d_mae, 2), delta_rmse_pct=round(d_rmse, 2),
                  promoted=promote, decision_reason=reason)
    return result


def apply_decision(result):
    """Tareas promote_model / reject_model: deja tags y, si corresponde, mueve el alias champion."""
    client = MlflowClient()
    version = result["candidate_version"]
    client.set_model_version_tag(MODEL_NAME, version, "decision", "promoted" if result["promoted"] else "rejected")
    client.set_model_version_tag(MODEL_NAME, version, "decision_reason", result["decision_reason"])
    if result["promoted"]:
        client.set_registered_model_alias(MODEL_NAME, ALIAS, version)
    log.info("Candidato v%s: %s", version, result["decision_reason"])
    return result


def compare_and_promote(candidate):
    """Comparación + decisión seguidas (lo usa scripts/simulate_pipeline.py)."""
    result = compare_with_production(candidate["batch_ids"], candidate["version"],
                                     candidate["run_id"], candidate["metrics"])
    return apply_decision(result)
