"""Comparación candidato vs campeón y promoción por alias en MLflow."""
import logging

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from lib.train import MODEL_NAME, metrics, prepare_features

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


def compare_and_promote(candidate):
    """Aplica la regla: promover si el MAE baja >= 3 % y el RMSE no empeora más de 1 %."""
    client = MlflowClient()
    champ = get_champion(client)
    cand_mae, cand_rmse = candidate["metrics"]["test_mae"], candidate["metrics"]["test_rmse"]
    result = {"candidate_version": candidate["version"], "candidate_mae": cand_mae, "candidate_rmse": cand_rmse,
              "champion_version": None, "champion_mae": None, "champion_rmse": None,
              "delta_mae_pct": None, "delta_rmse_pct": None}

    if champ is None:
        promote, reason = True, "se promueve: no existía modelo productivo (línea base inicial)"
    else:
        test = candidate["test"]
        champ_m = metrics(test["price"], load_champion_model().predict(prepare_features(test)))
        d_mae = (cand_mae - champ_m["mae"]) / champ_m["mae"] * 100
        d_rmse = (cand_rmse - champ_m["rmse"]) / champ_m["rmse"] * 100
        result.update(champion_version=champ.version, champion_mae=champ_m["mae"], champion_rmse=champ_m["rmse"],
                      delta_mae_pct=round(d_mae, 2), delta_rmse_pct=round(d_rmse, 2))
        promote = d_mae <= -RULE["min_mae_improvement_pct"] and d_rmse <= RULE["max_rmse_worsening_pct"]
        verb = "se promueve" if promote else "se rechaza"
        reason = (f"{verb}: MAE {d_mae:+.2f} % (exige <= -{RULE['min_mae_improvement_pct']} %), "
                  f"RMSE {d_rmse:+.2f} % (exige <= +{RULE['max_rmse_worsening_pct']} %) vs campeón v{champ.version}")
        client.log_metric(candidate["run_id"], "champion_test_mae", champ_m["mae"])
        client.log_metric(candidate["run_id"], "champion_test_rmse", champ_m["rmse"])

    version = candidate["version"]
    client.set_model_version_tag(MODEL_NAME, version, "decision", "promoted" if promote else "rejected")
    client.set_model_version_tag(MODEL_NAME, version, "decision_reason", reason)
    if promote:
        client.set_registered_model_alias(MODEL_NAME, ALIAS, version)
    log.info("Candidato v%s: %s", version, reason)
    return {**result, "promoted": promote, "decision_reason": reason}
