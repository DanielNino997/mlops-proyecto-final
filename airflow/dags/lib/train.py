"""Entrenamiento del modelo de precio y registro del candidato en MLflow."""
import logging
import os
import subprocess
import tempfile
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from mlflow.models import infer_signature  # noqa: E402
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor  # noqa: E402
from sklearn.ensemble import HistGradientBoostingRegressor  # noqa: E402
from sklearn.inspection import permutation_importance  # noqa: E402
from sklearn.metrics import (mean_absolute_error, mean_absolute_percentage_error,  # noqa: E402
                             mean_squared_error, r2_score)
from sklearn.pipeline import Pipeline  # noqa: E402
from sklearn.preprocessing import OneHotEncoder, TargetEncoder  # noqa: E402

from lib.db import connect  # noqa: E402

log = logging.getLogger(__name__)

MODEL_NAME = os.environ.get("MODEL_NAME", "house-price-model")
EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT", "house-price")
NUM_COLS = ["bed", "bath", "acre_lot", "house_size", "years_since_prev_sale"]
TE_COLS = ["city", "state", "zip_code", "brokered_by"]     # alta cardinalidad -> target encoding
OH_COLS = ["status"]                                      # pocas categorías -> one-hot
FEATURES = NUM_COLS + TE_COLS + OH_COLS
MODEL_PARAMS = {"max_iter": 300, "learning_rate": 0.1, "max_leaf_nodes": 63,
                "min_samples_leaf": 40, "early_stopping": True, "random_state": 42}
TEST_PCT, VAL_PCT = 15, 15      # división determinista por raw_row_id % 100


def load_clean(batch_ids):
    conn = connect("CLEAN_DB_URL")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT raw_row_id, batch_id, price, {', '.join(FEATURES)} "
                        "FROM properties_clean WHERE batch_id = ANY(%s)", (list(batch_ids),))
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


def prepare_features(df):
    """Mismo formato de entrada para entrenamiento, evaluación y la API de inferencia."""
    X = pd.DataFrame(index=df.index)
    for c in NUM_COLS:
        X[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in TE_COLS + OH_COLS:
        X[c] = df[c].astype("object").where(df[c].notna(), "desconocido").astype(str)
    return X


def split(df):
    bucket = df["raw_row_id"] % 100
    test = df[bucket < TEST_PCT]
    val = df[(bucket >= TEST_PCT) & (bucket < TEST_PCT + VAL_PCT)]
    train = df[bucket >= TEST_PCT + VAL_PCT]
    return train, val, test


def build_model():
    pre = ColumnTransformer([
        ("num", "passthrough", NUM_COLS),
        ("te", TargetEncoder(target_type="continuous", random_state=42), TE_COLS),
        ("oh", OneHotEncoder(handle_unknown="ignore", sparse_output=False), OH_COLS),
    ])
    pipe = Pipeline([("pre", pre), ("model", HistGradientBoostingRegressor(**MODEL_PARAMS))])
    return TransformedTargetRegressor(regressor=pipe, func=np.log1p, inverse_func=np.expm1)


def metrics(y_true, y_pred):
    return {"mae": float(mean_absolute_error(y_true, y_pred)),
            "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "r2": float(r2_score(y_true, y_pred)),
            "mape": float(mean_absolute_percentage_error(y_true, y_pred))}


def _git_sha():
    if os.environ.get("GIT_SHA"):
        return os.environ["GIT_SHA"]
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:  # noqa: BLE001
        return "desconocido"


def _save_plots(model, X_test, y_test, out_dir):
    sample = X_test.sample(min(5000, len(X_test)), random_state=0)
    y_s = y_test.loc[sample.index]
    pred = model.predict(sample)

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(y_s, pred, s=3, alpha=0.3)
    lims = [y_s.min(), y_s.max()]
    ax.plot(lims, lims, "r--", lw=1)
    ax.set(xscale="log", yscale="log", xlabel="precio real", ylabel="precio predicho", title="Real vs predicho (test)")
    fig.savefig(Path(out_dir) / "real_vs_predicho.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(np.log(pred) - np.log(y_s), bins=80)
    ax.set(xlabel="log(predicho) - log(real)", title="Residuales en escala log (test)")
    fig.savefig(Path(out_dir) / "residuales.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    small = sample.head(2000)
    imp = permutation_importance(model, small, y_test.loc[small.index], n_repeats=3, random_state=0,
                                 scoring="neg_mean_absolute_error")
    order = np.argsort(imp.importances_mean)
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.barh(np.array(FEATURES)[order], imp.importances_mean[order])
    ax.set(xlabel="aumento del MAE al permutar (USD)", title="Importancia por permutación")
    fig.savefig(Path(out_dir) / "importancia_permutacion.png", dpi=120, bbox_inches="tight")
    plt.close(fig)


def fit_and_save(batch_ids, out_dir):
    """Tarea train_candidate_model: entrena con los lotes y guarda el modelo en disco."""
    import joblib
    train = split(load_clean(batch_ids))[0]
    model = build_model()
    model.fit(prepare_features(train), train["price"])
    path = Path(out_dir) / "model.joblib"
    joblib.dump(model, path)
    return {"model_path": str(path), "n_train": len(train)}


def evaluate_saved(batch_ids, model_path, out_dir):
    """Tarea evaluate_candidate_model: métricas train/val/test y gráficos."""
    import joblib
    model = joblib.load(model_path)
    train, val, test = split(load_clean(batch_ids))
    mets = {}
    for name, part in (("train", train), ("val", val), ("test", test)):
        for k, v in metrics(part["price"], model.predict(prepare_features(part))).items():
            mets[f"{name}_{k}"] = v
    plots = Path(out_dir) / "graficos"
    plots.mkdir(parents=True, exist_ok=True)
    _save_plots(model, prepare_features(test), test["price"], plots)
    return {"metrics": mets, "plots_dir": str(plots), "n_train": len(train), "n_val": len(val), "n_test": len(test)}


def register_saved(batch_ids, batch_labels, train_reason, model_path, evaluation, extra_tags=None):
    """Tarea register_candidate_in_mlflow: run con parámetros, métricas, gráficos y modelo registrado."""
    import joblib
    model = joblib.load(model_path)
    example = prepare_features(split(load_clean(batch_ids))[2].head(5))
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f"lotes-{batch_labels}") as run:
        mlflow.set_tags({"batch_ids": ",".join(map(str, batch_ids)), "batch_numbers": batch_labels,
                         "git_sha": _git_sha(), "train_reason": train_reason, **(extra_tags or {})})
        mlflow.log_params({**{f"model_{k}": v for k, v in MODEL_PARAMS.items()},
                           "target_transform": "log1p", "num_cols": ",".join(NUM_COLS),
                           "target_encoded_cols": ",".join(TE_COLS), "onehot_cols": ",".join(OH_COLS),
                           "split": f"raw_row_id%100: test<{TEST_PCT}, val<{TEST_PCT + VAL_PCT}",
                           "n_train": evaluation["n_train"], "n_val": evaluation["n_val"],
                           "n_test": evaluation["n_test"]})
        mlflow.log_metrics(evaluation["metrics"])
        mlflow.log_artifacts(evaluation["plots_dir"], "graficos")
        mlflow.sklearn.log_model(model, "model", signature=infer_signature(example, model.predict(example)),
                                 input_example=example, registered_model_name=MODEL_NAME)
        run_id = run.info.run_id
    version = MlflowClient().search_model_versions(f"run_id='{run_id}'")[0].version
    log.info("Candidato v%s registrado (run %s): test_mae=%.0f", version, run_id, evaluation["metrics"]["test_mae"])
    return {"run_id": run_id, "version": version}


def train_candidate(batch_ids, batch_labels, train_reason, extra_tags=None):
    """Las tres etapas seguidas (lo usa scripts/simulate_pipeline.py)."""
    with tempfile.TemporaryDirectory() as tmp:
        fit = fit_and_save(batch_ids, tmp)
        evaluation = evaluate_saved(batch_ids, fit["model_path"], tmp)
        reg = register_saved(batch_ids, batch_labels, train_reason, fit["model_path"], evaluation, extra_tags)
    return {**reg, "metrics": evaluation["metrics"], "batch_ids": list(batch_ids)}
