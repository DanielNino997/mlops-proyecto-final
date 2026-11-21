"""Prueba local de B4: aplica decisión, entrenamiento y promoción a los 6 lotes, en orden de llegada."""
import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
from mlflow import MlflowClient  # noqa: E402

from lib.db import connect  # noqa: E402
from lib.decision import decide_training  # noqa: E402
from lib.promote import champion_degradation, compare_and_promote, get_champion  # noqa: E402
from lib.train import MODEL_NAME, load_clean, train_candidate  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logging.getLogger("mlflow").setLevel(logging.WARNING)

parser = argparse.ArgumentParser()
parser.add_argument("--reports", required=True, help="carpeta con reporte-lote-N.json de B3")
parser.add_argument("--out", required=True, help="archivo JSON de historial")
parser.add_argument("--reset", action="store_true", help="borrar el modelo registrado antes")
args = parser.parse_args()

if args.reset:
    try:
        MlflowClient().delete_registered_model(MODEL_NAME)
        print(f"modelo registrado '{MODEL_NAME}' borrado")
    except Exception:  # noqa: BLE001
        pass

conn = connect("RAW_DB_URL")
with conn.cursor() as cur:
    cur.execute("SELECT id, batch_number FROM batches WHERE group_number = 2 ORDER BY batch_number")
    batches = cur.fetchall()
conn.close()

history = []
for i, (batch_id, batch_number) in enumerate(batches):
    summary = json.loads((Path(args.reports) / f"reporte-lote-{batch_number}.json").read_text())
    has_champion = get_champion() is not None
    degradation = champion_degradation(load_clean([batch_id])) if has_champion else None
    train, reason, _ = decide_training(summary, has_champion, degradation)
    row = {"batch_number": batch_number, "trained": train, "train_reason": reason, "degradation": degradation}
    if train:
        ids = [b for b, _ in batches[: i + 1]]
        labels = "-".join(str(n) for _, n in batches[: i + 1])
        cand = train_candidate(ids, labels, reason)
        row.update({k: v for k, v in compare_and_promote(cand).items()})
        row["mlflow_run_id"] = cand["run_id"]
    history.append(row)
    print(f"\n### batch {batch_number}: {reason}")
    if train:
        print(f"    v{row['candidate_version']} MAE={row['candidate_mae']:,.0f} -> {row['decision_reason']}")

Path(args.out).write_text(json.dumps(history, indent=2, default=str))
print(f"\ncampeón final: v{get_champion().version}")
