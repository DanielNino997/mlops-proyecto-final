"""Prueba local de B3: procesa los lotes guardados en RAW (en orden) hacia CLEAN y guarda sus reportes."""
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
from lib.batch_processing import process_batch  # noqa: E402
from lib.db import connect, ensure_schema  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/reportes")
out_dir.mkdir(parents=True, exist_ok=True)

ensure_schema()
conn = connect("RAW_DB_URL")
with conn.cursor() as cur:
    cur.execute("SELECT id FROM batches ORDER BY group_number, batch_number")
    batch_ids = [r[0] for r in cur.fetchall()]
conn.close()

for batch_id in batch_ids:
    s = process_batch(batch_id)
    (out_dir / f"reporte-lote-{s['batch_number']}.json").write_text(json.dumps(s, indent=2, default=str))
    if s["status"] != "valido":
        print(f"batch {s['batch_number']}: INVALIDO -> {s['schema']} {s['quality']['target_invalido_pct']}%")
        continue
    psis = {c: v["psi"] for c, v in s["drift"].get("columnas", {}).items() if v["psi"] is not None}
    top = max(psis, key=psis.get) if psis else None
    print(f"batch {s['batch_number']} | raw {s['n_rows_raw']:>7} -> clean {s['n_rows_clean']:>7} | "
          f"+vol {s['volume_increase_pct']}% | target inválido {s['quality']['target_invalido_pct']}% | "
          f"estados nuevos {s['new_categories']['state']['n_nuevas']} "
          f"({s['new_categories']['state']['filas_con_nuevas_pct']}% filas) | "
          f"ciudades nuevas {s['new_categories']['city']['filas_con_nuevas_pct']}% filas | "
          f"PSI máx {top}={psis.get(top)}")
