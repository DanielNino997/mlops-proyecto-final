"""Diagnóstico B3: compara la distribución de un lote contra la referencia, por tramos de PSI."""
import sys

import numpy as np

sys.path.insert(0, "airflow/dags")
from lib.db import connect  # noqa: E402

conn = connect("CLEAN_DB_URL")


def col(batch_ids, c):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {c} FROM properties_clean WHERE batch_id = ANY(%s) AND {c} IS NOT NULL", (batch_ids,))
        return np.array([r[0] for r in cur.fetchall()], dtype=float)


for nombre, ref_ids, cur_id, c in [("lote 1", [1], 2, "house_size"),
                                   ("lote 4", [1, 2, 3, 8], 9, "price"),
                                   ("lote 3", [1, 2, 3], 8, "price")]:
    ref, cur = col(ref_ids, c), col([cur_id], c)
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, 11)))
    print(f"\n== {nombre} / {c}: n_ref={len(ref)} n_lote={len(cur)}")
    print("percentiles ref :", np.round(np.quantile(ref, [0, .1, .5, .9, 1]), 1))
    print("percentiles lote:", np.round(np.quantile(cur, [0, .1, .5, .9, 1]), 1))
    print("cortes          :", np.round(edges, 1))
    e = edges.copy()
    e[0], e[-1] = -np.inf, np.inf
    print("conteo ref por tramo :", np.histogram(ref, e)[0])
    print("conteo lote por tramo:", np.histogram(cur, e)[0])
conn.close()
