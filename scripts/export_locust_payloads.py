"""Genera services/locust/payloads.json con 500 propiedades reales de CLEAN en el formato de /predict."""
import json
import os
import sys

import psycopg2

conn = psycopg2.connect(os.environ["CLEAN_DB_URL"])
with conn.cursor() as cur:
    cur.execute("""SELECT bed, bath, acre_lot, house_size, city, state, zip_code, status, brokered_by, prev_sold_date
                   FROM properties_clean
                   WHERE bed IS NOT NULL AND bath IS NOT NULL AND acre_lot IS NOT NULL AND house_size IS NOT NULL
                     AND city IS NOT NULL AND state IS NOT NULL AND zip_code ~ '^[0-9]{3,5}$'
                     AND bed <= 100 AND bath <= 100
                   ORDER BY random() LIMIT 500""")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
conn.close()

for r in rows:
    r["prev_sold_date"] = r["prev_sold_date"].isoformat() if r["prev_sold_date"] else None
out = sys.argv[1] if len(sys.argv) > 1 else "services/locust/payloads.json"
with open(out, "w") as f:
    json.dump(rows, f)
print(f"{len(rows)} payloads -> {out}")
