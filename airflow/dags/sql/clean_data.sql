-- Base CLEAN_DATA: datos tipados y listos para entrenar + auditoría del pipeline.
CREATE TABLE IF NOT EXISTS properties_clean (
    raw_row_id             BIGINT PRIMARY KEY,   -- trazabilidad con raw_data.properties_raw
    batch_id               INT    NOT NULL,      -- trazabilidad con raw_data.batches
    brokered_by            TEXT,
    status                 TEXT,
    price                  DOUBLE PRECISION NOT NULL,
    bed                    DOUBLE PRECISION,
    bath                   DOUBLE PRECISION,
    acre_lot               DOUBLE PRECISION,
    city                   TEXT,
    state                  TEXT,
    zip_code               TEXT,
    house_size             DOUBLE PRECISION,
    prev_sold_date         DATE,
    years_since_prev_sale  DOUBLE PRECISION,
    loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_properties_clean_batch ON properties_clean (batch_id);

CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.batch_history (
    id                  SERIAL PRIMARY KEY,
    batch_id            INT,
    group_number        INT,
    batch_number        INT,
    airflow_run_id      TEXT,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    n_rows_raw          INT,
    n_rows_clean        INT,
    validations         JSONB,
    new_categories      JSONB,
    drift               JSONB,
    trained             BOOLEAN,
    train_reason        TEXT,
    mlflow_run_id       TEXT,
    model_version       TEXT,
    promoted            BOOLEAN,
    decision_reason     TEXT,
    candidate_mae       DOUBLE PRECISION,
    candidate_rmse      DOUBLE PRECISION,
    champion_mae        DOUBLE PRECISION,
    champion_rmse       DOUBLE PRECISION,
    delta_mae_pct       DOUBLE PRECISION,
    final_status        TEXT,
    alertas             JSONB                 -- drift / categorías nuevas / degradación, se entrene o no
);
-- Para bases creadas antes de B8:
ALTER TABLE audit.batch_history ADD COLUMN IF NOT EXISTS alertas JSONB;
