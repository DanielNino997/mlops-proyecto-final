-- Base RAW_DATA: lo que llega de la API y de la API de inferencia, sin transformar.
CREATE TABLE IF NOT EXISTS batches (
    id              SERIAL PRIMARY KEY,
    group_number    INT         NOT NULL,
    batch_number    INT         NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    n_rows          INT         NOT NULL,
    payload_hash    CHAR(64)    NOT NULL,
    columns         JSONB       NOT NULL,
    airflow_run_id  TEXT,
    UNIQUE (group_number, batch_number)
);

CREATE TABLE IF NOT EXISTS properties_raw (
    raw_row_id  BIGSERIAL PRIMARY KEY,
    batch_id    INT   NOT NULL REFERENCES batches(id),
    row_number  INT   NOT NULL,
    record      JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_properties_raw_batch ON properties_raw (batch_id);

CREATE TABLE IF NOT EXISTS inference_events (
    id             BIGSERIAL PRIMARY KEY,
    ts             TIMESTAMPTZ NOT NULL DEFAULT now(),
    input          JSONB,
    prediction     DOUBLE PRECISION,
    model_name     TEXT,
    model_version  TEXT,
    status         TEXT NOT NULL,
    error          TEXT,
    latency_ms     DOUBLE PRECISION
);
