-- Raw readings (FR7, NFR8). One row per reading of one machine.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE readings (
    machine_id          text             NOT NULL,
    sequence            bigint           NOT NULL,
    measured_at         timestamptz      NOT NULL,
    temperature_c       double precision NOT NULL,
    -- The four stage timestamps make the latency of every stage computable
    -- from stored rows. measured_at is set by the sensor, kafka_appended_at
    -- by the broker, processed_at by the processor, and stored_at by the
    -- database (ADR-0011).
    kafka_appended_at   timestamptz      NOT NULL,
    processed_at        timestamptz      NOT NULL,
    stored_at           timestamptz      NOT NULL DEFAULT now(),
    -- ADR-0012: a redelivered reading hits this key and is ignored, which
    -- makes the storage idempotent. A unique index on a hypertable has to
    -- contain the partitioning column, which measured_at does.
    PRIMARY KEY (machine_id, measured_at)
);

-- ADR-0014: the chunk interval of one day keeps a chunk small at the load of
-- the case and bounded at the plant-size reference of A9.
SELECT create_hypertable('readings', by_range('measured_at', INTERVAL '1 day'));
