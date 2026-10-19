-- Compression and retention of the raw readings (ADR-0014, assumption A12).
ALTER TABLE readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'machine_id',
    timescaledb.compress_orderby = 'measured_at DESC'
);

SELECT add_compression_policy('readings', INTERVAL '7 days');
SELECT add_retention_policy('readings', INTERVAL '90 days');
