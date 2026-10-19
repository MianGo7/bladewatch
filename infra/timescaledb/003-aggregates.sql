-- Hourly minimum, mean, and maximum per machine (FR8). The continuous
-- aggregate is also the view that the existing data warehouse loads (FR10,
-- ADR-0010).
CREATE MATERIALIZED VIEW readings_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    time_bucket(INTERVAL '1 hour', measured_at) AS bucket,
    machine_id,
    min(temperature_c) AS min_temperature_c,
    avg(temperature_c) AS mean_temperature_c,
    max(temperature_c) AS max_temperature_c,
    count(*)           AS reading_count
FROM readings
GROUP BY bucket, machine_id
WITH NO DATA;

-- ADR-0014: the refresh window starts at three days, which is far inside the
-- retention of the raw readings. A window that reached beyond the retention
-- would recompute buckets from chunks that have already been dropped.
SELECT add_continuous_aggregate_policy(
    'readings_hourly',
    start_offset      => INTERVAL '3 days',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '15 minutes'
);
