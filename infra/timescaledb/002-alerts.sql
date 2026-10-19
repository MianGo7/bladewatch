-- Alerts (FR7). The table is an append-only log of alert events: every raise,
-- escalation, and closing of an alert is one row, so that the course of an
-- incident can be reconstructed from the history (ADR-0014).
CREATE TABLE alerts (
    alert_id       text             NOT NULL,
    machine_id     text             NOT NULL,
    kind           text             NOT NULL CHECK (kind IN ('threshold', 'rate_of_rise')),
    severity       text             NOT NULL CHECK (severity IN ('warning', 'critical')),
    status         text             NOT NULL CHECK (status IN ('raised', 'closed')),
    temperature_c  double precision NOT NULL,
    -- Threshold in degrees Celsius for kind threshold, rate limit in degrees
    -- Celsius per minute for kind rate_of_rise.
    "limit"        double precision NOT NULL,
    measured_at    timestamptz      NOT NULL,
    processed_at   timestamptz      NOT NULL,
    stored_at      timestamptz      NOT NULL DEFAULT now(),
    -- ADR-0012: the same event delivered twice is stored once.
    PRIMARY KEY (alert_id, severity, status)
);

CREATE INDEX alerts_machine_measured_at_idx ON alerts (machine_id, measured_at DESC);
