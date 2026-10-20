-- Views that the dashboard reads (FR9, ADR-0021). The dashboard queries views
-- and not the tables, so that the logic of what counts as open or current lives
-- in the schema and is tested there.

-- An alert is open when it has no closing event. Its latest event carries the
-- severity that the alert has reached.
CREATE VIEW open_alerts AS
SELECT DISTINCT ON (a.alert_id)
    a.alert_id,
    a.machine_id,
    a.kind,
    a.severity,
    a.temperature_c,
    a."limit",
    a.measured_at
FROM alerts AS a
WHERE NOT EXISTS (
    SELECT 1 FROM alerts AS c WHERE c.alert_id = a.alert_id AND c.status = 'closed'
)
ORDER BY a.alert_id, a.measured_at DESC;

-- The latest reading of every machine with its thresholds and the severity of
-- its open alert, if any. The lateral subquery reads one row per machine from the
-- index of the primary key and does not scan the history.
CREATE VIEW machine_status AS
SELECT
    m.machine_id,
    m.machine_type,
    m.normal_c,
    m.warning_c,
    m.critical_c,
    r.temperature_c,
    r.measured_at,
    (
        SELECT o.severity FROM open_alerts AS o
        WHERE o.machine_id = m.machine_id
        ORDER BY o.measured_at DESC LIMIT 1
    ) AS open_alert_severity
FROM machines AS m
LEFT JOIN LATERAL (
    SELECT temperature_c, measured_at
    FROM readings
    WHERE machine_id = m.machine_id
    ORDER BY measured_at DESC
    LIMIT 1
) AS r ON TRUE;
