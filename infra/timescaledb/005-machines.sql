-- The monitored machines with their thresholds (FR9). The processor writes this
-- table from its configuration each time it starts, so that the thresholds have
-- one source and the dashboard never holds a second copy (ADR-0021).
CREATE TABLE machines (
    machine_id    text             PRIMARY KEY,
    machine_type  text             NOT NULL,
    normal_c      double precision NOT NULL,
    warning_c     double precision NOT NULL,
    critical_c    double precision NOT NULL,
    CHECK (normal_c < warning_c AND warning_c < critical_c)
);
