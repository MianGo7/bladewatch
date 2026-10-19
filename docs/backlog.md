# Backlog

The work queue. Items are taken one at a time and in order unless a
dependency says otherwise. Each item names its scope, the condition under
which it counts as done, and the documents that have to be updated in the same
change.

Status values: open, in progress, done.

---

## B0: Repository and project set up

Status: done.

Scope: create the Python project with uv and locked dependencies, with pytest
and Ruff as the only dependencies at this stage, configure the static checks,
the `integration` test marker, and a continuous integration workflow that runs
the format check, the lint, and the unit tests. Create `README.md` with the
purpose of the project, the course, the task, and the author, and the
`.github/workflows/` file. Record the selection of Task 2 and the reasons in
the journal.

Done when: the dependencies install from the lock file, the checks and an
initial test pass, and the workflow file is valid.

Documents to update: `dev-journal.md`, `decision-log.md`.

---

## B1: Requirements, architecture, and tool selection

Status: done.

Scope: close the open points O1 to O4 in `concept.md`. Revise the
requirements against the case. Fix the assumptions A1 to A9 with their values
and their basis. Choose a tool for every layer of the architecture, with the
rejected alternatives, including every example tool named in the task.
Decide the architectural style and the delivery semantics. Draw the
architecture overview and the sequence of a reading from the sensor to the
alert. No code.

Done when: `concept.md` lists no open points, every layer has a tool and an
ADR, every assumption has a value and a basis, both diagrams render without
errors, and every requirement in `requirements.md` is testable.

Documents to update: `concept.md`, `requirements.md`, `decision-log.md`,
`diagrams/README.md`, `dev-journal.md`.

---

## B2: Infrastructure with Docker Compose

Status: done.

Scope: `compose.yaml` with Apache Kafka in KRaft mode, TimescaleDB, and
Grafana, each pinned to an exact image version and with a health check. The
topics `machine-temperature` and `machine-alerts` are created with their
partition count and retention. The database schema in `infra/timescaledb/`:
a hypertable for readings, a table for alerts, a continuous aggregate per
machine and hour that serves as the view for the DWH, and a retention and
compression policy. Grafana is provisioned with the TimescaleDB data source.
A smoke test checks that every service is reachable.

Done when: `docker compose up -d` brings every container to healthy from a
clean state, the topics and the schema exist, and an integration test
confirms it.

Documents to update: `requirements.md`, `decision-log.md`, `dev-journal.md`,
`README.md`.

---

## B3: Sensor simulator and producer

Status: open.

Scope: the configuration of the machines as frozen dataclasses, the
temperature model of a machine with normal operation, noise, and an
injectable overheating profile, the message format of a reading with a schema
version, and the producer that publishes readings keyed by machine to
`machine-temperature`. The model takes an explicit random number generator.
Dependency for this item: the Kafka client library.

Done when: the temperature model is covered by unit tests with a fixed seed,
including the mean and spread of normal operation against their analytic
values and the course of an injected overheating, two runs with the same seed
produce identical readings, and an integration test finds the published
readings in the topic.

Documents to update: `requirements.md`, `concept.md` (A1 to A4 if refined),
`decision-log.md`, `dev-journal.md`.

---

## B4: Alert rules

Status: open.

Scope: the alert rules as pure functions and data: warning and critical
thresholds per machine type, hysteresis so that an alert does not flap around
a threshold, the state of an alert per machine (normal, warning, critical,
recovered), and a windowed check of the rate of temperature rise that warns
before the threshold is reached. Draw the state diagram of an alert.

Done when: every transition of the alert state is covered by a unit test,
the hysteresis is tested at the exact threshold values, the windowed check is
tested with a deterministic series, and the state diagram renders and matches
the code.

Documents to update: `requirements.md`, `diagrams/README.md`,
`decision-log.md`, `dev-journal.md`.

---

## B5: Stream processor and storage

Status: open.

Scope: the processor that consumes `machine-temperature` in a consumer group,
applies the rules, writes readings in batches to TimescaleDB, writes alerts
to the alert table, and publishes them to `machine-alerts`. Offsets are
committed only after the database write, and writes are idempotent, so that a
restart neither loses nor duplicates readings. Timestamps of every stage are
stored for the latency measurement. Dependency for this item: the PostgreSQL
driver.

Done when: an integration test sends a known set of readings and finds every
one of them exactly once in the database, an overheating series produces the
expected alerts in the table and in the topic, and a restart of the processor
in the middle of a run loses and duplicates nothing.

Documents to update: `requirements.md`, `decision-log.md`, `dev-journal.md`.

---

## B6: Dashboard and alerting

Status: open.

Scope: the Grafana dashboard as provisioned JSON: the current temperature of
every machine with its thresholds, a time series per machine type, the table
of open and recent alerts, and the history of a selected machine from the
continuous aggregate. A Grafana alert rule with a contact point that makes a
critical alert visible outside the dashboard. Screenshots of the dashboard in
normal operation.

Done when: the dashboard loads from the provisioned file on a clean start,
refreshes within the interval stated in the concept, shows a critical alert
from the processor, and the screenshots are in `docs/figures/`.

Documents to update: `requirements.md`, `decision-log.md`, `dev-journal.md`.

---

## B7: Demonstration scenario for the steering committee

Status: open.

Scope: the scripted and seeded scenario from `concept.md`: normal operation,
one machine overheating, warning, critical alert, and the history afterwards.
A single command starts it. Measurement M4. Screenshots of the dashboard at
the moment of the critical alert and of the history.

Done when: the scenario runs identically on repeated starts, M4 is measured
and recorded, and the screenshots are in `docs/figures/`.

Documents to update: `concept.md`, `requirements.md`, `dev-journal.md`.

---

## B8: Measurements

Status: open.

Scope: the measurements M1 to M3 as code with their load profiles, the
recording of the hardware and the Docker resource limits, and the figures and
tables in `docs/figures/`. Dependency for this item: Matplotlib, and pandas if
the evaluation needs it.

Done when: M1 to M3 have run with the recorded settings, the figures and
tables are committed together with the measurement definitions, and the
journal records the headline values with the commit and the hardware.

Documents to update: `requirements.md`, `decision-log.md`, `dev-journal.md`.

---

## B9: Structure diagrams, limits, and finish

Status: open.

Scope: the component diagram of the package and the deployment diagram of
the containers, derived from the code and `compose.yaml`, with a test that
compares the component diagram with the imports. The final status of every
requirement, the limits of the prototype, and the README with the steps to
run the demonstration.

Done when: both diagrams render and match the code, every requirement has a
final status, the limits are written down, and the README lets a reader run
the scenario from a clean clone.

Documents to update: `requirements.md`, `diagrams/README.md`,
`dev-journal.md`, `README.md`.
