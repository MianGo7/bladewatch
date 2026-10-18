# Requirements

This document lists the requirements of the prototype and traces each one to
the code and the tests that satisfy it. It is the working basis for the
description of the solution in the case study and for the evaluation of the
result. A requirement is only marked as done when it is implemented and
covered by a test, a measurement, or a generated figure.

The requirement set was derived from the case and revised in B1 against the
assumptions in `concept.md`. Every requirement names the backlog item that
implements it and the check that decides whether it is met, so that each one
is testable. Every later change is recorded in `dev-journal.md`.

Status values: open, in progress, done, partly met, deferred. A deferred
requirement is specified on purpose but not implemented, and the reason is
stated next to it.

## Functional requirements

| ID | Requirement | Status | Item | Check | Implemented in | Verified by |
| --- | --- | --- | --- | --- | --- | --- |
| FR1 | The prototype captures the temperature of each of the twelve machines of A1 once per second (A3) and publishes each reading with the fields of ADR-0011. | open | B3 | An integration test finds the readings of every machine in `machine-temperature`, with the expected fields and consecutive `sequence` values. | | |
| FR2 | Readings are buffered in Kafka, keyed by machine, and retained for a configured period, so that a consumer can read them again from the start of the retention. | open | B2, B3 | An integration test reads a topic from the earliest offset with a new consumer group and receives the readings that were published before. | | |
| FR3 | Every reading is checked against the warning and the critical threshold of the type of its machine (A4), and an alert is raised when a threshold is reached. | open | B4 | Unit tests with readings below, at, and above each threshold of each machine type. | | |
| FR4 | An alert does not flap. It is raised once when a threshold is reached and closed only when the temperature has fallen below the threshold by a configured margin. | open | B4 | Unit tests at the exact threshold values and with a series that oscillates around a threshold and raises exactly one alert. | | |
| FR5 | A rise of the temperature over a configured window above a configured limit raises a warning before the warning threshold is reached. | open | B4 | A unit test with a deterministic series that rises faster than the limit while it stays below the warning threshold. | | |
| FR6 | An alert is visible to an employee on the dashboard and through a notification outside the dashboard, which is the contact point of A6. | open | B6 | The alert table of the dashboard shows a critical alert from the processor, and the contact point delivers a message to the local web hook. | | |
| FR7 | Every reading is stored with its four timestamps, `measured_at`, the Kafka log append time, `processed_at`, and `stored_at`, and every alert is stored with `measured_at`, `processed_at`, and `stored_at`. | open | B5 | An integration test finds every reading and every alert of a known run in the database, each with the timestamps named here. | | |
| FR8 | The history of a machine can be analysed as hourly minimum, mean, and maximum. | open | B2 | An integration test compares the continuous aggregate with the same aggregation computed from the raw readings of a known series. | | |
| FR9 | A dashboard shows the current temperature of every machine, its thresholds, and the open alerts, and refreshes within the interval stated in the concept. | open | B6 | The dashboard loads from the provisioned file on a clean start, and a screenshot in normal operation shows all twelve machines. | | |
| FR10 | The hourly aggregates are available as a view that the existing DWH can read (A7). | open | B2 | An integration test selects from the view with a plain SQL query. | | |
| FR11 | The demonstration scenario starts with one command and produces the same readings and the same alerts on every start. | open | B7 | Two runs with the same seed give identical alert rows, apart from the timestamps of the stages. | | |
| FR12 | Raw readings are moved to a long-term archive. | deferred | | Not testable in the prototype, see ADR-0009. | | |

FR12 belongs to the architecture but not to the prototype. It is specified so
that the limit of the prototype is explicit.

## Non-functional requirements

| ID | Requirement | Status | Item | Check | Implemented in | Verified by |
| --- | --- | --- | --- | --- | --- | --- |
| NFR1 | A critical temperature is visible as an alert in the database within five seconds of its measurement (A5) at the load of the case, 12 readings per second. | open | B7, B8 | Measurements M1 and M4 report median, 95th percentile, and maximum of the time from `measured_at` to the alert row. The requirement is met if the maximum is at most five seconds over the recorded runs. | | |
| NFR2 | No reading is lost and none is stored twice when the processor is stopped and restarted under load. | open | B5, B8 | An integration test and measurement M3 compare the `sequence` values sent with those stored and report gaps and repeats, which have to be zero. | | |
| NFR3 | Every tool used in the prototype is available free of charge. | open | B2 | The list of tools with their licences in `README.md`, checked against the licence texts of the pinned versions. | | |
| NFR4 | The complete stack starts with a single command on a developer machine. | open | B2 | `docker compose up -d` brings every container to healthy from a clean state. | | |
| NFR5 | Every container image and every Python dependency is pinned to an exact version. | open | B0, B2 | A check of `compose.yaml` for tags other than an exact version, and `uv sync --locked` in continuous integration. | | |
| NFR6 | The streaming part absorbs a measured load, and the load at which it misses the latency target or accumulates consumer lag is found with the steps of A9. | open | B8 | Measurement M2 reports offered against achieved readings per second for every step of A9 and the first step at which the stop rule applies. | | |
| NFR7 | The rules and the sensor model are covered by unit tests that run without Docker. | open | B3, B4 | `uv run pytest -m "not integration"` passes in continuous integration, and the modules `config`, `sensor`, `rules`, and `messages` import neither the Kafka client nor the database driver. | | |
| NFR8 | The timestamps `measured_at`, the Kafka log append time, `processed_at`, and `stored_at` are stored with every reading, so that the latency of each stage can be computed from the database. | open | B5 | An integration test reads the four timestamps of a stored reading and checks that they are in the order of the stages. | | |
