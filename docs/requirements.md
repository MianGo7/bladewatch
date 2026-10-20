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
| FR1 | The prototype captures the temperature of each of the twelve machines of A1 once per second (A3) and publishes each reading with the fields of ADR-0011. | done | B3 | An integration test finds the readings of every machine in `machine-temperature`, with the expected fields and consecutive `sequence` values. | `src/bladewatch/sensor.py`, `messages.py`, `producer.py`, `cli.py` (`python -m bladewatch produce`) | `tests/integration/test_producer.py` (twelve machines, consecutive sequence numbers, format), `tests/test_producer.py` (one tick per interval), `tests/test_messages.py` |
| FR2 | Readings are buffered in Kafka, keyed by machine, and retained for a configured period, so that a consumer can read them again from the start of the retention. | done | B2, B3 | An integration test reads a topic from the earliest offset with a new consumer group and receives the readings that were published before. | `compose.yaml` (`kafka-init`), `src/bladewatch/producer.py` (key is the machine identifier) | `tests/integration/test_producer.py::test_the_message_key_is_the_machine_identifier`, `test_all_readings_of_a_machine_are_in_one_partition`, and the replay with a new consumer group in `tests/integration/test_stack.py` |
| FR3 | Every reading is checked against the warning and the critical threshold of the type of its machine (A4), and an alert is raised when a threshold is reached. | done | B4 | Unit tests with readings below, at, and above each threshold of each machine type. | `src/bladewatch/rules.py` (`AlertTracker`), thresholds in `config.py` | `tests/test_rules.py`: readings below, at, and above the warning and critical threshold of each machine type |
| FR4 | An alert does not flap. It is raised once when a threshold is reached and closed only when the temperature has fallen below the threshold by a configured margin. | done | B4 | Unit tests at the exact threshold values and with a series that oscillates around a threshold and raises exactly one alert. | `src/bladewatch/rules.py` (closing level and margin of A14) | `tests/test_rules.py`: exact closing level, oscillation around the warning threshold and around the closing level |
| FR5 | A rise of the temperature over a configured window above a configured limit raises a warning before the warning threshold is reached. | done | B4 | A unit test with a deterministic series that rises faster than the limit while it stays below the warning threshold. | `src/bladewatch/rules.py` (`RateWindow`, limit and window of A15) | `tests/test_rules.py`: deterministic series checked against an independent least squares fit, normal operation and normal heating without a warning |
| FR6 | An alert is visible to an employee on the dashboard and through a notification outside the dashboard, which is the contact point of A6. | done | B6 | The alert table of the dashboard shows a critical alert from the processor, and the contact point delivers a message to the local web hook. | `infra/grafana/provisioning/alerting/` (contact point, alert rule, policy), `src/bladewatch/notifier.py`, `infra/timescaledb/006-views.sql` (`open_alerts`), `infra/grafana/dashboards/bladewatch.json` (open alerts panel) | `tests/integration/test_alerting.py`: a critical alert in the database reaches the receiver as a firing notification and its closing as a resolved one; `tests/integration/test_dashboard.py`: the open alerts panel shows the critical alert of the processor |
| FR7 | Every reading is stored with its four timestamps, `measured_at`, the Kafka log append time, `processed_at`, and `stored_at`, and every alert is stored with `measured_at`, `processed_at`, and `stored_at`. | done | B5 | An integration test finds every reading and every alert of a known run in the database, each with the timestamps named here. | `src/bladewatch/storage.py`, `processor.py`, `infra/timescaledb/001-readings.sql`, `002-alerts.sql` | `tests/integration/test_storage.py`, `tests/integration/test_processor.py`: every reading of a known run with its four timestamps, every alert event with `measured_at`, `processed_at`, and `stored_at` |
| FR8 | The history of a machine can be analysed as hourly minimum, mean, and maximum. | done | B2 | An integration test compares the continuous aggregate with the same aggregation computed from the raw readings of a known series. | `infra/timescaledb/003-aggregates.sql` (`readings_hourly`) | `tests/integration/test_stack.py::test_the_hourly_aggregate_equals_the_aggregate_of_the_raw_readings` |
| FR9 | A dashboard shows the current temperature of every machine, its thresholds, and the open alerts, and refreshes within the interval stated in the concept. | done | B6 | The dashboard loads from the provisioned file on a clean start, and a screenshot in normal operation shows all twelve machines. | `infra/grafana/dashboards/bladewatch.json`, `infra/timescaledb/005-machines.sql`, `006-views.sql`, `src/bladewatch/processor.py` (registers the machines) | `tests/integration/test_dashboard.py`: the provisioned dashboard, its refresh of one second, and the result of every panel query for a run of the pipeline; `tests/test_dashboard_file.py`; the screenshot `docs/figures/dashboard-overview-normal-operation.png` shows all twelve machines in normal operation |
| FR10 | The hourly aggregates are available as a view that the existing DWH can read (A7). | done | B2 | An integration test selects from the view with a plain SQL query. | `infra/timescaledb/003-aggregates.sql` (`readings_hourly`, read with plain SQL) | `tests/integration/test_stack.py::test_the_hourly_aggregate_equals_the_aggregate_of_the_raw_readings` |
| FR11 | The demonstration scenario starts with one command and produces the same readings and the same alerts on every start. | done | B7 | Two runs with the same seed give identical alert rows, apart from the timestamps of the stages. | `src/bladewatch/scenario.py`, `cli.py` (`python -m bladewatch scenario --reset`) | `tests/integration/test_scenario.py`: repeated runs raise identical alert events, and the same seed and start time give identical identifiers; in the measurement M4 the alert events of three runs are identical in kind, severity, status, temperature, limit, and time from the start (`docs/figures/m4-demonstration-scenario.md`) |
| FR12 | Raw readings are moved to a long-term archive. | deferred | | Not testable in the prototype, see ADR-0009. | | |

FR12 belongs to the architecture but not to the prototype. It is specified so
that the limit of the prototype is explicit.

## Non-functional requirements

| ID | Requirement | Status | Item | Check | Implemented in | Verified by |
| --- | --- | --- | --- | --- | --- | --- |
| NFR1 | A critical temperature is visible as an alert in the database within five seconds of its measurement (A5) at the load of the case, 12 readings per second. | done | B7, B8 | Measurements M1 and M4 report median, 95th percentile, and maximum of the time from `measured_at` to the alert row. The requirement is met if the maximum is at most five seconds over the recorded runs. | `src/bladewatch/scenario.py` (measurement M4), `src/bladewatch/bench/latency.py` (measurement M1), `processor.py`, `storage.py` | M1 on the commit 5a63d01 with a clean working tree, three undisturbed runs of 600 seconds at 12 readings per second: over 21,600 readings the time from measurement to storage has a median of 0.126, a 95th percentile of 0.218, and a maximum of 0.557 seconds, and the six raised alert events reached the database after at most 0.229 seconds, against the five seconds of A5 (`docs/figures/m1-latency.md`). M4 gives 0.106 to 0.151 seconds for the critical alert (`docs/figures/m4-demonstration-scenario.md`) |
| NFR2 | No reading is lost and none is stored twice when the processor is stopped and restarted under load. | done | B5, B8 | An integration test and measurement M3 compare the `sequence` values sent with those stored and report gaps and repeats, which have to be zero. | `src/bladewatch/processor.py` (order of processing, restoration), `storage.py` (idempotent insert), `src/bladewatch/bench/faults.py` (measurement M3) | `tests/integration/test_processor.py`: exactly once after a clean restart in the middle of an open alert, after a crash between the database commit and the offset commit, and after a failed database write. M3 on the commit 5a63d01, six runs at 2,000 readings per second, three with the processor killed and three stopped after its batch: of 240,000 readings sent in each run, 240,000 distinct readings were stored, none was missing, and no row was stored twice (`docs/figures/m3-faults.md`). The same result came in a first session on an uncommitted tree. The restarted processor received no reading again in any of the twelve runs, so that the path of the redelivery is covered by the integration test and not by M3 |
| NFR3 | Every tool used in the prototype is available free of charge. | done | B2 | The list of tools with their licences in `README.md`, checked against the licence texts of the pinned versions. | `README.md` (tools and licences), ADR-0013 | Licence texts read in the pinned images and in the TimescaleDB repository, see ADR-0013 |
| NFR4 | The complete stack starts with a single command on a developer machine. | done | B2 | `docker compose up -d` brings every container to healthy from a clean state. | `compose.yaml` | `tests/integration/test_stack.py::test_every_long_running_container_is_healthy` after `docker compose down -v` and `docker compose up -d --wait` (11 seconds on 2026-10-18) |
| NFR5 | Every container image and every Python dependency is pinned to an exact version. | done | B0, B2 | A check of `compose.yaml` for tags other than an exact version, and `uv sync --locked` in continuous integration. | `compose.yaml`, `pyproject.toml`, `uv.lock` | `tests/test_compose_images.py`, `tests/test_dependency_pins.py` |
| NFR6 | The streaming part absorbs a measured load, and the load at which it misses the latency target or accumulates consumer lag is found with the steps of A9. | done | B8 | Measurement M2 reports offered against achieved readings per second for every step of A9 and the first step at which the stop rule applies. | `src/bladewatch/bench/throughput.py` (measurement M2), `measure/stats.py` (criteria) | Two results hold in both sessions of M2, each of three runs of the ramp with 120 seconds per step. The step of 2,000 readings per second passed in all runs, and the storage rate of one processor saturates at about 24,000 to 25,200 readings per second: the step of 50,000 stored 25,217 and 25,054 per second in the session on the commit 5a63d01 with a 95th percentile of 65 seconds, and the step of 25,000 stored 24,018 and 24,913 per second in a first session on an uncommitted tree. Between 5,000 and 25,000 readings per second the verdicts vary and the limit lies in that band: on the clean commit, 5,000 failed in one run by a growth of the lag floor of 55.8 against a limit of 50 messages per second and passed in two, and 10,000 and 25,000 passed in the two runs that reached them, whereas the first session failed 10,000 in two runs and 25,000 in one (`docs/figures/m2-throughput.md`) |
| NFR7 | The rules and the sensor model are covered by unit tests that run without Docker. | done | B3, B4 | `uv run pytest -m "not integration"` passes in continuous integration, and the modules `config`, `sensor`, `rules`, and `messages` import neither the Kafka client nor the database driver. | `tests/test_config.py`, `test_sensor.py`, `test_messages.py`, `test_rules.py` | `uv run pytest -m "not integration"` locally; `tests/test_module_boundaries.py` checks that `config`, `sensor`, `rules`, and `messages` import neither the Kafka client nor a database driver |
| NFR8 | The timestamps `measured_at`, the Kafka log append time, `processed_at`, and `stored_at` are stored with every reading, so that the latency of each stage can be computed from the database. | done | B5 | An integration test reads the four timestamps of a stored reading and checks that they are in the order of the stages. | `src/bladewatch/processor.py`, `storage.py`, `infra/timescaledb/001-readings.sql` | `tests/integration/test_storage.py` and `tests/integration/test_processor.py`: the four timestamps of a stored reading are in the order of the stages |

## Final status

Of the 20 requirements, 19 are done and verified by a test, a measurement, or a check
of a file, as the last column states, and one, FR12, is deferred. FR12 belongs to the
architecture and not to the prototype, and ADR-0009 gives the reason. The measured
requirements, NFR1, NFR2, and NFR6, rest on the runs of 2026-10-20 that are named in
their rows, and the first two were met in both sessions of those runs.

## Limits of the prototype

The limits below are the ones that the work found or accepted. Each is stated with the
record in which it is argued, so that the critical reflection can start from them.

- The sensors are simulated, with a constant setpoint and white noise and without a daily
  rhythm (ADR-0017), and the data collection has no edge gateway, no protocol conversion,
  and no buffering at the edge (ADR-0004).
- The stack is one node: one Kafka broker with a replication factor of 1 and one
  database, so that neither survives its own failure (A8, ADR-0013). It has no
  authentication between its components, uses development credentials, and binds every
  port to the loopback interface.
- One processor runs per group. A second consumer is not supported, because the state of
  a machine would have to be restored when it moves (ADR-0020), so that the scale-out that
  the six partitions allow is argued and not measured (A10). The processor stops on a
  database error and relies on a supervisor that the prototype does not configure.
- A measured limit of that processor is about 25,000 readings per second, and the verdicts
  between 5,000 and 25,000 readings per second differed between two sessions with the same
  code (NFR6). From the 60th second of a stream the rate check costs about ten times as
  much per reading, because the least squares fit runs over a full window (ADR-0018,
  ADR-0026). The start of the processor restores the state of every machine from the
  database, which took 54 seconds for 50,000 streams.
- Delivery is at least once. The alert topic can hold a repeated event after a failure of
  the database write, and a consumer recognises the repeat by the identifier and the
  severity (ADR-0012, ADR-0020). The twelve runs of M3 did not hit the window between the
  database commit and the commit of the offsets, so that this path is covered by the
  integration test and not by the measurement.
- A rule has no timeout for a sensor that falls silent while an alert is open (ADR-0018),
  and a message that is not valid is skipped, counted in the log, and not stored
  (ADR-0020).
- The archive of FR12 is not built, so that nothing can be recomputed beyond the retention
  of seven days of the topics, and the hot storage deletes raw readings after 90 days
  (ADR-0009, A11, A12). The hourly aggregate is refreshed every 15 minutes, so that the
  view lags by up to that time.
- The integration with the existing data warehouse is a SQL view, and that the existing
  load can read from PostgreSQL is an assumption to be verified before a pilot (A7,
  ADR-0010).
- The notification reaches a local receiver 25.9 to 28.7 seconds after the critical reading
  and not within the five seconds of A5, which is stated for the alert in the database and
  on the dashboard. About 20 seconds of it have not been explained (ADR-0023). The dashboard
  shows an alert at its next refresh of one second (A18).
- The TimescaleDB licence for compression and continuous aggregates is not an open source
  licence, which limits the statement that every tool is freely available (ADR-0013).
  The code is not checked by a static type checker (ADR-0002).
- The measurements ran on one laptop with the default limits of Docker Desktop and with other
  activity not controlled, the clocks of all stages are those of one machine, and a run
  during which the machine slept was discarded and repeated (ADR-0023). The load generator
  publishes the readings of a tick in a burst, and the streams of a step are one per sensor
  (ADR-0024, A21). M3 stops the processor and does not fail the broker or the database.

