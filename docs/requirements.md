# Requirements

This document lists the requirements of the prototype and traces each one to
the code and the tests that satisfy it. It is the working basis for the
description of the solution in the case study and for the evaluation of the
result. A requirement is only marked as done when it is implemented and
covered by a test, a measurement, or a generated figure.

The requirement set is a draft derived from the case. It is revised in B1
against the assumptions in `concept.md`, and every later change is recorded
in `dev-journal.md`.

Status values: open, in progress, done, partly met, deferred. A deferred
requirement is specified on purpose but not implemented, and the reason is
stated next to it.

## Functional requirements

| ID | Requirement | Status | Implemented in | Verified by |
| --- | --- | --- | --- | --- |
| FR1 | The prototype captures the temperature of every monitored machine at a fixed interval and publishes each reading with the machine, the time of measurement, and the value. | open | | |
| FR2 | Readings are buffered in a message queue that decouples the sensors from the processing and retains the stream for a configurable period, so that it can be replayed. | open | | |
| FR3 | Every reading is checked against a warning and a critical threshold for its machine type, and an alert is raised when a threshold is reached. | open | | |
| FR4 | An alert does not flap: it is raised once when a threshold is reached and closed only when the temperature has fallen below the threshold by a defined margin. | open | | |
| FR5 | A rapid rise in temperature raises a warning before the warning threshold is reached. | open | | |
| FR6 | Alerts are made visible to an employee on the dashboard and through a notification outside the dashboard. | open | | |
| FR7 | All readings and alerts are stored with their timestamps for historical analysis. | open | | |
| FR8 | The history of every machine can be analysed as aggregates over time, such as the hourly minimum, mean, and maximum. | open | | |
| FR9 | A real-time dashboard shows the current state of every machine, its thresholds, and the open alerts. | open | | |
| FR10 | Hourly aggregates are available as a view that the existing data warehouse can load. | open | | |
| FR11 | A demonstration scenario with an overheating machine can be started with one command and runs identically on every start. | open | | |
| FR12 | Raw readings are moved to a long-term archive. | deferred | | |

FR12 belongs to the architecture but not to the prototype. It is specified so
that the limit of the prototype is explicit.

## Non-functional requirements

| ID | Requirement | Status | Implemented in | Verified by |
| --- | --- | --- | --- | --- |
| NFR1 | A critical temperature is visible as an alert within the latency target of assumption A5 after its measurement, at the load of the case. | open | | |
| NFR2 | No reading is lost and none is stored twice when the stream processor is stopped and restarted. | open | | |
| NFR3 | Every tool used in the prototype is freely available. | open | | |
| NFR4 | The complete stack starts with a single command on a developer machine. | open | | |
| NFR5 | Every container image and every Python dependency is pinned to an exact version. | open | | |
| NFR6 | The streaming part scales by adding partitions and consumers, and the load it absorbs on the prototype hardware is measured. | open | | |
| NFR7 | The rules and the sensor model are covered by unit tests that run without Docker. | open | | |
