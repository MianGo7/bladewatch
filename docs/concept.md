# Concept

Status: fixed in B1. A later change to an assumption, a tool, or a decision is
recorded in `decision-log.md`.

## Initial situation

RotorWind GmbH is a medium-sized manufacturer of rotor blades for wind
turbines in Emden. Unplanned machine failures are a significant cost driver,
and experience in the plant shows that the temperature of a machine is the
critical factor: a machine that runs too hot breaks down, and the material
inside it can be damaged as well. The company therefore intends to equip its
production machines with temperature sensors that measure at short, regular
intervals, to alert an employee as soon as a machine reaches a critical
temperature, and to keep the history of every machine for later analysis. A
real-time dashboard is meant to show the state of all machines at a glance.

The existing IT landscape consists of a data warehouse (DWH) with relational
structures that is loaded once a week and serves business intelligence (BI)
reporting. Temperature data is not processed at all so far. It arrives
continuously and at high frequency, its volume grows with every sensor that is
added, and it does not fit the relational, batch-oriented structure of the
DWH. A weekly load cannot raise an alert within seconds, and the DWH was not
designed to store and query billions of time-stamped readings. A solution
therefore needs additional components that work alongside the DWH rather than
replacing it.

## Goal and questions

The goal of the project is an architecture that captures, processes, stores,
and visualises machine temperatures in real time, and a working prototype of
that architecture that the steering committee can see in operation. The
prototype answers three questions that a decision about the next steps
depends on:

1. Can a critical temperature be detected and reported to an employee within
   a few seconds of the measurement, using only freely available tools?
2. How much load can the streaming part of the architecture absorb on modest
   hardware, and where is its limit?
3. Does the architecture lose or duplicate readings when a component fails
   and recovers?

## Architecture

The architecture follows the stages of a streaming data pipeline: data
collection at the machines, a message queue that decouples the producers of
data from its consumers, continuous analysis of the stream, storage of the
raw and aggregated data, and access to the data through a dashboard and
alerts. The existing DWH remains the system for weekly reporting and receives
aggregated temperature data instead of raw readings. Long-term retention of
raw data in a low-cost archive is part of the architecture but not of the
prototype.

| Layer | Responsibility | Prototype |
| --- | --- | --- |
| Data collection | Read the sensors and publish readings | Sensor simulator with a Kafka producer |
| Message queuing | Buffer and distribute the stream, retain it for replay | Apache Kafka |
| Stream processing | Check every reading against the thresholds, raise alerts | Python stream processor |
| Hot storage | Store raw readings and alerts, aggregate them over time | TimescaleDB |
| Data access | Real-time dashboard, alerting, historical analysis | Grafana, with a webhook receiver for the notification |
| Archive | Keep raw data cheaply for years | Conceptual only |
| Reporting | Weekly BI reporting in the existing DWH | Conceptual only, fed by an aggregate view |

The tool for every layer, the alternatives that were rejected, and the
reasons are recorded in ADR-0003 to ADR-0012 and summarised below.

## Architectural style

The streaming path follows a Kappa style: one processing path, one code base
for the rules, and the retained log of the message queue as the source for
recomputation. A Lambda style with a separate batch layer was rejected because
it would implement the alert rules twice and keep two views of the same data
synchronised. The existing DWH is not a batch layer of this architecture. It
remains the system for weekly reporting and receives hourly aggregates
through a view, so that it is a consumer of the architecture and not a part
of its processing path. The reasoning is recorded in ADR-0003.

## Tool matrix

| Layer | Tool in the prototype | Rejected alternatives | Main reason | ADR |
| --- | --- | --- | --- | --- |
| Data collection | Python sensor simulator with a direct Kafka producer; an MQTT or OPC UA gateway at the edge in production | Eclipse Mosquitto with a bridge to Kafka, OPC UA gateway | The sensors are simulated, so a field protocol would only test the bridge | ADR-0004 |
| Message queuing | Apache Kafka in KRaft mode | RabbitMQ, an MQTT broker alone | Retained, partitioned log that several consumers read independently and that can be replayed | ADR-0005 |
| Stream processing | Python consumer | Kafka Streams, Apache Flink, Apache Spark Structured Streaming | The scale of the case does not need a distributed engine, and the rules stay testable without a cluster | ADR-0006 |
| Hot storage | TimescaleDB | InfluxDB, Apache Cassandra | SQL access for the DWH and for Grafana, continuous aggregates, retention and compression policies | ADR-0007 |
| Data access | Grafana | Splunk Free, the existing BI tool | Real-time refresh, built-in alerting, and a PostgreSQL data source | ADR-0008 |
| Archive | S3-compatible object storage with columnar files, conceptual only | Apache Hadoop HDFS | No cluster to operate for write-once files | ADR-0009 |
| DWH integration | SQL view on the hourly continuous aggregate | Talend Open Studio | The tool is retired, and the existing weekly load needs only a view | ADR-0010 |

All four example tools named in the task are addressed: Apache Kafka is chosen,
and Apache Hadoop, Talend Open Studio, and Splunk Free are rejected for the
prototype with the reasons given in ADR-0009, ADR-0010, and ADR-0008. The
tools of the prototype are available free of charge, which satisfies NFR3. The
licence texts of the pinned versions are checked in B2, when the versions are
fixed, because parts of TimescaleDB are published under the Timescale
License and not under an open source licence.

## Message format and delivery semantics

Readings and alerts are serialised as JSON documents with an explicit
`schema_version`, encoded as UTF-8, and keyed by `machine_id`, so that all
messages of one machine reach one partition in order. Avro with a schema
registry was rejected because it adds a container and a build step without
benefit at the load of the case; the cost of text encoding is measured in M2
(ADR-0011). A reading carries these fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | integer | Version of this format, 1 |
| `machine_id` | string | Identifier of the machine, also the message key |
| `sequence` | integer | Counter per machine, starting at 0, increased by one per reading, so that a gap shows a lost reading and a repeat shows a duplicate independently of the clocks |
| `measured_at` | string | Time of measurement in UTC, ISO 8601, set by the sensor simulator |
| `temperature_c` | number | Temperature in degrees Celsius |

The machine type is not part of the message. The processor derives it from the
configuration, so that the thresholds have one source. An alert carries these
fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | integer | Version of this format, 1 |
| `alert_id` | string | Derived from the machine, the kind, and the `measured_at` of the reading that opened the alert, so that a replayed reading produces the same identifier |
| `machine_id` | string | Machine that raised the alert, also the message key |
| `kind` | string | `threshold` or `rate_of_rise` |
| `severity` | string | `warning` or `critical` |
| `status` | string | `raised` or `closed` |
| `temperature_c` | number | Temperature of the reading that caused the transition |
| `limit` | number | Threshold in degrees Celsius, or rate limit in degrees Celsius per minute |
| `measured_at` | string | Time of measurement of that reading, UTC |
| `processed_at` | string | Time at which the processor raised or closed the alert, UTC |

The states and transitions of an alert are specified in B4 and may add
fields under a new schema version. Four timestamps make the latency of every
stage computable from stored rows: `measured_at` from the sensor simulator,
the log append time of the Kafka record, `processed_at` from the processor,
and `stored_at` from the database (ADR-0011).

The delivery between Kafka and the database is at least once, and storing a
reading is idempotent. The processor commits its offset after the database
transaction has committed, and the readings table rejects a second row with the
same `machine_id` and `measured_at`. A crash between the two steps therefore
leads to a redelivery that is stored once. Exactly-once semantics through Kafka
transactions were rejected because they cover Kafka topics and not a database
sink (ADR-0012).

## Assumptions

Every value the prototype relies on appears here with its unit and either a
source with a page number or the word assumption and the reasoning. All values
are fixed in B1 and a later change is recorded in `decision-log.md`.

| ID | Assumption | Value | Basis |
| --- | --- | --- | --- |
| A1 | Number and types of monitored machines | Twelve machines in three types, four per type: heated blade moulds, post-curing ovens, and grinding machines | Assumption. Moulds are the machines whose heating cycle the literature describes in detail (Lux et al., 2020, p. 400). Ovens and grinding machines are assumed to complete the process chain, and grinding machines are kept to show that the architecture is not tied to one machine type. Four per type gives an overview that is readable on one dashboard and a scenario in which one machine fails among eleven that do not. The identifiers are `mould-01` to `mould-04`, `oven-01` to `oven-04`, and `grinder-01` to `grinder-04` |
| A2 | Sensors per machine | One temperature sensor per machine | Assumption, simplification. Several sensors per machine change the load, not the architecture, and the load is covered by A9 |
| A3 | Measurement interval | One reading per sensor per second | Assumption. The heating rate of a mould in the published cycles lies between 0.53 and 1.5 degrees Celsius per minute (Lux et al., 2020, p. 400 and p. 405), which is at most 0.025 degrees Celsius per second, so one second resolves the process far more finely than needed. The interval also leaves at most one second of the five second budget of A5 to the sampling |
| A4 | Normal operating temperature, warning threshold, and critical threshold per machine type | See the table below | See the table below |
| A5 | Latency target from measurement to visible alert | At most five seconds, measured from `measured_at` to the alert row in the database and on the dashboard | Assumption. The temperature of a machine rises over minutes (Lux et al., 2020, p. 400), so an alert within seconds leaves time to intervene |
| A6 | Alert channel | Alert topic, alert table on the dashboard, and a Grafana contact point that posts to a local web hook; no real e-mail or text message | Assumption. A real channel would need an external service and credentials, and it would not change the architecture |
| A7 | Integration with the existing DWH | Hourly aggregates per machine, exposed as a view for the existing weekly load | Assumption. The weekly load needs aggregates, not raw readings, and a view requires no change to the existing process |
| A8 | Deployment | Single node with Docker Compose for the prototype; clustered deployment described for production | Assumption. The prototype has to start with one command on a developer machine (NFR4) |
| A9 | Load for the scalability measurement | Reference of 200 machines with 10 sensors each at one reading per second, which is 2,000 readings per second. Steps of 12, 100, 500, 2,000, 5,000, 10,000, 25,000, 50,000, and 100,000 readings per second. The ramp stops at the first step that misses the target of A5 or at which the consumer lag grows continuously over the step | Assumption. The 12 readings per second of the case are the first step. The plant-size reference is a plausible order of magnitude for a site with several production halls, and the steps above it search for the limit of the test machine. If no step fails, the result is that the limit lies above the highest step, and it is not extrapolated. The measured limit is that of a single-node prototype under the recorded Docker limits, not of a production cluster |
| A10 | Partitions and replication of the Kafka topics | `machine-temperature` with 6 partitions, `machine-alerts` with 3 partitions, replication factor 1 | Assumption. Parallelism of a consumer group is limited to the number of partitions (ADR-0006), and six partitions allow up to six consumers for the scalability measurement. The default partitioner of the producer library places the twelve machine identifiers on five of the six partitions, with six of them on one partition (observed on 2026-10-19 with confluent-kafka 2.16.0 and a topic of six partitions), so that at most five consumers of a group have work at the load of the case. Many more keys at the higher steps of A9 are expected to even out the spread, and M2 shows whether they do The alert topic carries far fewer messages, so three partitions suffice. A single broker (A8) cannot hold more than one replica |
| A11 | Retention of the Kafka topics | Seven days for both topics | Assumption. The retention has to outlast an outage of the processor over a weekend with margin, and it matches the weekly rhythm of the existing DWH. At the highest steps of A9 the volume of seven days is not bounded by this value, so the topics are recreated between the steps of M2 |
| A12 | Chunking, compression, and retention of the readings in TimescaleDB | Chunk interval of one day, compression of chunks older than seven days, deletion of raw readings older than 90 days, refresh of the hourly aggregate every 15 minutes over a window from three days to one hour before now | Assumption. At the 12 readings per second of the case a one day chunk holds about one million rows, and at the 2,000 readings per second of A9 about 173 million rows, which is still a manageable chunk. Recent chunks stay uncompressed for the dashboard and for late rows. Ninety days bound the hot storage while the hourly aggregates, which are kept without limit in the prototype, and the archive of the target architecture hold the longer history. The refresh window has to start well inside the retention, so that no bucket is recomputed from dropped chunks |
| A13 | Standard deviation of the measurement noise of a sensor | 0.5 degrees Celsius, independent per reading and normally distributed | Assumption. No source gives the noise of a sensor on such a machine. The value is small against the smallest margin of 10 degrees Celsius between the normal and the warning temperature of a type, which is 20 standard deviations, so that normal operation does not raise an alert by chance, and it is large enough to show as noise on the dashboard. The tests of the rules in B4 use series of their own around a threshold and do not depend on this value (ADR-0017) |
| A14 | Margin by which the temperature has to fall before an alert closes | 2.0 degrees Celsius below the warning threshold | Assumption. The margin is four standard deviations of the sensor noise of A13, so that noise alone does not reopen or flap an alert, and it is far below the gap of 10 to 15 degrees Celsius between the normal and the warning temperature of every machine type, so that a machine that returns to normal always closes its alert. The alert of a mould closes below 78, of an oven below 108, and of a grinding machine below 58 degrees Celsius (ADR-0018) |
| A15 | Window and limit of the rate of rise check | Sliding window of 60 seconds, least squares slope over its readings, limit of 3 degrees Celsius per minute, check silent until the window is full | Assumption. The limit is twice the highest heating rate of 1.5 degrees Celsius per minute in the published cycles (Lux et al., 2020, p. 405). With the noise of A13 the standard error of the slope over 60 readings is 0.22 degrees Celsius per minute, so that the limit lies 6.7 standard errors above the highest normal heating rate. Sliding windows are described by Akidau et al. (2015, p. 1794). A rate alert additionally closes only after the rate has stayed below the limit for a full window (ADR-0018) |
| A16 | Rate of the overheating in the demonstration scenario | 5 degrees Celsius per minute | Assumption, chosen for the demonstration. The rate lies above the limit of 3 degrees Celsius per minute of A15, so that the rate of rise warning is raised before the warning threshold is reached. For a mould that starts at 70 degrees Celsius, the warning threshold of 80 is crossed after 120 seconds and the critical threshold of 95 after 300 seconds |
| A17 | Batch size and wait of the processor | At most 500 readings per batch, and a partial batch is processed after 0.2 seconds | Assumption. The batch bounds the size of one database transaction and one round trip, and the wait is the largest delay that batching adds to the latency, which is 4 percent of the five seconds of A5. The values are fixed before the measurements and are not changed to improve a result (ADR-0020) |
| A18 | Refresh interval of the dashboard | One second | Assumption. The dashboard polls the database and does not receive pushed updates, so the refresh interval adds to the time until an alert is visible on it. At one second the worst case adds one second and the duration of the queries to the latency of the alert row, which keeps the dashboard within reach of the five seconds of A5. Grafana has to be told to allow an interval below its default lower limit of five seconds (ADR-0021). Measurement M4 reports the alert on the dashboard separately |
| A19 | Evaluation and notification timing of the alert rule | The rule is evaluated every 10 seconds with no pending period, a new alert is notified at once, and a further alert of the same group follows within 10 seconds | Assumption. Ten seconds is the shortest evaluation interval that Grafana allows by default, so it was expected to bound the delay between an alert in the database and its notification. The measured delay of 25.9 to 27.2 seconds exceeds the 20 seconds of the evaluation interval plus the group interval. It consists of 6.3 to 7.1 seconds until the rule evaluates the alert as firing and a constant 20.02 seconds from there to the notification, and the cause of the constant has not been established (ADR-0023). The notification is not part of the target of A5, which ends at the alert row and the dashboard |
| A20 | Timeline of the demonstration scenario | All machines run normally for 100 seconds, then one mould rises at the rate of A16 until the run ends after 460 seconds, and the measurement M4 is repeated three times | Assumption. The 100 seconds fill the window of 60 seconds of A15 with normal readings, so that the rate warning is a reaction to the rise and not to the start of the run. From 70 degrees Celsius at 5 degrees Celsius per minute the critical threshold of 95 is reached after 400 seconds, which leaves 60 seconds for the notification and the history after it. Three repetitions give a median, a maximum, and a check of the repeatability at a total of about 25 minutes |

The thresholds of A4 are the values at which the processor checks every
reading. The warning threshold is the lower of the two, and a reading at or
above the critical threshold is critical.

| Machine type | Normal in degrees Celsius | Warning in degrees Celsius | Critical in degrees Celsius | Basis |
| --- | --- | --- | --- | --- |
| Heated blade mould | 70 | 80 | 95 | The normal value is the hold temperature of the standard mould cycle (Lux et al., 2020, p. 400). The warning value is the maximum mould temperature that was chosen for the thick zones so that the adhesive peak stays below its degradation limit of 160 degrees Celsius (Lux et al., 2020, p. 405; the limit is stated on p. 400). The critical value is the highest mould temperature used anywhere in the published cycle, in the thin trailing edge zone (Lux et al., 2020, p. 405). A reading above it lies outside every published operating point, which is the interpretation applied here and not a statement of the source |
| Post-curing oven | 100 | 110 | 120 | Assumption for all three values. The 100 degrees Celsius of the laboratory validation oven in the same source (Lux et al., 2020, p. 400) is a plausibility check only, because it is not a production post-curing oven. The margins of 10 and 20 degrees Celsius are chosen so that the warning leaves time to intervene before the critical value |
| Grinding machine | 45 | 60 | 75 | Assumption for all three values, no source. Grinding machines are kept to show that the architecture is not tied to one machine type |

The parameters of the checks that A4 does not cover, the margin of A14 and the
window and limit of A15, were fixed when the rules were implemented in B4.

## Demonstration scenario for the steering committee

The scenario shows the task of the streaming part of the architecture: all
machines run in their normal temperature band, one machine starts to overheat
at a defined time, the processor raises a warning and then a critical alert,
the alert appears on the dashboard and through the contact point, and the
history of the machine shows the course of the incident afterwards. The
scenario is scripted and seeded, so that it runs identically every time and
the time from crossing the critical threshold to the visible alert can be
measured. The command `python -m bladewatch scenario --reset` runs it (ADR-0023).

The overheating of the scenario rises at 5 degrees Celsius per minute (A16),
which is above the limit of 3 degrees Celsius per minute of A15, so that the
rate of rise warning comes first. For a mould that starts at 70 degrees Celsius,
the warning threshold of 80 is crossed after about 2 minutes and the critical
threshold of 95 after about 5 minutes. In the tests of the rules, a mould that
has run normally for 100 seconds and then rises at that rate raises the rate of
rise warning 34 seconds after the rise begins, at 72.9 degrees Celsius, which is
86 seconds before the warning threshold is reached. If the rise begins at the
start of a run, the window has to fill first, and the warning follows after 60
seconds at 75 degrees Celsius, which is still 60 seconds before the threshold.

## Measurement plan

| ID | Measurement | Indicator |
| --- | --- | --- |
| M1 | End-to-end latency under the load of the case | median, 95th percentile, and maximum per stage: measurement to broker, broker to processor, processor to database, measurement to alert |
| M2 | Throughput | offered against achieved readings per second, consumer lag, the load at which the latency target is missed |
| M3 | Fault tolerance | readings sent, stored, and duplicated when the processor is stopped and restarted under load, and the time to catch up |
| M4 | Demonstration scenario | time from crossing the critical threshold to the alert in the database and on the dashboard |

The measurement M4 is defined in the code of the scenario. Its starting point is the time of
measurement of the reading that reaches the critical threshold. Its end points are the
storage of the alert event in the database, the first answer of a query to Grafana that
contains the alert, which is sent every 100 milliseconds against the view that the table of
open alerts of the dashboard reads, and the arrival of the notification at the receiver. The
dashboard itself shows the alert at its next refresh, which is at most one second later
(A18). The path to the database is split into the stages of measurement to broker, broker to
processor, and processor to database. A run lasts 460 seconds and the measurement is
repeated three times (A20), each time from an empty state.

## Points closed in B1

- O1: A1, A3, A4, and A9 have values and a basis in the assumption table.
- O2: every layer has a tool and an ADR, ADR-0004 to ADR-0010.
- O3: the architectural style is Kappa, ADR-0003.
- O4: the message format is ADR-0011 and the delivery semantics ADR-0012.
