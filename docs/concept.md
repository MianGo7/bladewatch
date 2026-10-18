# Concept

Status: draft. The open points at the end of this document are closed in
backlog item B1; changes after that point are recorded in `decision-log.md`.

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

## Architecture draft

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
| Data access | Real-time dashboard, alerting, historical analysis | Grafana |
| Archive | Keep raw data cheaply for years | Conceptual only |
| Reporting | Weekly BI reporting in the existing DWH | Conceptual only, fed by an aggregate view |

The tool for every layer, the alternatives considered, and the reasons are
fixed in B1 and recorded as ADRs. The candidates are listed in the tool
matrix below.

## Tool matrix, candidates

| Layer | Candidates | Selection criteria |
| --- | --- | --- |
| Data collection | Direct Kafka producer, MQTT broker such as Eclipse Mosquitto with a bridge to Kafka, OPC UA gateway | Fit with industrial protocols, effort for the prototype |
| Message queuing | Apache Kafka, RabbitMQ, MQTT broker alone | Throughput, retention and replay, delivery semantics, free availability |
| Stream processing | Python consumer, Kafka Streams, Apache Flink, Apache Spark Structured Streaming | Latency, operational effort, fit with the scale of the case |
| Hot storage | TimescaleDB, InfluxDB, Apache Cassandra | Time series functions, SQL access for the DWH, retention and compression, licence |
| Data access | Grafana, Splunk Free, the existing BI tool | Real-time refresh, alerting, licence limits |
| Archive | Apache Hadoop HDFS, object storage | Cost per terabyte, operational effort |
| Data integration with the DWH | Talend Open Studio, SQL views on aggregates loaded by the existing process | Availability of the tool, effort |

## Assumptions

Every value the prototype relies on appears here with its unit and either a
source with a page number or the word assumption and the reasoning. The
values marked as proposed are fixed in B1.

| ID | Assumption | Value | Basis |
| --- | --- | --- | --- |
| A1 | Number and types of monitored machines | proposed: twelve machines in three types, heated blade moulds, post-curing ovens, and grinding machines | assumption, to be checked against the literature on blade manufacturing |
| A2 | Sensors per machine | proposed: one temperature sensor per machine | assumption, simplification; several sensors per machine change the load, not the architecture |
| A3 | Measurement interval | proposed: one reading per sensor and second | assumption |
| A4 | Normal operating temperature, warning threshold, and critical threshold per machine type | open | to be taken from the literature where possible, otherwise assumption |
| A5 | Latency target from measurement to visible alert | proposed: at most five seconds | assumption, reasoning: the temperature of a machine rises over minutes, so an alert within seconds leaves time to intervene |
| A6 | Alert channel | proposed: alert topic, alert table on the dashboard, and a Grafana contact point; no real e-mail or text message | assumption |
| A7 | Integration with the existing DWH | proposed: hourly aggregates per machine exposed as a view for the existing weekly load | assumption |
| A8 | Deployment | proposed: single node with Docker Compose for the prototype; clustered deployment described for production | assumption |
| A9 | Load for the scalability measurement | open, derived from a plausible plant size with several sensors per machine and a higher sampling rate | assumption |

## Demonstration scenario for the steering committee

The scenario shows the task of the streaming part of the architecture: all
machines run in their normal temperature band, one machine starts to overheat
at a defined time, the processor raises a warning and then a critical alert,
the alert appears on the dashboard and through the contact point, and the
history of the machine shows the course of the incident afterwards. The
scenario is scripted and seeded, so that it runs identically every time and
the time from crossing the critical threshold to the visible alert can be
measured. Details are fixed in B7.

## Measurement plan

| ID | Measurement | Indicator |
| --- | --- | --- |
| M1 | End-to-end latency under the load of the case | median, 95th percentile, and maximum per stage: measurement to broker, broker to processor, processor to database, measurement to alert |
| M2 | Throughput | offered against achieved readings per second, consumer lag, the load at which the latency target is missed |
| M3 | Fault tolerance | readings sent, stored, and duplicated when the processor is stopped and restarted under load, and the time to catch up |
| M4 | Demonstration scenario | time from crossing the critical threshold to the alert in the database and on the dashboard |

## Open points, closed in B1

- O1: fix A1, A3, A4, and A9 with sources where they exist.
- O2: choose a tool for every layer of the tool matrix and record the
  reasoning in ADRs.
- O3: decide between a Lambda and a Kappa style for the combination of the
  stream with the existing batch DWH.
- O4: fix the message format of a reading and an alert, and the delivery
  semantics between Kafka and the database.
