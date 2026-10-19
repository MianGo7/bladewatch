# Decision log

Architecture, tooling, and design decisions in the form of architecture
decision records (ADR). Each record states the context, the decision, the
alternatives that were rejected, and the consequences. A decision that changes
later is not edited in place; a new record supersedes it and says so.

Template for every record:

    ## ADR-NNNN: Title

    Date: YYYY-MM-DD. Status: accepted.

    **Context.** The situation and the forces that made a decision necessary.

    **Decision.** What was decided.

    **Alternatives.** What else was considered and why it was rejected.

    **Consequences.** What follows from the decision, including its costs.

---

## ADR-0001: Python 3.13 with uv and a src layout

Date: 2026-10-18. Status: accepted.

**Context.** The prototype consists of a Python package with a simulator, a
stream processor, and a measurement module, and its results have to be
reproducible on another machine from the repository alone. The interpreter
that macOS provides is version 3.9, which is too old for the language
features the package relies on, so the interpreter and the dependencies have
to be managed by the project.

**Decision.** The project requires Python 3.13, pinned for local development
in `.python-version`, and is managed with uv. The dependencies are declared in
`pyproject.toml` with exact versions and resolved in `uv.lock`, which is
committed. The package lives in `src/bladewatch/` and is built with the uv
build backend, so that the tests import the installed package and never the
working directory by accident.

**Alternatives.** Poetry offers the same lock file workflow but is a
separate tool to install and is slower to resolve, while uv covers the
interpreter, the environment, the lock file, and the build with one binary.
A plain virtual environment with `pip` and a `requirements.txt` was rejected
because it does not lock transitive dependencies, which would undermine
NFR5. A flat layout without `src/` was rejected because it lets a test pass
against uninstalled code.

**Consequences.** Every contributor needs uv, which downloads the interpreter
when it is missing. Any dependency change goes through `uv add` and
`uv remove` and needs its own record in this log. At this stage the project has
no runtime dependency at all.

## ADR-0002: pytest, Ruff, and a continuous integration workflow

Date: 2026-10-18. Status: accepted.

**Context.** The quality criterion rewards tests and a reproducible build, and
the conventions of the project (complete type hints, docstrings, no `print`
in the services, no global random state, no commented out code) are only
worth stating if a tool enforces them. The streaming part cannot be tested
without Docker, so unit and integration tests have to be separable.

**Decision.** Tests are written with pytest 9.1.1. The marker `integration`
is registered and `--strict-markers` is set, so that a misspelled marker
fails instead of silently selecting nothing. Ruff 0.17.1 is the only tool for
formatting and linting. The selected rule families are the pycodestyle and
Pyflakes defaults, import order, bugbear, modern syntax, naming, docstrings in
the Google convention, type annotations, `print` calls, legacy NumPy random
state, import conventions, early returns, simplifications, and commented out
code. The only exemption is the docstring family for files under `tests/`,
because the plain English test names already state the behaviour and a
docstring would repeat them. A GitHub Actions workflow runs the format check,
the lint, and the unit tests on every push and pull request, with the
integration tests excluded because the runner has no Compose stack. The
workflow pins the runner image, both actions, and the uv version to exact
tags.

**Alternatives.** The standard library `unittest` was rejected because it has
no parametrisation or fixtures of comparable convenience. Black, isort,
flake8, and pylint were rejected because Ruff covers their functions in one
dependency with one configuration. Pinning the actions to commit hashes would
be stricter against a moved tag but makes the workflow harder to read, and
the repository is a prototype without secrets in the workflow. A static type
checker such as mypy was not added: the annotation rule only checks that hints
exist and not that they are correct, which is a limit of the current checks
and is revisited if type errors show up in the later modules.

**Consequences.** Two development dependencies are fixed by this record. The
workflow could only be checked for well-formed YAML locally, so its first real
run happens when the repository is pushed to GitHub.

## ADR-0003: Kappa style with the data warehouse as a downstream consumer

Date: 2026-10-18. Status: accepted.

**Context.** The case combines a stream of temperature readings that has to
produce an alert within seconds with an existing data warehouse (DWH) that is loaded once a
week. Two styles are common for such a combination. The Lambda style
unifies online and batch processing in one framework, with a batch layer that
precomputes views from the full data set, a speed layer that computes recent
results with low latency, and a serving layer that answers queries (Kiran et
al., 2015, p. 2786). The Kappa style drops the batch layer and runs every
computation as a stream job over a retained log.

**Decision.** The streaming path follows a Kappa style. There is one
processing path, implemented once, and the retained Kafka log is the source
from which results are recomputed by replay. The existing DWH is not treated
as the batch layer. It stays the system for weekly reporting and receives
hourly aggregates per machine through a view (ADR-0010), so that it consumes
the output of the architecture and does not take part in the processing of
the alerts.

**Alternatives.** A Lambda style was rejected for three reasons. It requires
the alert rules in two implementations, one for the speed layer and one for
the batch layer, and Feick et al. (2018, p. 5) describe the resulting
architecture as highly complex and redundant, with additional effort to keep
both layers synchronised. The batch layer would recompute views that the
case does not need, because the question of the steering committee is the
current state and the recent history of a machine, not a recomputation of
years of readings. Finally, the weekly load of the existing DWH is far too
slow to serve as a speed layer, and a second batch system next to it would
duplicate it. The Kappa style has a recognised limit: without a batch layer it
is not suited to computation intensive jobs such as the training of large
machine learning models (Feick et al., 2018, p. 6). This limit is accepted
because the prototype contains no such job, and it is named again as the
reason for the archive and a batch component in the outlook on predictive
maintenance.

**Consequences.** Recomputation is possible only within the retention period
of the Kafka topics, which is fixed in B2. Older data can be recomputed only
from the archive, which is conceptual in this prototype. The alert rules are
written once, in `rules`, and tested without Docker. Sahal et al. (2020, p.
148) state that predictive maintenance use cases need batch as well as stream
processing; in this architecture the batch part is the weekly DWH reporting
and, in the outlook, analysis of the archive, and both are consumers of the
same stream and not parts of its path.

## ADR-0004: Direct Kafka producer for data collection

Date: 2026-10-18. Status: accepted.

**Context.** The sensors are simulated. The simulator produces readings that
have to reach Kafka with the fields of ADR-0011. On a real shop floor the
sensors would speak a field protocol and a gateway would translate it.

**Decision.** The prototype uses a Python producer that publishes the readings
of the simulator directly to the topic `machine-temperature`. In the target
architecture an edge gateway sits between the sensors and Kafka, speaks
Message Queuing Telemetry Transport (MQTT) or Open Platform Communications
Unified Architecture (OPC UA) towards the machines, and publishes to Kafka. The gateway is described
in the case study and not built.

**Alternatives.** An Eclipse Mosquitto broker with a bridge to Kafka was
rejected for the prototype. It would add a container and a bridge
configuration, and it would test the bridge and the protocol conversion
while the simulator already produces the data in its final form; the
question of the case is the streaming part. An OPC UA gateway was rejected for the same reason and because it would need an
OPC UA server or simulator as a further component. An MQTT broker is
nevertheless the more realistic first hop for sensors with limited resources.

**Consequences.** The prototype does not demonstrate protocol conversion,
buffering at the edge, or the behaviour of a sensor that loses its connection.
These are limits of the prototype and belong in the reflection.

## ADR-0005: Apache Kafka in KRaft mode as the message queue

Date: 2026-10-18. Status: accepted.

**Context.** The queue decouples the sensors from the processing, retains the
stream so that it can be replayed after a failure of the processor (FR2,
NFR2), and has to serve several independent consumers: the stream processor
today, and an archive writer or a second analysis in the target architecture.

**Decision.** Apache Kafka is used, in Kafka Raft (KRaft) mode, so that no separate
coordination service has to be operated. Readings are partitioned by
`machine_id`. Sahal et al. (2020, p. 140) distinguish distributed queuing
technologies from traditional message queues: a traditional queue removes
processed messages and cannot let several consumers act independently on the
same event, whereas a distributed queue persists messages for a period and
serves groups of consumers, with each partition read by one consumer of a
group. The partition count and the retention are fixed in B2.

**Alternatives.** RabbitMQ was rejected, although Sahal et al. (2020, p. 147
and p. 148) propose it for the wind turbine use case. Their reason is the
requirement R6.3, federated queues that link wind farms at different
locations. RotorWind operates machines at one plant, so that requirement does
not apply to this case, and the properties the case does need, retained
history and replay for independent consumers, favour a log. Later RabbitMQ
versions add a log-based stream type that was not evaluated, which limits the
comparison. An MQTT broker alone was rejected because it is designed for
delivery to subscribers and not for retention and replay. Amazon Kinesis,
Microsoft Event Hubs, and Google Pub/Sub, which appear in the same comparison
(Sahal et al., 2020, p. 147), were excluded because they are not freely
available tools that run on a developer machine (NFR3, NFR4).

**Consequences.** Kafka is heavier than the case needs at 12 readings per
second. The measurement M2 shows how much headroom a single broker has on the
test machine. Kafka is the example tool of the task for the processing of data
streams, and the choice therefore covers that item directly.

## ADR-0006: A Python consumer as the stream processor

Date: 2026-10-18. Status: accepted.

**Context.** The processor checks every reading against thresholds, detects a
rapid rise, keeps the state of each alert so that it does not flap (FR3, FR4,
FR5), writes to the database, and publishes alerts. The load of the case is 12
readings per second. The logic of the rules is the part the steering
committee cares about and the part that carries the densest tests.

**Decision.** The processor is a Python application that consumes the topic as
a member of a consumer group and applies the rules from the pure Python module
`rules`. The Kafka client library is chosen in B3, in an ADR of its own, when
the first dependency on it is added.

**Alternatives.** Isah et al. (2019, p. 154300) compare four open source
engines.
Apache Spark Streaming is a micro-batch engine (p. 154309), which in its
default mode adds the length of a batch to the latency and is therefore a poor
match for a target of five seconds. Apache Flink guarantees exactly-once state updates by
checkpointing and replay, and end-to-end exactly-once delivery only when the
source and the sink take part in the checkpointing (p. 154312). It is the
stronger engine for a larger plant but needs a job manager and at least one
task manager as separate processes to operate for 12 readings per second.
Kafka Streams runs inside the Kafka ecosystem, offers windowing and state
stores with fault tolerance (pp. 154310 to 154311), and would keep state across
restarts without own code. It was rejected because it is a Java library, which
would move the rules out of the Python module that is tested without Docker,
and because the prototype is built to show the logic of the rules visibly.
Isah et al. (2019, p. 154313) chose the Spark ecosystem for their own use case
because of its maturity and user base. That reasoning does not carry over to
this case, because a micro-batch engine conflicts with the latency target of
A5, and a mature ecosystem built for large clusters has no advantage at a scale
of 12 readings per second.

**Consequences.** Everything a framework would provide has to be written, and
the cost is a finding of the case study. The state of an alert and of the rate
window is lost when the process stops and has to be restored on start, which is
designed and tested in B5. Parallelism is limited to the number of
partitions, and the measurement M2 determines what one Python process absorbs.
The recommendation for a production plant with several sites is to move to
Flink or Kafka Streams once the load or the number of rules exceeds what one
consumer group in Python handles, and the limits found in M2 support that
argument with a number.

## ADR-0007: TimescaleDB as the hot storage

Date: 2026-10-18. Status: accepted.

**Context.** Readings and alerts must be stored with their timestamps (FR7),
aggregated over time (FR8), exposed to the existing DWH (FR10), and queried by
the dashboard (FR9). Old data should be compressed and removed by policy so that
the size of the store stays bounded.

**Decision.** TimescaleDB, an extension of PostgreSQL, stores the readings in a
hypertable partitioned by time, the alerts in an ordinary table, and the
hourly aggregates in a continuous aggregate. Retention and compression are
policies of the database. Because TimescaleDB is PostgreSQL, the same SQL
serves the processor, the dashboard, and the DWH view.

**Alternatives.** InfluxDB was rejected. In the SciTS benchmark it ingests
faster than TimescaleDB, with saturation at 790,000 points per second against
550,000 at a batch size of 20,000 (Mostafa et al., 2022, p. 6), so the decision does not rest on
ingestion speed; both values are far above the 2,000 readings per second of
the plant-size reference in A9. The decisive points are that the existing DWH
is relational and loads through SQL, that alerts and machine data are joined
by ordinary relational queries, and that the same engine holds both. Apache
Cassandra was rejected because the scalability it offers is not needed at this
scale, whereas Sahal et al. (2020, p. 147) prefer column and NoSQL stores for
wind turbine data on the grounds of scalability and node failure; its query
model is built around known access paths per table and does not offer the ad
hoc joins and aggregations that the dashboard and the DWH view need. The
benchmark also shows that TimescaleDB inherits limitations of PostgreSQL and
gains a factor of 2.33 in ingestion over plain PostgreSQL (Mostafa et al.,
2022, p. 7), which is the reason for the hypertable instead of a plain table.

**Consequences.** The ingestion limit of the database is not expected to be the
bottleneck in M2, and a measurement that shows otherwise is reported. The
compression and continuous aggregate features of TimescaleDB are published under
the Timescale License, which permits free self-hosted use but is not an open
source licence. The term freely available in NFR3 is therefore read as free of
charge, and the licence text of the pinned version is checked in B2.

## ADR-0008: Grafana for data access and alerting

Date: 2026-10-18. Status: accepted.

**Context.** Employees need a dashboard that shows the state of every machine
in real time (FR9) and an alert that reaches them outside the dashboard (FR6).
Splunk Free is the reporting tool named as an example in the task.

**Decision.** Grafana reads TimescaleDB through its PostgreSQL data source,
shows the state and the open alerts on a provisioned dashboard, and sends the
notification through a contact point. Ramos et al. (2025, p. 884) use Grafana
in the same role, as the visualisation of time series data in an industrial
monitoring platform.

**Alternatives.** Splunk Free was rejected. The documentation of
Splunk Enterprise, Admin Manual version 9.4, page "About Splunk Free" (Splunk
Inc., n.d.), states that "The Free license allows you to index 500 MB per day"
and that "Alerting (monitoring) is not available". Alerting is requirement FR6,
and the volume limit would bind as soon as the sensor count grows, which is the
premise of the case. The existing business intelligence (BI) tool was rejected
because it is loaded weekly and has no refresh in seconds; it stays the tool
for reporting.

**Consequences.** The dashboard depends on the PostgreSQL data source, which
queries the database on refresh and does not receive pushed updates. The
shortest dashboard refresh interval therefore enters the latency of the
visible alert, and M4 measures the alert in the database and on the dashboard
separately. Provisioning from files keeps the dashboard reproducible.

## ADR-0009: Object storage for the archive, Hadoop HDFS rejected

Date: 2026-10-18. Status: accepted.

**Context.** Raw readings have to be kept cheaply for years in the target
architecture, while the hot storage keeps a bounded period (FR12). Apache
Hadoop is the storage example named in the task, and the literature
recommends the Hadoop Distributed File System (HDFS) for the raw data of wind
turbine maintenance, stored centrally and distributed by site (Sahal et al., 2020, p. 147).

**Decision.** The archive is described as S3-compatible object storage that holds
columnar files written once and partitioned by date, filled from the Kafka topic
by an archive writer, a second consumer group. It is not built in the prototype.

**Alternatives.** Apache Hadoop HDFS was rejected for this case. Its strengths,
the master data set for batch recomputation and distributed processing with
MapReduce (Feick et al., 2018, p. 4), serve the batch layer that ADR-0003
excludes, and a Hadoop cluster is an additional distributed system to operate for
write-once files of a single plant. The recommendation
concerns data that is distributed across wind farms at several locations (Sahal
et al., 2020, p. 147), which a single plant does not have.

**Consequences.** FR12 is deferred, and the limit is stated. The archive is
the piece that would make recomputation beyond the Kafka retention possible.

## ADR-0010: A SQL view for the DWH instead of Talend Open Studio

Date: 2026-10-18. Status: accepted.

**Context.** The existing DWH is loaded once a week by a process that already
exists, and it should receive hourly temperature aggregates per machine
(FR10). Talend Open Studio is the data processing tool named as an example in
the task.

**Decision.** The continuous aggregate per machine and hour is exposed as a
view, which the existing load reads like any relational source. No integration
tool is added.

**Alternatives.** Talend Open Studio was rejected. Qlik retired the open source
version on 31 January 2024, and it is no longer hosted or updated (Qlik, n.d.),
so a new project would start on a product whose maintained successors are
commercial. Independently of that, the transformation is a read of aggregated rows, which a view serves without a separate tool and
without another component to operate.

**Consequences.** The existing load has to be able to read from PostgreSQL. This
is an assumption about the DWH (A7), to be verified with the DWH team before a
pilot.

## ADR-0011: JSON messages with an explicit schema version

Date: 2026-10-18. Status: accepted.

**Context.** Producer, processor, and measurement code share the format of a
reading and an alert. It has to be inspectable during development, extendable
without breaking consumers, and rich enough to compute the latency of every
stage and to detect lost and repeated readings.

**Decision.** Messages are JSON documents in UTF-8 with the fields listed in
`concept.md`, starting with `schema_version`. A reading carries a per machine
`sequence`, so that a loss or a repeat can be counted without relying on clocks.
The key of every message is `machine_id`. Time stamps are ISO 8601 in UTC. The
Kafka topic is configured to use the log append time, so that the broker, and not the producer,
sets the time of arrival, and the processor and the database add their own
timestamps (`processed_at`, `stored_at`).

**Alternatives.** Avro or Protocol Buffers with a schema registry would give a
compact binary format and enforced compatibility. They were rejected because they
add a container, a build step, and a tool to read messages at a load of 12
readings per second where size does not matter. The cost of text is real at
high load, and M2 reports whether it is visible.

**Consequences.** Validation happens in `messages`, which rejects a message with
an unknown version. A change of the format adds a version and does not edit
version 1.

## ADR-0012: At-least-once delivery with idempotent storage

Date: 2026-10-18. Status: accepted.

**Context.** NFR2 requires that no reading is lost and none is stored twice
when the processor is stopped and restarted. Isah et al. (2019, p. 154306) name
three semantics: at-most-once, which may lose a message; at-least-once, which
may deliver it several times; and exactly-once, the most desirable, with more
acknowledgement checks.

**Decision.** The producer sets `acks=all` and enables idempotence, so that
retries do not duplicate a message in the partition. The processor commits its
offset only after the database transaction has committed, which gives
at-least-once delivery to the database. Storing is idempotent: the readings table
has the primary key `machine_id` and `measured_at`, and a second insert of the
same reading is ignored. Alerts carry a deterministic `alert_id`, are stored by
that key, and are published after the commit; a consumer of the alert topic
treats a repeated `alert_id` as the same alert.

**Alternatives.** At-most-once delivery was rejected because a crash would lose
readings, which contradicts NFR2. Exactly-once processing through Kafka
transactions was rejected because it covers records read from Kafka topics and
results written to Kafka topics and state stores (Isah et al., 2019, p. 154312),
and the sink here is a database. Flink offers an end-to-end guarantee only if
the sink takes part in the checkpointing (p. 154312); a PostgreSQL sink would
need a two-phase commit, which is more machinery than an idempotent insert.

**Consequences.** The effect of a restart is that a reading may be processed
twice, and the second processing changes nothing in the database. The state of
the alert rules is not covered by this mechanism and is restored in B5 from the
stored readings and the open alerts. M3 verifies the claim by counting
`sequence` gaps and repeats after a stop and restart under load, and the result
is reported as measured.

## ADR-0013: Docker Compose stack with pinned images and a topic creation job

Date: 2026-10-18 to 2026-10-19. Status: accepted.

**Context.** The stack has to start with one command on a developer machine
(NFR4), every image has to be pinned to an exact version (NFR5), and the tools
have to be free of charge (NFR3). Three services are required, Kafka,
TimescaleDB, and Grafana, and the topics have to exist with their
configuration before the first producer or consumer connects.

**Decision.** `compose.yaml` defines the services `kafka`, `timescaledb`, and
`grafana` and the one-shot job `kafka-init`. The images are `apache/kafka:4.3.1`,
`timescale/timescaledb:2.30.2-pg17`, and `grafana/grafana:13.2.4`, which were the
newest release lines on Docker Hub on 2026-10-18 with a patch release already
published. TimescaleDB runs on PostgreSQL 17, one major version behind the newest
offered, 18, because the former has had a full release cycle. Kafka runs as a single combined broker and
controller in KRaft mode (ADR-0005) with two client listeners, one advertised as
`localhost:9092` for processes on the developer machine and one as
`kafka:29092` for the other containers. Automatic topic creation is switched off,
and `kafka-init` creates `machine-temperature` and `machine-alerts` with the
values of A10 and A11 and the log append time (ADR-0011). Every long running
service has a health check, and `docker compose up -d --wait` returns when all
of them are healthy and the job has exited with status 0. The database check
uses the TCP address, because during the first initialisation the image runs a
temporary server on the socket alone, and a check over the socket would report
healthy before the schema scripts have run. All ports are bound to the loopback
interface, and the credentials are the development defaults documented in
`README.md`.

**Licences.** The licence texts of the pinned versions were checked. The Kafka
image contains the Apache License 2.0 in `/opt/kafka/LICENSE`, and the Grafana
image contains the GNU Affero General Public License version 3 in
`/usr/share/grafana/LICENSE`. The TimescaleDB image does not contain a licence
text, but the running server reports `timescaledb.license = timescale`, which
means that the compression and continuous aggregate features are used under the
Timescale License. Its text, read from the tag 2.30.2 of the project repository
(`tsl/LICENSE-TIMESCALE`, Timescale License Agreement posted 24 September 2020),
grants a licence to use the software "solely for Your own internal business
purposes" and prohibits offering it as database-as-a-service or
software-as-a-service. Self-hosted internal use at no cost, as in this case, is
therefore permitted, and NFR3 is read as free of charge.

**Alternatives.** The `latest` tag was rejected because it makes the stack
unreproducible. A Kafka image from a third party, for instance with a bundled
ZooKeeper, was rejected because the official Apache image supports KRaft
directly. Topic creation by the first producer was rejected because it would
create a topic with the broker defaults and hide a misspelled name. Creating the
topics from a test or a script on the host was rejected because the stack would
not be complete after the single start command. A health check over the socket
for PostgreSQL was rejected for the reason given above.

**Consequences.** The job `kafka-init` is the one container that does not keep
running and shows as exited in `docker compose ps -a`. The initialisation scripts
of the database run only when the data volume is empty, so a change to the
schema takes effect after `docker compose down -v` and a new start. The
Timescale License is not an open source licence, which stays a limit of the
statement that all tools are freely available. The stack has no authentication
between its components and is not suitable for anything but a development
machine.

## ADR-0014: Schema, retention, and compression of the hot storage

Date: 2026-10-18 to 2026-10-19. Status: accepted.

**Context.** The hot storage holds the readings (FR7), the alerts (FR7), and the
hourly aggregates (FR8, FR10). It has to carry the four stage timestamps (NFR8),
reject a redelivered reading (ADR-0012), and stay bounded in size.

**Decision.** The table `readings` is a hypertable partitioned by `measured_at`
with a chunk interval of one day. Its primary key is `machine_id` and
`measured_at`, which makes the insert idempotent and contains the partitioning
column as TimescaleDB requires. The columns `kafka_appended_at`, `processed_at`,
and `stored_at` hold the other three stage timestamps, and `stored_at` defaults
to the time of the insert. The table `alerts` is an append-only log of alert
events, keyed by `alert_id`, `severity`, and `status`, so that a raise, an
escalation from warning to critical, and a closing of one alert are three rows
and a redelivered event is a single row. The continuous aggregate
`readings_hourly` holds minimum, mean, maximum, and count per machine and hour,
refreshes every 15 minutes, and reads the most recent hour from the raw table
because real-time aggregation is switched on. Chunks older than seven days are
compressed, segmented by machine, and raw readings older than 90 days are dropped
(A12). The aggregate is the view that the existing data warehouse reads, as
decided in ADR-0010.

**Alternatives.** An alert table with one row per alert that is updated on every
transition was rejected, because it would lose the course of an incident, which
the demonstration scenario shows from the history, and because an update is not
idempotent in the way an insert with a key is. A plain table instead of a
hypertable was rejected in ADR-0007. Storing the machine type and thresholds in
the database was left out of this item, because the thresholds have one source in
the configuration (concept, message format), and the dashboard will need them
only in B6. A continuous aggregate that is materialised only was rejected,
because the dashboard would then show the current hour with a delay of up to the
refresh interval.

**Consequences.** The primary key rejects a second reading of one machine at the
same instant, which is correct for the simulator and has to hold for any real
sensor as well. The alert event model is a decision about the log and not about
the states, which are specified in B4, and a change of the states may need a new
version of the table. The aggregate has no retention policy, so it grows by one
row per machine and hour. The refresh window of three days has to stay inside
the retention of 90 days, otherwise a refresh would recompute buckets from
dropped chunks.

## ADR-0015: Infrastructure tests without a Kafka or database client

Date: 2026-10-18 to 2026-10-19. Status: accepted.

**Context.** The backlog requires a smoke test of every service and checks of the
topics and the schema, but the Kafka client library is chosen in B3 and the
database driver in B5, and a dependency may only be added with a record that
names it.

**Decision.** The integration tests of B2 use the standard library only. They
test reachability with sockets, Grafana through its HTTP interface with
`urllib`, and Kafka and the database through `docker compose exec` with the
command line tools that are already inside the containers: `kafka-topics.sh`,
the console producer and consumer, and `psql`. The tests that write rows or
topics use names of their own, remove them in a `finally` block, and leave the
stack as they found it. They skip with a message when a port is closed. The
check that every image is pinned and the check that every Python dependency is
pinned are unit tests that read `compose.yaml` and `pyproject.toml` as text.

**Alternatives.** Adding a Kafka client and a PostgreSQL driver in B2 was
rejected because it would settle the choice of the library ahead of the item that
needs it and without the comparison that B3 and B5 owe. Parsing `compose.yaml`
with a YAML library was rejected for the same reason, and a regular expression on
the `image:` lines is sufficient for the check.

**Consequences.** The tests start a process per call, which makes them slower than
a client in the test process, about 7 seconds for 12 tests. They depend on the
container and service names of `compose.yaml`. The tests of B5 and B8 use the
client libraries and may replace parts of these.

## ADR-0016: confluent-kafka as the Kafka client library

Date: 2026-10-19. Status: accepted.

**Context.** The producer of B3 and the processor of B5 need a Kafka client for
Python. The delivery semantics of ADR-0012 require acknowledgement by all
in-sync replicas and an idempotent producer, and the scalability measurement M2
offers up to 100,000 readings per second from a single producer process.

**Decision.** The producer uses `confluent-kafka` 2.16.0, which binds the C
library librdkafka and is distributed under the Apache License 2.0. The
configuration is `acks=all` and `enable.idempotence=true`, and the producer
address family is fixed to IPv4, because `localhost` resolves to the IPv6
address first and the stack listens on IPv4 only, which logged a refused
connection for every client. The batching and linger settings stay at the
defaults of the library, because changing them belongs to the measurements and
must not be done to improve a result. The package is pinned to the exact
version in `pyproject.toml` (NFR5). The processor of B5 uses the same library
unless a measurement or a defect argues against it, and a change is recorded
in a new ADR.

**Alternatives.** `kafka-python` 3.0.11 is a pure Python client and was
published on 2026-08-16, so it is maintained. It was rejected because a client
in C is expected to carry the highest steps of M2 with less CPU time in the
producer process, so that the limit that M2 finds is the limit of the broker and
the database and not of the load generator. This is an expectation and not a
measurement, and it is the first thing to revisit if the producer turns out to
be the bottleneck. `aiokafka` 0.14.0 is built on `asyncio`. It was rejected
because the paced producer loop and the consumer loop of the processor are
sequential, and an event loop would add complexity without a gain at the scale
of the case. The version of `kafka-python-ng`, a fork, was last published on
2024-10-02 and was not considered further.

**Consequences.** The library ships a binary wheel for the platforms of the
developer machine and the continuous integration runner, and a source build would
need librdkafka. The test double of the producer has to follow the signature of
the real `produce` call, which the protocol class in `producer.py` documents.
The partitioner of the library decides which partition a machine identifier
lands on, which matters for the parallelism of consumers (A10).

## ADR-0017: Sensor model with a constant setpoint, white noise, and NumPy

Date: 2026-10-19. Status: accepted.

**Context.** The sensors are simulated, and the simulation has to be
reproducible (FR11), testable against analytic values (NFR7), and able to inject
an overheating (FR3, FR5). The values of the model must not claim more than the
sources support.

**Decision.** A sensor reports the normal temperature of its machine type plus
independent Gaussian noise per reading, with the standard deviation of
assumption A13. An injected overheating adds a rise that is linear in time from
a start time on and has no upper bound. Every sensor draws from a
`numpy.random.Generator` that is passed in. The generators of a simulation are
spawned from one seed with `SeedSequence`, one per machine, so that the readings
of a machine do not change when another machine is added or overheats, and a run
is identical for a seed. NumPy 2.5.4 is added as a dependency, pinned to the
exact version; the architecture of the project names it as the source of random
numbers for the sensor module, and its licence expression in the package
metadata consists of permissive licences. The Kafka client is the only
dependency that the backlog item names, so NumPy is recorded here.

**Alternatives.** An autocorrelated process, for instance a first order
autoregressive process around the setpoint, resembles a real temperature
better, but it needs a correlation parameter for which no source exists, and
the analytic mean and spread of the unit tests would depend on it. Noise on a
slowly varying thermal model of the machine was rejected for the same reason. The
Python standard library `random` module was rejected because a project rule
forbids it and because a global state would couple the machines. A single
generator for the whole plant was rejected, because the draws of one machine
would then depend on the order and the number of the other machines. An
overheating with a ceiling or a cool-down phase was left out, because the
scenario of B7 ends before a ceiling would matter and an unused parameter
would not be tested by the course of the run.

**Consequences.** The normal operation has no daily rhythm or warm-up phase,
which is a simplification that the case study states as a limit. The unit tests
of the mean and the spread use 100,000 samples and tolerances of five standard
errors, derived in the comments of the tests, and a different seed would pass
with a probability above 99.9999 percent. The rate of an injected overheating is
a parameter of the scenario and is fixed in B7, together with the rate limit of
the rule in B4.
