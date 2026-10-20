# Development journal

The chronological record of the work. It is the main source for the
description of the approach and for the critical reflection in the case
study, so problems are recorded as they happened, including the ones that
turned out to be the author's own mistakes. Every entry carries the date on
which the work was done.

Template for every entry:

    ## YYYY-MM-DD

    **Worked on.** What was done, with backlog item ids.

    **Decisions.** What was decided and why, with ADR numbers where one exists.

    **Problems.** What went wrong, how it was noticed and how it was solved.

    **Next.** What comes next.

---

## 2026-10-18

**Worked on.** B0, the repository and project set up. A uv project for Python
3.13 with the package `bladewatch` in a `src/` layout, pytest and Ruff as the
only dependencies, the `integration` marker, a first test that compares the
package version with the project metadata, a continuous integration workflow
in `.github/workflows/ci.yaml`, and the `README.md`.

B1, the requirements, the architecture, and the tool selection, without code.
The literature collected for the case study was read for the pages that are
cited: the curing cycles of Lux et al. (2020) for the temperatures of A4, the
comparison of Lambda and Kappa by Kiran et al. (2015) and Feick et al.
(2018), the survey of stream processing frameworks by Isah et al. (2019), the
requirements mapping of Sahal et al. (2020), and the time series benchmark of
Mostafa et al. (2022). The result is the revised `concept.md` with fixed
assumptions, ADR-0003 to ADR-0012, a revised `requirements.md` in which every
requirement names its item and its check, and the diagrams
`architecture-overview.puml` and `reading-to-alert-sequence.puml`, both
rendered with PlantUML without errors.

B2, the infrastructure with Docker Compose, implemented and verified in the
evening. `compose.yaml` with Apache Kafka in KRaft mode, TimescaleDB, and
Grafana, each pinned to an exact tag and with a health check, and the one-shot
job `kafka-init` that creates the topics `machine-temperature` (6 partitions)
and `machine-alerts` (3 partitions) with seven day retention and the log append
time. The schema scripts in `infra/timescaledb/`: the hypertable `readings`, the
table `alerts`, the continuous aggregate `readings_hourly`, and the compression
and retention policies. The Grafana provisioning of the TimescaleDB data source
and of the dashboard provider, with an empty dashboard directory until B6.
Twelve integration tests and four unit tests, and the sections on running the
stack and on the tools and licences in `README.md`.

**Decisions.** Task 2 was selected out of the three tasks of the course. Task
1 bundles about eight separate analysis requirements, a time series built from
snapshot data, and topic modelling of multilingual reviews into 7 to 10 pages
with a limit on figures. Task 3 is purely conceptual and has no
implementation to show. Task 2 has a closed scope of four items, can be
demonstrated and measured, and matches the author's background as a software
developer. The toolchain is recorded in ADR-0001 and ADR-0002. The versions
of the two actions and of uv in the workflow were taken from the tags of the
upstream repositories, not from memory.

The thresholds of A4 and the load of A9 were fixed by the author. The mould
values 70, 80, and 95 degrees Celsius rest on the hold temperature of the
standard cycle and on the mould temperatures of the published cycle (Lux et
al., 2020, pp. 400 and 405), and the critical value is explicitly an
interpretation: a reading above the highest mould temperature used anywhere in
the published cycle lies outside every published operating point. The oven and
the grinding machine are labelled as assumptions throughout. The 100 degrees
Celsius of the laboratory oven in the same source is used only as a plausibility
check, because it is not a production post-curing oven. A9 is a stepped ramp
from 12 to 100,000 readings per second with a plant-size reference of 2,000
readings per second and a stop rule, so that the limit of the test machine is
found and not extrapolated. The architectural style is Kappa (ADR-0003), the
processor is a plain Python consumer (ADR-0006), and the delivery is at least
once with idempotent storage (ADR-0012). The margin for closing an alert (FR4)
and the window and limit of the rate of rise (FR5) are deliberately left to
B4, where the rules are written, and the requirements are worded so that they
are testable with configured values.

ADR-0013 records the stack, the image versions, the listeners, the topic
creation job, the health checks, and the licence check. ADR-0014 records the
schema, in particular the alert table as a log of events and the refresh window
of the aggregate inside the retention. ADR-0015 records that the tests of this
item use only the standard library and `docker compose exec`, so that the choice
of the Kafka client in B3 and of the database driver in B5 is not made early.
The partition counts, the topic retention, and the chunking, compression, and
retention of the readings are new assumptions A10 to A12 in `concept.md`. FR8,
FR10, NFR3, NFR4, and NFR5 are marked done, and FR2 is partly met because the
keying by machine follows with the producer in B3.

**Problems.** The Python interpreter of macOS is version 3.9, which is why the
interpreter is managed by uv. The first test run failed because
`pyproject.toml` already named a `README.md` that did not exist yet; the file
was written before the build was repeated. Neither `actionlint` nor the GitHub
command line tool is installed, so the workflow is only verified as well-formed
YAML and has not run yet. A static type checker is not part of the checks, which
is noted as a limit in ADR-0002.

The literature contradicted the intended result in one place. Sahal et al.
(2020, pp. 147 and 148) propose RabbitMQ and not Kafka for the wind turbine use
case, because it supports federated queues across wind farms (requirement R6.3).
The recommendation was not ignored but weighed in ADR-0005: the requirement
does not apply to a single plant, and the need for retained history and replay
favours a log. The same source recommends HDFS and column stores, which are
answered in ADR-0009 and ADR-0007. The SciTS benchmark shows that InfluxDB
ingests faster than TimescaleDB (Mostafa et al., 2022, p. 6), so the storage
decision rests on SQL access and not on speed, and ADR-0007 says so.
Two product facts are not in the peer reviewed literature and come from vendor
pages, of which a PDF copy was kept for the final check: the retirement of Talend Open
Studio on 31 January 2024, stated on the Qlik product page, and the limits of
Splunk Free of 500 megabytes per day and no alerting, stated in the Splunk
Enterprise Admin Manual version 9.4. The Splunk limits were first taken from the
documentation of version 7.0.6, which could not be fetched a second time, and
were then verified against the 9.4 page, whose wording is the one quoted in
ADR-0008. Feick et al. (2018) carries no printed proceedings page range in
the file, so its pages are cited as numbered in the paper itself. The
Timescale License of the compression and continuous aggregate features
means that NFR3 has to be read as free of charge, and the licence text of the
pinned version has to be checked in B2. The limits of the design are recorded
where they arise: no protocol conversion or edge buffering (ADR-0004), no
framework support for state and windows (ADR-0006), recomputation only within
the Kafka retention (ADR-0003), and a database that is polled and not pushed to
(ADR-0008).

Both failures of the first test run of B2 were faults in the tests and not in
the stack. The test of idempotent storage built two different timestamps with
`now()` in two statements and therefore stored two readings, which was noticed
because the count was 2 and not 1. The call to the Grafana interface used a
basic authentication handler that waits for a challenge that Grafana does not
send, so the call received a 401; an explicit header solved it. A first draft of
the tests also contained a dead code fragment and an undefined helper, which were
removed before the first run, and the helpers were moved out of the `conftest.py`
into a module of their own because a relative import from a `conftest.py` does
not work. The TimescaleDB image contains no licence text, so the Timescale
License was read from the repository of the pinned version, and the statement
that NFR3 means free of charge stays a limit. The output of
`docker compose up --wait` reports the finished job `kafka-init` as healthy,
which is correct but easy to misread. No change to the infrastructure was needed
after the first start, which is noted because Grafana 13 is a recent major
version and adjustments to the provisioning were expected.

**Observation.** From an empty state, `docker compose down -v` followed by
`docker compose up -d --wait` brought every service to healthy in 11 seconds,
and the twelve integration tests ran in about 7 seconds. These are single
observations on one machine and not measurements. Docker Desktop was set to 18
CPUs and 7.7 GiB of memory; the hardware is recorded in full before B8.

**Next.** B3, the sensor simulator and the producer, which includes the choice
of the Kafka client library and its record. The distribution of twelve machine
identifiers over six partitions should be looked at when the producer exists,
because hashing does not guarantee an even spread. Before the first push, the
workflow has to be seen running on GitHub.

## 2026-10-19

**Worked on.** Closing of B2. The implementation, the tests, and the first
version of the documents were finished on the evening of 2026-10-18, as recorded
in that entry. This day was used to go through the documents of B2 once more and
to commit the work in separate commits.

B3, the sensor simulator and the producer. The modules `config` (frozen
settings of the plant of A1 to A4 with validation), `sensor` (a constant
setpoint with Gaussian noise and a linear overheating, one seeded generator per
machine), `messages` (the reading with its schema version, validation, and JSON
serialisation), `producer` (a pure generator of readings per tick and a publisher
for Kafka with acknowledgement by all and idempotence), and `cli`
(`python -m bladewatch produce`). The tests were written before each module: 24
for the configuration, 19 for the sensor, 25 for the messages, 17 for the
producer, 3 for the command line, and 4 for the module boundaries, and 5
integration tests that publish to a temporary topic and read it back with a new
consumer group.

B4, the alert rules. The module `rules` with the alert state per machine (normal,
warning, critical, recovered), the closing of an alert with a hysteresis margin,
the sliding window of the rate of rise with a least squares slope, and the
events that the processor of B5 will store and publish. `RuleSettings` was added
to `config` for the margin, the window, and the limit. The state diagram
`alert-states.puml` is checked by a test against the allowed transitions in the
code, and 50 tests cover the rules, among them the exact threshold and closing
values, the oscillation around both levels, a deterministic series checked
against an independent least squares fit, normal operation for two hours per
machine type with noise, and the redelivery of readings.

B5, the stream processor and the storage. The module `storage`, which is the only
one that imports the database driver, with one transaction per batch, idempotent
inserts, and the queries that a restart needs. The module `processor`, which
consumes in batches, applies the rules, publishes the alerts, writes the database
transaction, and commits the offsets. The alert message in `messages`, the method
`restore` and the open alert in `rules`, the settings of the database and the
processor in `config`, and the command `python -m bladewatch process`. The unit
tests are 17 for the processor with fake collaborators, and 13 integration tests
for the storage and 6 for the processor, which run against isolated topics, a
consumer group, and machine identifiers of their own and remove them afterwards.
The command was also run once for real with the producer: 60 readings arrived and
were stored exactly once, no alert was raised in normal operation, and the process
stopped cleanly on an interrupt.

**Decisions.** ADR-0013 to ADR-0015 span both days. ADR-0016 selects
confluent-kafka as the client library, with `kafka-python` and `aiokafka` as the
rejected alternatives and the expectation about the load generator named as an
expectation and not as a measurement. ADR-0017 fixes the sensor model: no
autocorrelation, because no source supports a correlation parameter, and an
overheating without a ceiling. NumPy was added as a dependency that the backlog
item does not name, because the architecture of the project makes it the source
of random numbers for the sensor; the ADR says so. The measurement noise is the
new assumption A13 with a value of 0.5 degrees Celsius and no source. FR1 and FR2
are marked done, and NFR7 is partly met until the rules exist in B4.

ADR-0018 records the lifecycle of an alert. The severity is the highest one
reached, so that a critical alert is not downgraded, because a downgrade would
repeat a severity and status under one alert identifier and collide with the key
of the alert table of ADR-0014; the table did not have to change. An alert that a
rise caused closes only after the rate has stayed below the limit for a full
window, because otherwise a rise that stops at 72 degrees Celsius would close the
alert at the next reading. The author fixed the parameters: a margin of 2.0
degrees Celsius (A14), a window of 60 seconds with a limit of 3 degrees Celsius
per minute (A15), and an overheating of 5 degrees Celsius per minute for the
demonstration (A16). The sources are the heating rates of Lux et al. (2020, p.
405) and the sliding windows of Akidau et al. (2015, p. 1794).

ADR-0019 selects psycopg 3 with a statement that unnests arrays for one round trip
per batch, and sets `stored_at` with `clock_timestamp()` instead of `now()`, so
that the delay of a batch is visible. ADR-0020 fixes the order of processing,
the restoration of the rules from the database, the handling of messages that are
not valid, and the single consumer. The batch size of 500 and the wait of 0.2
seconds are the new assumption A17. FR7 and NFR8 are marked done, and NFR2 is
partly met until measurement M3 has run under load in B8.

**Problems.** `uv add` wrote the new dependencies with a lower bound, which the
requirement of exact pins does not allow, and they were pinned with a second
call before anything else was built. Ruff reported the name
`UnsupportedSchemaVersion` of an exception, and the class was renamed to end in
`Error`, instead of disabling the rule. Every connection of the first producer
run logged a refused attempt on the IPv6 address, because `localhost` resolves to
`::1` first and the stack listens on IPv4 only; fixing the address family of the
client removed the noise. The first version of the integration test published and
read the topic once per test and took 25 seconds, which module scoped fixtures
reduced to 10. To check the command line against the real topic, the producer was
run for three seconds against `machine-temperature`. This left 36 readings in the
topic that the processor of B5 would have read, so the topic was deleted and
recreated by the job `kafka-init`, and the stack is in the state it had before. A
test run against the real topic is not repeated for that reason.

Several tests of B4 were wrong before the rules were. One test unpacked a single
event from a series that correctly produced two. Two series were meant to stay
below the warning threshold but climbed above it, because their start values were
too high for the rate and the length, which was noticed because the assertion saw
an event that was not expected. Ruff reported section comments of the form `FR4:
hysteresis` as commented out code, and the comments were rewritten as sentences
instead of silencing the rule. The first draft of the exactness test at the limit
was dropped, because a rate of exactly 3 degrees Celsius per minute depends on
floating point rounding; the tests use rates on both sides of the limit and the
ADR names the imprecision. The slope is recomputed from all 61 readings of the
window per reading, which is simple and correct but may be a visible share of the
processing time in M2.

The most important finding of B5 was a flaw in an earlier decision. ADR-0012 stated
that alerts are published after the database commit. Working through a stop between
the commit and the publication showed that the restarted rules restore the open alert
from the database, ignore the redelivered readings as already seen, and never publish
the event, so the alert would be stored and lost for the consumers of the topic.
ADR-0020 therefore supersedes that order: the alerts are published before the
commit, which can repeat an event after a failed write and is covered by the at least
once semantics, and the integration tests cover both failure cases. The old ADR is
marked as partly superseded and was not rewritten.

All six integration tests of the processor passed on their first run, which was
treated as a reason for doubt and not as a result. Two deliberate defects were put
into the processor to see whether the tests notice them. Without the restoration at
start the restart test failed, and with the offsets committed before the database
write the unit tests and the test of the failed write failed; the processor was
restored byte for byte afterwards. A first draft of the processor converted stored
text into values of an enumeration with inline imports and a lint suppression, which
was replaced by ordinary imports before anything ran, and the first helper of the
processor tests mixed up which fake owned the journal of calls and was rewritten.
`uv add` was first given a version of psycopg that was a guess, 3.3.5, and the
current release 3.3.6 was pinned instead. The table of tools and licences in the
README had no rows for confluent-kafka and NumPy since B3, and they were added with
psycopg. The real run of the command and the integration tests left rows, a topic
with readings, and thirteen empty consumer groups behind; the rows and the topic were
deleted and recreated, the groups were removed, and the tests now delete every
consumer group that they create.

**Observation.** The default partitioner of the client does not spread the
twelve machine identifiers evenly over six partitions. With the seed of the
check and a topic of six partitions the machines landed as follows: partition 0
held `grinder-01` and `grinder-03`, partition 1 held `grinder-04`, `mould-02`,
`mould-03`, `oven-01`, `oven-02`, and `oven-03`, partition 2 held `oven-04`,
partition 4 held `grinder-02` and `mould-04`, partition 5 held `mould-01`, and
partition 3 held none. The result depends on the identifiers and on the
partitioner of the client and not on the hardware or the seed. At the load of the
case a consumer group therefore has work for at most five consumers, and one
consumer of the group would carry half of the machines. This is recorded in A10
and is a point for the discussion of the limits of scaling by partitions, because
a Java client with a different partitioner would place the same keys differently.

**Observation (B4).** In the tests of the rules, a mould that has run normally
for 100 seconds and then rises at 5 degrees Celsius per minute raises the rate of
rise warning 34 seconds after the rise begins, at 72.9 degrees Celsius, and the
warning threshold of 80 would be reached after 120 seconds and the critical
threshold of 95 after 300 seconds, at which the critical alert is raised under
the identifier of the same alert. If a run starts with the rise, the window has to
fill first and the warning follows after 60 seconds at 75 degrees Celsius. These
are properties of the rules for a deterministic series and not measurements of
the system.

**Observation (B5).** In the real run, the 60 stored readings show a difference of
0.055 to 0.559 seconds between the time of measurement and the time of storage. This
is a single observation of a run that started the processor four seconds before the
producer, and it is not a measurement; the latency is measured in B8 with the load
profile, the hardware, and the Docker limits recorded.

**Next.** B6, the dashboard and the alerting: a Grafana dashboard provisioned from a
file, with the current temperature of every machine and its thresholds, a time series
per machine type, the table of open and recent alerts, and the history of a machine from
the continuous aggregate, and a Grafana alert rule with a contact point (A6). The
thresholds are not in the database yet, and the dashboard needs them, so a table or a
view of the machines and their thresholds has to be decided there. The continuous
integration run on GitHub is still outstanding.

## 2026-10-20

**Worked on.** B6, the dashboard and the alerting, with the screenshots of the dashboard in
normal operation. In the
database, the table `machines` and the views `open_alerts` and `machine_status`.
In `storage` and `processor`, the registration of the machines with their thresholds
at start. The dashboard `bladewatch.json` with a table of the state of every machine,
one time series per machine type with the threshold lines, a table of the open
alerts, a table of the alert events of the last 24 hours, and the hourly history of a
selected machine from the continuous aggregate. The provisioned alert rule, contact
point, and notification policy of Grafana, and the Compose settings for the refresh
interval and the host name of the contact point. The new module `notifier`, a
standard library webhook receiver, with the command `python -m bladewatch notify`. The
architecture diagram and the sequence diagram show the notifier and the table
`machines`. The stack was recreated from empty volumes with `docker compose down -v`,
and the dashboard, the data source, the alert rule, the contact point, and the policy
were all present after the start. Sixteen unit tests check the dashboard file, the
compose file, and the provisioning against the configuration and against each other,
eleven unit tests cover the notifier, and 12 integration tests cover the dashboard
and the alerting.

B7, the demonstration scenario and the measurement M4. The module `scenario` and the command
`python -m bladewatch scenario [--reset]`. The command starts the processor with a consumer
group of its own, the notification receiver, and a poller of the dashboard, publishes the
readings of the twelve machines with the overheating of `mould-01` after 100 seconds of
normal operation, waits until every reading is stored, and computes M4 from the stored rows.
It refuses to start on a stack that holds data of the scenario and names what `--reset`
would delete. `storage` gained the scoped row counts, the scoped reset, the alert events with
the timestamps of their reading, and the latency summary, and `config` the settings of the
scenario and of Grafana. The tests are 29 unit tests of the pure parts and the report, 6 for
the rule that detects a suspension of the machine, and 8 integration tests that run the
scenario in a mode without waiting on isolated topics and machines: the alerts of repeated
runs are identical, the refusal and the reset behave as described, and the reset touches only
the machines of the scenario. The measurement was run for real with three repetitions, a
demonstration run followed for the screenshots, and the results are in
`docs/figures/m4-demonstration-scenario.md`.

**Decisions.** ADR-0021 fixes the dashboard from views, with the machines registered by
the processor so that the thresholds keep one source, and ADR-0022 fixes the webhook
notification and the module `notifier`. The package structure of the architecture is
extended by one leaf module that imports only the standard library and `config`, which
a test checks. The new assumptions are A18, a refresh of the dashboard every second,
and A19, an evaluation of the alert rule every 10 seconds. The colours are the first
four slots of the validated categorical palette, one per machine number, checked with
the validation script of the visualisation guidance: the worst adjacent separation for
colour vision deficiency is 9.1, and aqua and yellow fall below 3:1 contrast on a light
surface, which the legend with values and the status table compensate. The screenshots
are captured at the full device resolution in four parts per picture with the page frozen,
joined without scaling, and `docs/figures/README.md` lists the views, their addresses, and
their sizes. FR6 and FR9 are marked done, and B6 is done.

ADR-0023 records the module, the explicit reset, and the definition of M4, and the new
assumption A20 fixes the timeline of the scenario. The reset deletes the messages of the two
topics with the delete records call and the rows of the scenario machines only, so that the
configuration of the topics and the rows of other machines are not touched. Each run waits
until its consumer group is stable before the producer starts, because the first readings
would otherwise carry the start of the processor in their latency. FR11 is marked done and
NFR1 is partly met until the distribution over all readings under load exists in B8.

**Problems.** The dashboard was first seen rendered when the screenshots were prepared,
and the view showed layout faults that the tests of the queries and of the file could not
show. The state table was too short for its twelve rows and showed seven, the legends of
the time series showed three of four machines, six columns of the table did not fit
because Grafana gives a column a minimum width of 150 pixels, and the age of a reading
appeared in milliseconds under a heading in seconds. The panels were enlarged, the tables
were given a narrower minimum width, the type column was dropped, the open alerts were
placed beside the state table so that the first screen fits into one window, and the unit
of the age was changed. The window of the display allows 941 pixels in height, which is why
the control bar is hidden in the pictures. An attempt to scale the page to fit it in
distorted the layout and was dropped. The footer bar of Grafana overlapped the bottom of
the panels, and the first attempt to hide it hid the whole page. The legend of the grinding
machines still clips the descender of one line by about two pixels. The text for an empty table
of open alerts was set with the option for the value of an empty field, `noValue`, and
Grafana 13.2.4 loads it but still shows its own text "No data" for a table without rows,
so the option has no visible effect there; a query that adds a row with the text when no
alert is open would show it, at the cost of a row that is not data, and has not been done.
The option and its test were removed again, because a setting without effect is dead
configuration, and the panel keeps the text of Grafana.
The history panel holds
one point per hour, so after twenty minutes of data it shows two points per series and is of
little use as a figure until hours of data exist. The first version of the panel tests
guessed the shape of the frames that Grafana returns, which is one wide frame with a
field per machine, and the test of the history panel used a window that started one
minute before the data, so that the bucket, which is labelled with the start of its
hour, was left out; the dashboard itself was correct in both cases. Moving the helpers
of the integration tests into a shared module collided with a variable named `ticks`
and with two names that had lost their imports. The first version of the receiver
carried two lint suppressions, one of them for a rule that is not enabled, and the
method that the server looks up by the name `do_POST` conflicts with the naming rule,
so the handler class is built with the name as a string instead of disabling the rule.
The machines that earlier test runs had registered stayed in the table, because only
the tests of this item delete them; they were removed, and the environment of the tests
now cleans that table. Grafana evaluates the alert rule only every 10 seconds, which
bounds the notification delay from below, and a notification of a closing is slower
because of the group interval.

A first attempt of the measurement showed a result that no stage of the pipeline explains. Runs
1 and 2 were normal, with the critical alert in the database 0.2 seconds after its
measurement, but in run 3 every reading from the middle of the run on lay 99.27 seconds
between its measurement and its append at the broker, and the processor stage reached 143
seconds. The run had taken 559 seconds of wall time instead of 460. The cause is a
suspension of the machine: the producer paces itself with the monotonic clock, which does not
advance while a Mac sleeps, so that every later reading was produced 99 seconds after the
wall time that it carries. The power log of the machine was not read, so the sleep is the
conclusion from the clocks and not a proven fact. The run was not averaged in and was not
deleted: a rule was added that is independent of any latency, a run is disturbed when the wall
clock ran two seconds or more longer than the monotonic clock, and a disturbed run is listed in
the report, left out of the statistics, and repeated. The whole measurement was then repeated
under `caffeinate`, and the first attempt stays in the local results directory. Disturbed
runs are a limit of a measurement on a laptop and belong in the reflection.

The window of the browser for the screenshots would not resize in two of the three windows
that were opened, and it was solved by constraining the width of the page by style for the
overview and by opening a new window for the panels. The first look at the data showed
three more layout faults that only an alert could show: the table of open alerts was too
narrow for its columns, its temperature header was cut, and the time column of the recent
events cut the timestamp. They were corrected in the dashboard file, the open alerts table
still cuts a single letter of its header, and the pictures were retaken after the fixes. The
first run for the pictures was kept only as a trial.

**Observation.** In one run of the command with a critical alert inserted into the
database, the notification FIRING arrived 12 seconds later and RESOLVED followed the
closing event after 20 seconds. These are single observations with the stack at its
default settings and not measurements; M4 times the alert in the database and on the
dashboard, and the notification is not part of it.

**Observation (B7, measurement M4).** Date 2026-10-20, working tree of B7 on top of the commit
58b71f1, which the report names as the commit with uncommitted changes. Hardware: Mac17,8 with
an Apple M5 Pro, 48 GiB of memory, macOS 27.0.1. Docker Desktop with the Engine 29.8.2 and the
limits of 18 CPUs and 7.746 GiB of memory, which have not been fixed by hand. Images
apache/kafka 4.3.1, timescale/timescaledb 2.30.2 on PostgreSQL 17, and grafana/grafana
13.2.4. Load profile: twelve machines with one reading per second, which is 12 readings per
second, for 460 seconds per run with the seed 1, three undisturbed runs, each from an empty
state.

The critical alert, in seconds from the measurement of the reading that reached 95 degrees
Celsius, in the three runs: in the database 0.186, 0.213, and 0.214, with a median of 0.213, a
95th percentile of 0.214, and a maximum of 0.214; visible to a query of the dashboard 0.284,
0.247, and 0.251, with a median of 0.251 and a maximum of 0.284; in the notification 28.7, 28.2,
and 27.7. The database stages of the critical alert were 0.012 to 0.016 from the measurement to
the broker, 0.168 to 0.192 from the broker to the processor, and 0.002 to 0.012 from the
processor to the database. Over all 5,520 readings of a run the time from measurement to
storage had a median of 0.121 to 0.127, a 95th percentile of 0.213 to 0.222, and a maximum of
0.537 to 0.567 seconds. The alert events of the three runs are identical: the rate of rise
warning at 72.33 degrees Celsius 134 seconds after the start and the critical alert at 95.25
degrees Celsius after 396 seconds. The dashboard itself shows the alert at its next refresh,
at most one second later.

The notification is the surprising value. It took 27.7 to 28.7 seconds in the three runs, 5.6
and 25.2 seconds in the first two runs of the first attempt, and 2.9 seconds in the
demonstration run, which exceeds the 20 seconds that ADR-0022 expected. The cause has not been
established and the settings were not changed. The three runs are phase locked to the
evaluation cycle of Grafana because they last equally long, so their agreement says little
about the spread of the delay. ADR-0022 is marked as corrected by ADR-0023, and A19 no longer
claims a bound.

**Next.** B8, the measurements M1 to M3, with the recording of the hardware that was done here.
The hourly history screenshot is retaken after the runs of B8 and before the next
`scenario --reset`. The continuous integration run on GitHub has succeeded for the earlier
pushes and is not part of this item.
