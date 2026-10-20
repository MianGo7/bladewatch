# BladeWatch

BladeWatch is a prototype of a Big Data architecture for the real-time
temperature monitoring of production machines at RotorWind GmbH, a fictional
manufacturer of rotor blades for wind turbines in Emden. Simulated temperature
sensors publish readings to Apache Kafka, a stream processor checks every
reading against a warning and a critical threshold and raises alerts, the
readings and alerts are stored in TimescaleDB for historical analysis, and a
Grafana dashboard shows the state of every machine in real time.

The prototype is the practical part of a case study for the course Data
Analytics and Big Data (DLBINGDABD01_E) at IU International University of
Applied Sciences. It belongs to Task 2, which asks for a suitable system
architecture, a freely available Big Data tool for every component, an example
that demonstrates the task of a tool within the architecture, and a prototype
based on that example. The author is Mian Gohar Ehsan.

The concept, the requirements, the architecture decisions, and the
development journal are in `docs/`. The work is organised in the backlog items
of `docs/backlog.md`, and the status of every requirement is traced in
`docs/requirements.md`, which also lists the limits of the prototype. The diagrams
are the PlantUML sources in `docs/diagrams/`, and the tables and figures of the
measurements are in `docs/figures/`.

## Running the demonstration from a clean clone

The demonstration needs Docker with Compose and [uv](https://docs.astral.sh/uv/), which
installs the locked dependencies and the Python version of the project. In a clone of
the repository:

    uv sync                                          # install the locked dependencies
    docker compose up -d --wait                      # Kafka, TimescaleDB, Grafana
    uv run python -m bladewatch scenario --reset     # the demonstration, about 25 minutes

Open `http://localhost:3000/d/bladewatch-overview/bladewatch-machine-monitoring` while the
scenario runs (user `admin`, password `bladewatch`). All twelve machines are normal for the
first 100 seconds, then `mould-01` overheats, a warning for the rise appears after about two
minutes, the critical alert after about six and a half minutes, and the terminal prints the
notification that Grafana sends. With `--repetitions 1` the demonstration takes about eight
minutes. It writes the results of the measurement M4 to `results/`. `docker compose down`
stops the stack and keeps the data, and `docker compose down -v` also removes it.

## Running the stack

`docker compose up -d --wait` starts Apache Kafka, TimescaleDB, and Grafana and
returns when every service is healthy. A one-shot job creates the topics
`machine-temperature` and `machine-alerts` and then exits with status 0, so it
shows as exited in `docker compose ps -a`. The database schema is created on the
first start with an empty volume; `docker compose down -v` removes the volumes
for a clean start.

| Service | Address on the host | Development credentials |
| --- | --- | --- |
| Apache Kafka | `localhost:9092` | none |
| TimescaleDB | `localhost:5432`, database `bladewatch` | user `bladewatch`, password `bladewatch` |
| Grafana | `http://localhost:3000` | user `admin`, password `bladewatch` |

All ports are bound to the loopback interface, and the credentials are
development defaults that protect nothing beyond the machine they run on.

## Dashboard and notification

The dashboard is provisioned from `infra/grafana/dashboards/bladewatch.json` and
is at
`http://localhost:3000/d/bladewatch-overview/bladewatch-machine-monitoring`. It
shows data once the processor has run, because the processor writes the machines
and their thresholds to the database when it starts. A critical alert is also sent
by the contact point of Grafana to a web hook on port 8089. The receiver is started
with `uv run python -m bladewatch notify` and writes each notification to the
terminal; on a Linux host it has to be started with `--host 0.0.0.0` so that the
container can reach it.

## Demonstration scenario

`uv run python -m bladewatch scenario --reset` runs the demonstration scenario for the
steering committee with one command. It starts the processor and the notification
receiver, lets all machines run normally for 100 seconds, then lets `mould-01` overheat
at 5 degrees Celsius per minute for the rest of 460 seconds, and measures the time from
the reading that reaches the critical threshold to the alert in the database, on the
dashboard, and in the notification. It runs three times in a row and writes the results
of the measurement M4 to `results/`. Without `--reset` the command refuses to start when
the topics or the tables already hold data of the scenario, and names what `--reset`
would delete.

## Measurements

The latency, throughput, and fault tolerance measurements run against the started stack
and write a raw result to `results/`:

    uv run python -m bladewatch measure latency --reset      # M1, three runs of 10 minutes
    uv run python -m bladewatch measure throughput --reset   # M2, the ramp of load steps
    uv run python -m bladewatch measure faults --reset       # M3, the processor is stopped
    uv run python -m bladewatch measure figures results/m2-<stamp>.json

Without `--reset` a measurement refuses to start when the topics or the load streams
hold data. The ramp takes about 20 minutes per run and all three measurements about two
hours, during which the machine should not be used for other work.

## Tools and licences

| Tool | Version | Licence |
| --- | --- | --- |
| Apache Kafka | 4.3.1 | Apache License 2.0 |
| TimescaleDB | 2.30.2 on PostgreSQL 17 | Timescale License for compression and continuous aggregates, free for self-hosted internal use |
| Grafana | 13.2.4 | GNU Affero General Public License 3 |
| Python | 3.13 | Python Software Foundation License |
| Matplotlib | 3.11.2 | Matplotlib License, based on the Python Software Foundation License |
| confluent-kafka | 2.16.0 | Apache License 2.0 |
| NumPy | 2.5.4 | BSD 3-Clause, with permissive licences for parts of the distribution |
| psycopg | 3.3.6 | GNU Lesser General Public License 3.0 |
| pytest | 9.1.1 | MIT |
| Ruff | 0.17.1 | MIT |

The licences of the three services were read in the pinned images and, for
TimescaleDB, in the licence file of the version in its repository. The licences
of the Python packages are taken from their package metadata. The reasoning is in
ADR-0013 of `docs/decision-log.md`.

## Prerequisites

The project is developed with Python 3.13 and managed with
[uv](https://docs.astral.sh/uv/). The container stack additionally needs Docker
with Compose.

## Commands

    uv sync                                   # install the locked dependencies
    uv run pytest -m "not integration"        # unit tests, no Docker needed
    uv run ruff format --check                # code style
    uv run ruff check                         # static checks
    uv run python -m bladewatch --help        # the entry points
    uv run python -m bladewatch produce --duration-s 60   # publish readings for a minute
    uv run python -m bladewatch process       # consume, apply the rules, store
    uv run python -m bladewatch notify        # receive and log Grafana notifications
    uv run python -m bladewatch scenario --reset   # the demonstration scenario
    uv run python -m bladewatch measure latency --reset   # the measurements, see above
    plantuml -tsvg -o out docs/diagrams/*.puml        # render the diagrams, needs PlantUML

The tests marked `integration` need the running container stack and are run
with `uv run pytest -m integration`. Continuous integration runs the format
check, the static checks, and the unit tests on every push and pull request.
