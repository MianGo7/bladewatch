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
`docs/requirements.md`.

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

## Tools and licences

| Tool | Version | Licence |
| --- | --- | --- |
| Apache Kafka | 4.3.1 | Apache License 2.0 |
| TimescaleDB | 2.30.2 on PostgreSQL 17 | Timescale License for compression and continuous aggregates, free for self-hosted internal use |
| Grafana | 13.2.4 | GNU Affero General Public License 3 |
| Python | 3.13 | Python Software Foundation License |
| pytest | 9.1.1 | MIT |
| Ruff | 0.17.1 | MIT |

The licences of the three services were read in the pinned images and, for
TimescaleDB, in the licence file of the version in its repository. The licences
of pytest and Ruff are taken from their package metadata. The reasoning is in
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

The tests marked `integration` need the running container stack and are run
with `uv run pytest -m integration`. Continuous integration runs the format
check, the static checks, and the unit tests on every push and pull request.
