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

## Prerequisites

The project is developed with Python 3.13 and managed with
[uv](https://docs.astral.sh/uv/). The container stack that follows in a later
backlog item additionally needs Docker with Compose.

## Commands

    uv sync                                   # install the locked dependencies
    uv run pytest -m "not integration"        # unit tests, no Docker needed
    uv run ruff format --check                # code style
    uv run ruff check                         # static checks

The tests marked `integration` need the running container stack and are run
with `uv run pytest -m integration`. Continuous integration runs the format
check, the static checks, and the unit tests on every push and pull request.
