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

**Next.** B2, the infrastructure with Docker Compose, including the check of
the licence texts of the pinned images and the partition count and retention
of the topics. Before the first push, the workflow has to be seen running on
GitHub.
