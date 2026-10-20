"""Tables of the measurements, written as Markdown from their raw results.

The tables are computed from the plain structure that a measurement writes to its
JSON file, so that a table of the case study can be rebuilt from the raw output
without running the measurement again (ADR-0024). Every table opens with the
commit, the date, the hardware, and the Docker limits that the result needs.
"""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from statistics import median
from typing import Any

from bladewatch.measure.stats import raw_step_passed

_STAGES = (
    "measured to broker",
    "broker to processor",
    "processor to stored",
    "measured to stored",
)


def _seconds(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def _count(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def _distribution_row(label: str, d: Mapping[str, Any]) -> str:
    return (
        f"| {label} | {_count(d['count'])} | {_seconds(d['median_s'])} | "
        f"{_seconds(d['p95_s'])} | {_seconds(d['p99_s'])} | "
        f"{_seconds(d['maximum_s'])} |"
    )


def _header(title: str, raw: Mapping[str, Any]) -> list[str]:
    environment = raw["environment"]
    return [
        f"# {title}",
        "",
        f"- date: {environment['date']}",
        f"- commit: {environment['commit']}",
        f"- hardware: {environment['hardware']}",
        f"- Docker: {environment['docker']}",
        f"- load profile: {raw['profile']}",
        "",
    ]


def latency_report(raw: Mapping[str, Any]) -> str:
    """Writes the tables of M1.

    Args:
        raw: The structure of the raw result file of M1.
    """
    result = raw["result"]
    lines = _header("M1, latency under the load of the case", raw)
    lines += [
        "## Every reading of every run, in seconds",
        "",
        "| Stage | Readings | Median | 95th percentile | 99th percentile | Maximum |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for stage in _STAGES:
        lines.append(_distribution_row(stage, result["pooled"][stage]))
    alerts = result["alerts"]
    lines += ["", "## Every raised alert event, in seconds", ""]
    if alerts is None:
        lines.append("No alert was raised.")
    else:
        lines += [
            "| Stage | Events | Median | 95th percentile | Maximum |",
            "| --- | --- | --- | --- | --- |",
            f"| measurement to alert in the database | {alerts['count']} | "
            f"{_seconds(alerts['median_s'])} | {_seconds(alerts['p95_s'])} | "
            f"{_seconds(alerts['maximum_s'])} |",
        ]
    lines += [
        "",
        "## Measurement to storage in each run, in seconds",
        "",
        "| Run | Readings | Median | 95th percentile | 99th percentile | Maximum |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for number, stages in enumerate(result["per_run"], start=1):
        lines.append(_distribution_row(str(number), stages["measured to stored"]))
    discarded = result["discarded"]
    if discarded:
        lines += ["", "## Runs that were left out", ""]
        lines += [
            f"- the run of {run['started_at']} was disturbed by a suspension of "
            f"{run['suspended_s']:.0f} seconds and was repeated"
            for run in discarded
        ]
    return "\n".join(lines) + "\n"


def _by_step(steps: Sequence[Mapping[str, Any]]) -> dict[int, list[Mapping[str, Any]]]:
    grouped: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
    for step in steps:
        grouped[step["offered_per_s"]].append(step)
    return dict(sorted(grouped.items()))


def _partition_share(steps: Sequence[Mapping[str, Any]]) -> str:
    shares = []
    for step in steps:
        counts = step.get("partition_messages") or {}
        total = sum(counts.values())
        if total:
            shares.append(max(counts.values()) / total)
    return f"{100 * median(shares):.1f} percent" if shares else "n/a"


def _profile_table(steps: Sequence[Mapping[str, Any]]) -> list[str]:
    failed = [
        step for step in steps if not raw_step_passed(step) and step.get("profile")
    ]
    if not failed:
        return []
    step = max(failed, key=lambda s: s["offered_per_s"])
    lines = [
        "",
        f"## Stage times over the step of {_count(step['offered_per_s'])} readings per "
        f"second, run {step['repetition']}, in seconds",
        "",
        "The readings are grouped by the time of their measurement, in stretches of "
        "10 seconds from the first one.",
        "",
        "| Stretch from | Readings | Broker to processor, median "
        "| Broker to processor, 95th percentile | Processor to storage, median "
        "| Processor to storage, 95th percentile |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for bucket in step["profile"]:
        queue = bucket["stages"]["broker to processor"]
        write = bucket["stages"]["processor to stored"]
        lines.append(
            f"| {bucket['start_s']:.0f} | {_count(bucket['readings'])} | "
            f"{_seconds(queue['median_s'])} | {_seconds(queue['p95_s'])} | "
            f"{_seconds(write['median_s'])} | {_seconds(write['p95_s'])} |"
        )
    return lines


def throughput_report(raw: Mapping[str, Any]) -> str:
    """Writes the tables of M2.

    Args:
        raw: The structure of the raw result file of M2.
    """
    result = raw["result"]
    lines = _header("M2, throughput", raw)
    lines += [
        "## Every step, over the runs of the ramp",
        "",
        "| Offered per second | Runs | Delivered per second | Stored per second "
        "| 95th percentile of measurement to storage in seconds "
        "| Largest lag in messages | Seconds to drain "
        "| Share of the largest partition | Verdict |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for offered, steps in _by_step(result["steps"]).items():
        duration = raw["settings"]["step_duration_s"]
        delivered = median(s["delivered"] / duration for s in steps)
        achieved = median(s["achieved_per_s"] for s in steps)
        p95s = [
            s["latency"]["measured to stored"]["p95_s"] for s in steps if s["latency"]
        ]
        drains = [s["drain_s"] for s in steps if s["drain_s"] is not None]
        verdicts = sorted({s["outcome"]["reason"] for s in steps})
        lines.append(
            f"| {_count(offered)} | {len(steps)} | {_count(delivered)} | "
            f"{_count(achieved)} | "
            f"{_seconds(median(p95s)) if p95s else 'n/a'} "
            f"(largest {_seconds(max(p95s)) if p95s else 'n/a'}) | "
            f"{_count(max(s['max_lag'] for s in steps))} | "
            f"{_seconds(median(drains)) if drains else 'n/a'} | "
            f"{_partition_share(steps)} | {'; '.join(verdicts)} |"
        )
    lines += _profile_table(result["steps"])
    lines += ["", "## Where each ramp stopped", ""]
    for repetition, limit in result["limits"].items():
        text = (
            "every step passed"
            if limit is None
            else f"the first failing step offered {_count(limit)} readings per second"
        )
        lines.append(f"- run {repetition}: {text}")
    return "\n".join(lines) + "\n"


def faults_report(raw: Mapping[str, Any]) -> str:
    """Writes the table of M3.

    Args:
        raw: The structure of the raw result file of M3.
    """
    lines = _header("M3, fault tolerance", raw)
    lines += [
        "## Every run, readings and seconds",
        "",
        "| Stop | Run | Sent | Stored | Missing | Stored twice | Received again "
        "| Backlog at the restart | Seconds to the first commit "
        "| Seconds to catch up | Seconds to drain |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run in raw["result"]:
        lines.append(
            f"| {run['mode']} | {run['repetition']} | {_count(run['delivered'])} | "
            f"{_count(run['distinct'])} | {_count(run['missing'])} | "
            f"{_count(run['duplicated'])} | {_count(run['redelivered'])} | "
            f"{_count(run['backlog_at_restart'])} | "
            f"{_seconds(run['first_commit_s'])} | {_seconds(run['catch_up_s'])} | "
            f"{_seconds(run['drain_s'])} |"
        )
    return "\n".join(lines) + "\n"
