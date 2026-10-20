"""Figures of the measurements, drawn from their raw results with Matplotlib.

The only module that imports Matplotlib. The figures are black on white, labelled in
English with units on every axis, set in Arial where it is installed, and saved as
PDF and as PNG at 300 dots per inch (ADR-0025). The drawing works on the plain
structure of the raw result files, so that a figure is rebuilt without a new run.
"""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import matplotlib as mpl
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.ticker import NullFormatter, ScalarFormatter

_STAGES = (
    "measured to broker",
    "broker to processor",
    "processor to stored",
    "measured to stored",
)
_MARKERS = {"median_s": "o", "p95_s": "s", "p99_s": "^", "maximum_s": "x"}
_LABELS = {
    "median_s": "median",
    "p95_s": "95th percentile",
    "p99_s": "99th percentile",
    "maximum_s": "maximum",
}
_LINES = ("-", "--", ":", "-.")
_DPI = 300


def _style() -> None:
    mpl.rcParams.update(
        {
            "font.family": ["Arial", "DejaVu Sans"],
            "font.size": 9,
            "axes.edgecolor": "black",
            "axes.labelcolor": "black",
            "text.color": "black",
            "xtick.color": "black",
            "ytick.color": "black",
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.frameon": False,
            "lines.color": "black",
        }
    )


def _plain_log(axis: Axes, which: str = "xy") -> None:
    """Writes the ticks of a logarithmic axis as plain numbers, not as powers."""
    for name in which:
        target = axis.xaxis if name == "x" else axis.yaxis
        target.set_major_formatter(ScalarFormatter())
        target.set_minor_formatter(NullFormatter())


def latency_figure(raw: Mapping[str, Any]) -> Figure:
    """Draws M1: the distribution of every stage and the course of the whole path.

    Args:
        raw: The structure of the raw result file of M1.
    """
    _style()
    result = raw["result"]
    figure = Figure(figsize=(9.0, 3.6))
    left, right = figure.subplots(1, 2)
    for row, stage in enumerate(_STAGES):
        distribution = result["pooled"][stage]
        for key, marker in _MARKERS.items():
            left.plot(
                distribution[key],
                row,
                marker,
                color="black",
                markerfacecolor="white" if marker != "x" else "black",
                label=_LABELS[key] if row == 0 else None,
            )
    left.set_yticks(range(len(_STAGES)), _STAGES)
    left.invert_yaxis()
    left.set_xscale("log")
    _plain_log(left, "x")
    left.set_xlabel("Time in seconds (logarithmic scale)")
    left.set_title("Every stage, all readings", loc="left", fontsize=9)
    left.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=4)
    for number, run in enumerate(result["runs"], start=1):
        values = sorted(run["samples"]["measured to stored"])
        fractions = [(i + 1) / len(values) for i in range(len(values))]
        right.step(
            values,
            fractions,
            color="black",
            linestyle=_LINES[(number - 1) % len(_LINES)],
            label=f"run {number}",
            where="post",
        )
    right.set_xscale("log")
    _plain_log(right, "x")
    right.set_xlabel("Measurement to storage in seconds (logarithmic scale)")
    right.set_ylabel("Share of readings")
    right.set_title("Cumulative distribution", loc="left", fontsize=9)
    right.legend(loc="lower right")
    figure.tight_layout()
    return figure


def _step_series(
    steps: Sequence[Mapping[str, Any]], value: str
) -> dict[int, list[tuple[int, float]]]:
    series: dict[int, list[tuple[int, float]]] = {}
    for step in steps:
        series.setdefault(step["repetition"], []).append(
            (step["offered_per_s"], _read(step, value))
        )
    return series


def _read(step: Mapping[str, Any], value: str) -> float:
    if value == "p95":
        latency = step["latency"].get("measured to stored")
        return float("nan") if latency is None else latency["p95_s"]
    return float(step[value])


def throughput_figure(raw: Mapping[str, Any]) -> Figure:
    """Draws M2: achieved rate, latency, lag, and the lag over time of the last step.

    Args:
        raw: The structure of the raw result file of M2.
    """
    _style()
    steps = raw["result"]["steps"]
    target = raw["settings"]["latency_target_s"]
    figure = Figure(figsize=(9.0, 6.4))
    (rate, latency), (lag, course) = figure.subplots(2, 2)
    for repetition, points in _step_series(steps, "achieved_per_s").items():
        style = _LINES[(repetition - 1) % len(_LINES)]
        offered = [p[0] for p in points]
        rate.plot(
            offered,
            [p[1] for p in points],
            style,
            marker="o",
            color="black",
            markerfacecolor="white",
            label=f"run {repetition}",
        )
    top = max(s["offered_per_s"] for s in steps)
    rate.plot(
        [10, top],
        [10, top],
        color="gray",
        linewidth=0.8,
        label="achieved equals offered",
    )
    rate.set_xscale("log")
    rate.set_yscale("log")
    _plain_log(rate)
    rate.set_xlabel("Offered readings per second")
    rate.set_ylabel("Stored readings per second")
    rate.legend(loc="upper left")
    for repetition, points in _step_series(steps, "p95").items():
        style = _LINES[(repetition - 1) % len(_LINES)]
        latency.plot(
            [p[0] for p in points],
            [p[1] for p in points],
            style,
            marker="s",
            color="black",
            markerfacecolor="white",
            label=f"run {repetition}",
        )
    latency.axhline(target, color="gray", linewidth=0.8)
    latency.annotate(
        f"target {target:g} s",
        (10, target),
        textcoords="offset points",
        xytext=(2, 3),
        fontsize=8,
    )
    latency.set_xscale("log")
    latency.set_yscale("log")
    _plain_log(latency)
    latency.set_xlabel("Offered readings per second")
    latency.set_ylabel("95th percentile of measurement\nto storage in seconds")
    for repetition, points in _step_series(steps, "max_lag").items():
        style = _LINES[(repetition - 1) % len(_LINES)]
        lag.plot(
            [p[0] for p in points],
            [max(p[1], 1.0) for p in points],
            style,
            marker="^",
            color="black",
            markerfacecolor="white",
        )
    lag.set_xscale("log")
    lag.set_yscale("log")
    _plain_log(lag)
    lag.set_xlabel("Offered readings per second")
    lag.set_ylabel("Largest lag in messages")
    last = steps[-1]
    # The lag is drawn to the end of the load. Afterwards no message arrives, and
    # the backlog drains within the seconds to drain of the table (ADR-0026).
    shown = [s for s in last["samples"] if s[0] <= last["produce_s"]]
    course.plot([s[0] for s in shown], [s[1] - s[2] for s in shown], color="black")
    course.set_xlabel("Seconds since the first message")
    course.set_ylabel("Lag in messages")
    course.set_title(
        f"Step of {last['offered_per_s']:,} readings per second, run "
        f"{last['repetition']}, to the end of the load",
        loc="left",
        fontsize=9,
    )
    figure.tight_layout()
    return figure


def failing_step(raw: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Returns the failing step with the largest load that has a profile, or None."""
    failed = [
        step
        for step in raw["result"]["steps"]
        if not step["outcome"]["passed"] and step.get("profile")
    ]
    return max(failed, key=lambda step: step["offered_per_s"], default=None)


def step_profile_figure(raw: Mapping[str, Any]) -> Figure | None:
    """Draws the stage times over a failing step, or None if no step failed.

    The readings are grouped by the time of their measurement, so that a stage
    that slows down at some moment of the step shows at that moment.

    Args:
        raw: The structure of the raw result file of M2.
    """
    step = failing_step(raw)
    if step is None:
        return None
    _style()
    figure = Figure(figsize=(9.0, 3.6))
    axes = figure.subplots(1, 2, sharey=True)
    stages = ("broker to processor", "processor to stored")
    for axis, stage in zip(axes, stages, strict=True):
        starts = [b["start_s"] for b in step["profile"]]
        for key, marker, style in (("median_s", "o", "-"), ("p95_s", "s", "--")):
            axis.plot(
                starts,
                [b["stages"][stage][key] for b in step["profile"]],
                style,
                marker=marker,
                color="black",
                markerfacecolor="white",
                label=_LABELS[key],
            )
        axis.set_yscale("log")
        _plain_log(axis, "y")
        axis.set_xlabel("Seconds from the first measurement")
        axis.set_title(f"Stage {stage}", loc="left", fontsize=9)
        axis.legend(loc="upper left")
    axes[0].set_ylabel("Time of the stage in seconds (logarithmic scale)")
    figure.suptitle(
        f"Step of {step['offered_per_s']:,} readings per second, run "
        f"{step['repetition']}, readings by the time of their measurement",
        fontsize=9,
        x=0.01,
        ha="left",
    )
    figure.tight_layout()
    return figure


def faults_figure(raw: Mapping[str, Any]) -> Figure:
    """Draws M3: the lag over time for each way of stopping the processor.

    Args:
        raw: The structure of the raw result file of M3.
    """
    _style()
    runs = raw["result"]
    modes = ["killed", "finished"]
    figure = Figure(figsize=(9.0, 3.6))
    axes = figure.subplots(1, 2, sharey=True)
    for axis, mode in zip(axes, modes, strict=True):
        selected = [run for run in runs if run["mode"] == mode]
        for run in selected:
            style = _LINES[(run["repetition"] - 1) % len(_LINES)]
            axis.plot(
                [s[0] for s in run["samples"]],
                [s[1] - s[2] for s in run["samples"]],
                style,
                color="black",
                label=f"run {run['repetition']}",
            )
        if selected:
            axis.axvline(selected[0]["stop_s"], color="gray", linewidth=0.8)
            axis.axvline(selected[0]["restart_s"], color="gray", linewidth=0.8)
            axis.annotate(
                "processor stopped",
                (selected[0]["stop_s"], 0),
                rotation=90,
                textcoords="offset points",
                xytext=(-8, 4),
                fontsize=8,
            )
            axis.annotate(
                "processor started",
                (selected[0]["restart_s"], 0),
                rotation=90,
                textcoords="offset points",
                xytext=(-8, 4),
                fontsize=8,
            )
        axis.set_title(f"Processor {mode}", loc="left", fontsize=9)
        axis.set_xlabel("Seconds since the start of the load")
        axis.legend(loc="center right")
    axes[0].set_ylabel("Lag in messages")
    figure.tight_layout()
    return figure


def save(figure: Figure, directory: Path, name: str) -> list[Path]:
    """Saves a figure as PDF and as PNG at 300 dots per inch.

    Args:
        figure: The figure.
        directory: The target directory, which is created if needed.
        name: The file name without the suffix.

    Returns:
        The paths of the written files.
    """
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for suffix in ("pdf", "png"):
        path = directory / f"{name}.{suffix}"
        figure.savefig(path, dpi=_DPI)
        paths.append(path)
    return paths
