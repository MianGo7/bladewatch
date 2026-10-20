from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
from matplotlib.figure import Figure

from bladewatch.measure import MeasureError
from bladewatch.measure.figures import (
    faults_figure,
    latency_figure,
    save,
    step_profile_figure,
    throughput_figure,
)
from bladewatch.measure.render import render
from bladewatch.measure.reports import faults_report, latency_report, throughput_report

ENVIRONMENT = {
    "date": "2026-10-21",
    "commit": "abc1234",
    "hardware": "Mac17,8 with Apple M5 Pro, 48 GiB of memory, macOS 27.0.1",
    "docker": "Docker Engine 29.8.2 with the limits of 18 CPUs and 7.746 GiB of memory",
}


def _distribution(base: float, count: int = 100) -> dict[str, float]:
    return {
        "count": count,
        "median_s": base,
        "p95_s": 2 * base,
        "p99_s": 3 * base,
        "maximum_s": 5 * base,
    }


def latency_raw() -> dict[str, Any]:
    stages = {
        "measured to broker": _distribution(0.01),
        "broker to processor": _distribution(0.1),
        "processor to stored": _distribution(0.002),
        "measured to stored": _distribution(0.12),
    }
    samples = {name: [0.1, 0.2, 0.3, 0.4] for name in stages}
    return {
        "measurement": "M1",
        "environment": ENVIRONMENT,
        "profile": "12 readings per second",
        "settings": {},
        "result": {
            "pooled": stages,
            "alerts": _distribution(0.13, 6),
            "per_run": [stages, stages],
            "runs": [{"samples": samples}, {"samples": samples}],
            "discarded": [{"started_at": "2026-10-21T10:00:00", "suspended_s": 99.0}],
        },
    }


def _stage(base: float) -> dict[str, float]:
    return {"median_s": base, "p95_s": 2 * base, "maximum_s": 3 * base}


def _profile() -> list[dict[str, Any]]:
    return [
        {
            "start_s": start,
            "readings": 250000,
            "stages": {
                "broker to processor": _stage(0.1 * (1 + start / 10)),
                "processor to stored": _stage(0.004),
            },
        }
        for start in (0.0, 10.0, 20.0)
    ]


def _step(offered: int, repetition: int, passed: bool = True) -> dict[str, Any]:
    return {
        "offered_per_s": offered,
        "repetition": repetition,
        "delivered": offered * 120,
        "achieved_per_s": float(offered),
        "latency": {
            "measured to stored": {"median_s": 0.1, "p95_s": 0.3, "maximum_s": 1.0}
        },
        "max_lag": 900,
        "partition_messages": {"0": offered * 60, "1": offered * 60},
        "drain_s": 1.5,
        "outcome": {"reason": "passed" if passed else "lag growing", "passed": passed},
        "produce_s": 1.5,
        "profile": _profile(),
        "samples": [
            (0.0, 0, 0),
            (1.0, offered, offered - 50),
            (2.0, 2 * offered, 2 * offered - 60),
        ],
    }


def throughput_raw() -> dict[str, Any]:
    steps = [_step(12, 1), _step(100, 1), _step(500, 1, passed=False), _step(12, 2)]
    return {
        "measurement": "M2",
        "environment": ENVIRONMENT,
        "profile": "steps",
        "settings": {"step_duration_s": 120.0, "latency_target_s": 5.0},
        "result": {"steps": steps, "limits": {"1": 500, "2": None}},
    }


def _fault(mode: str, repetition: int) -> dict[str, Any]:
    return {
        "mode": mode,
        "repetition": repetition,
        "delivered": 240000,
        "distinct": 240000,
        "missing": 0,
        "duplicated": 0,
        "redelivered": 500 if mode == "killed" else 0,
        "backlog_at_restart": 60000,
        "first_commit_s": 12.5,
        "catch_up_s": None,
        "drain_s": 3.25,
        "stop_s": 30.0,
        "restart_s": 60.0,
        "samples": [(0.0, 0, 0), (30.0, 60000, 60000), (60.0, 120000, 60000)],
    }


def faults_raw() -> dict[str, Any]:
    runs = [_fault(m, n) for n in (1, 2) for m in ("killed", "finished")]
    return {
        "measurement": "M3",
        "environment": ENVIRONMENT,
        "profile": "2000 readings per second",
        "settings": {},
        "result": runs,
    }


@pytest.mark.parametrize(
    ("report", "raw"),
    [
        (latency_report, latency_raw()),
        (throughput_report, throughput_raw()),
        (faults_report, faults_raw()),
    ],
)
def test_every_report_names_the_commit_the_date_the_hardware_and_the_limits(
    report: Callable[[Mapping[str, Any]], str], raw: Mapping[str, Any]
) -> None:
    text = report(raw)

    for value in ENVIRONMENT.values():
        assert value in text


def test_the_latency_report_has_a_row_for_every_stage_with_the_four_statistics() -> (
    None
):
    text = latency_report(latency_raw())

    assert "| measured to broker | 100 | 0.010 | 0.020 | 0.030 | 0.050 |" in text
    assert "| measured to stored | 100 | 0.120 | 0.240 | 0.360 | 0.600 |" in text


def test_the_latency_report_lists_the_alert_events_and_the_discarded_runs() -> None:
    text = latency_report(latency_raw())

    assert "| measurement to alert in the database | 6 | 0.130 |" in text
    assert "disturbed by a suspension of 99 seconds" in text


def test_the_latency_report_says_so_when_no_alert_was_raised() -> None:
    raw = latency_raw()
    raw["result"]["alerts"] = None

    assert "No alert was raised." in latency_report(raw)


def test_the_throughput_report_groups_the_runs_of_a_step() -> None:
    text = throughput_report(throughput_raw())

    assert "| 12 | 2 | 12 | 12 |" in text
    assert "50.0 percent" in text
    assert "| 500 | 1 | 500 | 500 |" in text
    assert "lag growing" in text


def test_the_throughput_report_names_where_each_ramp_stopped() -> None:
    text = throughput_report(throughput_raw())

    assert "run 1: the first failing step offered 500 readings per second" in text
    assert "run 2: every step passed" in text


def test_the_faults_report_has_a_row_for_every_run() -> None:
    text = faults_report(faults_raw())

    assert text.count("| killed |") == 2
    assert text.count("| finished |") == 2
    assert "| killed | 1 | 240 000 | 240 000 | 0 | 0 | 500 |" in text
    assert "| n/a |" in text


@pytest.mark.parametrize(
    ("figure", "raw"),
    [
        (latency_figure, latency_raw()),
        (throughput_figure, throughput_raw()),
        (faults_figure, faults_raw()),
    ],
)
def test_every_figure_labels_its_axes_in_english_with_units(
    figure: Callable[[Mapping[str, Any]], Figure], raw: Mapping[str, Any]
) -> None:
    drawn = figure(raw)

    labels = [a.get_xlabel() + " " + a.get_ylabel() for a in drawn.axes]
    assert all(label.strip() for label in labels)
    assert any("seconds" in label or "messages" in label for label in labels)


def test_a_figure_is_saved_as_pdf_and_as_png(tmp_path: Path) -> None:
    paths = save(faults_figure(faults_raw()), tmp_path, "m3-faults")

    assert [p.suffix for p in paths] == [".pdf", ".png"]
    assert all(p.stat().st_size > 0 for p in paths)
    assert paths[1].read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_a_result_file_is_rendered_into_a_table_and_a_figure(tmp_path: Path) -> None:
    import json

    source = tmp_path / "m3.json"
    source.write_text(json.dumps(faults_raw()), encoding="utf-8")

    written = render(source, tmp_path / "figures")

    assert sorted(p.name for p in written) == [
        "m3-faults.md",
        "m3-faults.pdf",
        "m3-faults.png",
    ]


def test_a_file_that_is_not_a_measurement_is_not_rendered(tmp_path: Path) -> None:
    source = tmp_path / "other.json"
    source.write_text("{}", encoding="utf-8")

    with pytest.raises(MeasureError, match="not the result"):
        render(source, tmp_path)


def test_the_throughput_report_tabulates_the_stage_times_of_the_failing_step() -> None:
    text = throughput_report(throughput_raw())

    assert "## Stage times over the step of 500 readings per second, run 1" in text
    assert "| 20 | 250 000 | 0.300 | 0.600 | 0.004 | 0.008 |" in text


def test_a_ramp_without_a_failing_step_has_no_stage_table() -> None:
    raw = throughput_raw()
    for step in raw["result"]["steps"]:
        step["outcome"]["passed"] = True

    assert "Stage times over the step" not in throughput_report(raw)


def test_the_profile_figure_draws_the_stage_times_of_the_failing_step() -> None:
    figure = step_profile_figure(throughput_raw())

    assert figure is not None
    assert len(figure.axes) == 2
    assert "seconds" in figure.axes[0].get_ylabel()


def test_there_is_no_profile_figure_when_every_step_passed() -> None:
    raw = throughput_raw()
    for step in raw["result"]["steps"]:
        step["outcome"]["passed"] = True

    assert step_profile_figure(raw) is None


def test_the_lag_of_the_failing_step_is_drawn_to_the_end_of_the_load_only() -> None:
    raw = throughput_raw()
    failing = raw["result"]["steps"][2]
    failing["produce_s"] = 1.5
    failing["samples"] = [(0.0, 0, 0), (1.0, 100, 90), (2.0, 200, 200)]

    figure = throughput_figure(raw)

    course = figure.axes[3]
    assert list(course.lines[0].get_xdata()) == [0.0, 1.0]


def test_the_lag_panel_and_the_profile_figure_show_the_same_step() -> None:
    raw = throughput_raw()
    later = _step(500, 2, passed=False)
    raw["result"]["steps"].append(later)

    figure = throughput_figure(raw)
    profile = step_profile_figure(raw)

    assert profile is not None
    assert "run 2" in figure.axes[3].get_title(loc="left")
    assert "run 2" in profile.texts[0].get_text()


def test_a_ramp_without_a_failing_step_draws_the_lag_of_its_last_step() -> None:
    raw = throughput_raw()
    for step in raw["result"]["steps"]:
        step["outcome"]["passed"] = True

    figure = throughput_figure(raw)

    assert "run 2" in figure.axes[3].get_title(loc="left")


def test_the_stage_table_takes_the_last_run_among_the_failing_steps_of_equal_load() -> (
    None
):
    raw = throughput_raw()
    raw["result"]["steps"].append(_step(500, 2, passed=False))

    text = throughput_report(raw)

    assert "## Stage times over the step of 500 readings per second, run 2" in text
