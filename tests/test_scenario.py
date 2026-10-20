from datetime import UTC, datetime, timedelta

import pytest

from bladewatch.config import ScenarioSettings, default_plant
from bladewatch.scenario import (
    CriticalAlertTimes,
    EventSignature,
    RunResult,
    alerting_time,
    critical_times,
    dashboard_query,
    format_report,
    has_rows,
    percentile,
    refusal_message,
    signature_of,
    suspended_seconds,
)
from bladewatch.storage import AlertEventRow, LatencySummary, StageStats

START = datetime(2026, 10, 20, 10, 0, 0, tzinfo=UTC)


def _event(
    severity: str = "critical",
    status: str = "raised",
    kind: str = "threshold",
    second: float = 400.0,
    stages: tuple[float, float, float] = (0.01, 0.20, 0.03),
) -> AlertEventRow:
    measured_at = START + timedelta(seconds=second)
    appended = measured_at + timedelta(seconds=stages[0])
    processed = appended + timedelta(seconds=stages[1])
    stored = processed + timedelta(seconds=stages[2])
    return AlertEventRow(
        alert_id="mould-01:threshold:x",
        kind=kind,
        severity=severity,
        status=status,
        temperature_c=95.2,
        limit=95.0,
        measured_at=measured_at,
        kafka_appended_at=appended,
        processed_at=processed,
        stored_at=stored,
    )


def _result(
    signature: tuple[EventSignature, ...] = (),
    critical: CriticalAlertTimes | None = None,
    suspended_s: float = 0.0,
) -> RunResult:
    stages = LatencySummary(5520, {"measured to stored": StageStats(0.2, 0.4, 0.9)})
    return RunResult(START, [], signature, critical, 5520, stages, suspended_s)


def _times(stored: float, dashboard: float | None = 0.5) -> CriticalAlertTimes:
    return CriticalAlertTimes(0.01, 0.2, 0.03, stored, dashboard, 12.0, 4.0, 8.0)


def test_the_median_of_an_even_sample_lies_between_the_two_middle_values() -> None:
    assert percentile([4.0, 1.0, 3.0, 2.0], 0.5) == 2.5


def test_a_percentile_is_interpolated_between_the_ranks() -> None:
    sample = [float(n) for n in range(1, 101)]

    # Rank position 0.95 * 99 = 94.05, between the values 95 and 96.
    assert percentile(sample, 0.95) == pytest.approx(95.05)


def test_the_extreme_percentiles_are_the_minimum_and_the_maximum() -> None:
    assert (percentile([3.0, 1.0, 2.0], 0.0), percentile([3.0, 1.0, 2.0], 1.0)) == (
        1.0,
        3.0,
    )


def test_the_percentile_of_one_value_is_that_value() -> None:
    assert percentile([7.0], 0.95) == 7.0


def test_the_percentile_of_an_empty_sample_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        percentile([], 0.5)


@pytest.mark.parametrize("fraction", [-0.1, 1.1])
def test_a_fraction_outside_zero_to_one_is_rejected(fraction: float) -> None:
    with pytest.raises(ValueError, match="fraction"):
        percentile([1.0], fraction)


def test_the_signature_of_events_measures_time_from_the_start_of_the_run() -> None:
    signature = signature_of([_event(second=134.0, kind="rate_of_rise")], START)

    assert signature == (
        EventSignature("rate_of_rise", "critical", "raised", 95.2, 95.0, 134.0),
    )


def test_two_runs_with_different_start_times_have_the_same_signature() -> None:
    later = START + timedelta(hours=3)

    assert signature_of([_event()], START) == signature_of(
        [_shifted(_event(), later)], later
    )


def _shifted(event: AlertEventRow, start: datetime) -> AlertEventRow:
    return AlertEventRow(
        event.alert_id,
        event.kind,
        event.severity,
        event.status,
        event.temperature_c,
        event.limit,
        start + timedelta(seconds=400.0),
        event.kafka_appended_at,
        event.processed_at,
        event.stored_at,
    )


def test_the_stage_times_of_the_critical_alert_add_up_to_its_time_in_the_database() -> (
    None
):
    times = critical_times([_event()], None, None)

    assert times is not None
    assert times.measured_to_broker_s == pytest.approx(0.01)
    assert times.broker_to_processor_s == pytest.approx(0.20)
    assert times.processor_to_stored_s == pytest.approx(0.03)
    assert times.measured_to_stored_s == pytest.approx(0.24)


def test_the_time_to_dashboard_and_notification_is_measured_from_the_reading() -> None:
    event = _event()

    times = critical_times(
        [event],
        event.measured_at + timedelta(seconds=0.6),
        event.measured_at + timedelta(seconds=11.0),
    )

    assert times is not None
    assert times.measured_to_dashboard_s == pytest.approx(0.6)
    assert times.measured_to_notification_s == pytest.approx(11.0)


def test_a_dashboard_that_was_not_watched_gives_no_time() -> None:
    times = critical_times([_event()], None, None)

    assert times is not None
    assert (times.measured_to_dashboard_s, times.measured_to_notification_s) == (
        None,
        None,
    )


def test_a_run_without_a_critical_alert_has_no_stage_times() -> None:
    events = [_event(severity="warning", kind="rate_of_rise")]

    assert critical_times(events, None, None) is None


def test_the_first_critical_alert_is_the_one_that_is_measured() -> None:
    first, second = _event(second=400.0), _event(second=500.0)

    times = critical_times(
        [first, second], first.measured_at + timedelta(seconds=1), None
    )

    assert times is not None
    assert times.measured_to_dashboard_s == pytest.approx(1.0)


def test_the_dashboard_query_asks_for_the_open_critical_alert_of_the_machine() -> None:
    body = dashboard_query("mould-01", "bladewatch-timescaledb")

    (query,) = body["queries"]
    assert query["datasource"]["uid"] == "bladewatch-timescaledb"
    assert "FROM open_alerts" in query["rawSql"]
    assert "severity = 'critical'" in query["rawSql"]
    assert "machine_id = 'mould-01'" in query["rawSql"]


def test_the_default_machine_identifier_is_safe_to_use_in_the_query() -> None:
    assert "'" not in ScenarioSettings().machine_id


def test_an_answer_with_a_row_is_recognised() -> None:
    answer = {"results": {"A": {"frames": [{"data": {"values": [["mould-01"]]}}]}}}

    assert has_rows(answer) is True


@pytest.mark.parametrize(
    "answer",
    [
        {"results": {"A": {"frames": [{"data": {"values": [[]]}}]}}},
        {"results": {"A": {"frames": []}}},
        {"results": {}},
        [],
        None,
    ],
)
def test_an_answer_without_a_row_is_recognised(answer: object) -> None:
    assert has_rows(answer) is False


def test_the_refusal_names_what_the_stack_holds_and_what_the_reset_deletes() -> None:
    plant = default_plant()

    message = refusal_message(
        plant, ["the table readings holds 5 rows of the scenario machines"]
    )

    assert "the table readings holds 5 rows" in message
    assert "--reset" in message
    assert plant.topics.readings in message
    assert plant.topics.alerts in message
    assert "hourly aggregate" in message


def test_the_report_gives_median_percentile_and_maximum_over_the_runs() -> None:
    results = [_result(critical=_times(stored)) for stored in (0.2, 0.3, 0.4)]

    report = format_report(results, ScenarioSettings(), {"commit": "abc1234"})

    assert "| median |" in report
    assert "| 95th percentile |" in report
    assert "| maximum |" in report
    assert "- commit: abc1234" in report
    # The median of 0.2, 0.3, and 0.4 is 0.3, and the maximum is 0.4.
    median_line = next(
        line for line in report.splitlines() if line.startswith("| median")
    )
    maximum_line = next(
        line for line in report.splitlines() if line.startswith("| maximum")
    )
    assert "0.300" in median_line
    assert "0.400" in maximum_line


def test_the_report_states_the_number_of_runs_behind_the_statistics() -> None:
    results = [_result(critical=_times(0.2))] * 3

    report = format_report(results, ScenarioSettings(), {})

    assert "over 3 runs, one critical alert each" in report


def test_the_report_says_so_when_the_runs_differ() -> None:
    one = (EventSignature("threshold", "critical", "raised", 95.2, 95.0, 400.0),)
    other = (EventSignature("threshold", "critical", "raised", 95.3, 95.0, 400.0),)

    report = format_report(
        [_result(one, _times(0.2)), _result(other, _times(0.2))], ScenarioSettings(), {}
    )

    assert "DIFFER" in report


def test_the_report_says_so_when_the_runs_are_identical() -> None:
    signature = (EventSignature("threshold", "critical", "raised", 95.2, 95.0, 400.0),)

    report = format_report(
        [_result(signature, _times(0.2)), _result(signature, _times(0.3))],
        ScenarioSettings(),
        {},
    )

    assert "are identical in kind, severity" in report
    assert "| threshold | critical | raised | 95.20 | 95 | 400 |" in report


def test_a_value_that_was_not_measured_is_written_as_not_available() -> None:
    results = [_result(critical=_times(0.2, dashboard=None))]

    report = format_report(results, ScenarioSettings(), {})

    assert "n/a" in report


def test_a_run_without_a_critical_alert_appears_in_the_report() -> None:
    report = format_report([_result()], ScenarioSettings(), {})

    assert "no critical alert" in report


# A suspension of the machine, which stops the monotonic clock but not the wall clock


def test_a_run_during_which_the_machine_did_not_sleep_has_no_suspension() -> None:
    wall_start = START
    wall_end = START + timedelta(seconds=460.4)

    assert suspended_seconds(wall_start, wall_end, 1000.0, 1460.4) == pytest.approx(0.0)


def test_a_suspension_is_the_time_that_only_the_wall_clock_counted() -> None:
    wall_end = START + timedelta(seconds=559.3)

    # The monotonic clock stood still for 99 seconds of the wall clock.
    assert suspended_seconds(START, wall_end, 1000.0, 1460.3) == pytest.approx(99.0)


def test_a_small_difference_between_the_clocks_is_not_a_suspension() -> None:
    wall_end = START + timedelta(seconds=460.0)

    assert suspended_seconds(START, wall_end, 1000.0, 1460.5) == pytest.approx(-0.5)


def test_a_disturbed_run_is_listed_but_left_out_of_the_statistics() -> None:
    results = [
        _result(critical=_times(0.2)),
        _result(critical=_times(0.3)),
        _result(critical=_times(99.3), suspended_s=99.0),
    ]

    report = format_report(results, ScenarioSettings(), {})

    assert "disturbed, the machine was suspended for 99 seconds" in report
    maximum_line = next(
        line for line in report.splitlines() if line.startswith("| maximum")
    )
    assert "0.300" in maximum_line
    assert "99.300" not in maximum_line
    assert "over 2 runs, one critical alert each" in report


def test_a_run_counts_as_disturbed_from_two_seconds_of_suspension() -> None:
    assert _result(suspended_s=2.5).disturbed is True
    assert _result(suspended_s=1.9).disturbed is False


def test_the_report_names_the_runs_that_were_left_out_below_the_statistics() -> None:
    results = [
        _result(critical=_times(0.2)),
        _result(critical=_times(50.0), suspended_s=60.0),
    ]

    report = format_report(results, ScenarioSettings(), {})

    assert "1 of 2 runs were disturbed and are not part of the statistics" in report


# The state history of Grafana, which separates the notification delay into two parts


def _annotation(
    second: float, state: str, machine: str = "mould-01"
) -> dict[str, object]:
    return {
        "time": int((START + timedelta(seconds=second)).timestamp() * 1000),
        "newState": state,
        "tags": ["alertname:Critical machine alert", f"machine_id:{machine}"],
    }


def test_the_first_transition_to_firing_after_the_reading_is_the_firing_time() -> None:
    history = [
        _annotation(390.0, "Normal"),
        _annotation(410.0, "Alerting"),
        _annotation(420.0, "Alerting"),
    ]

    assert alerting_time(history, "mould-01", START + timedelta(seconds=400)) == (
        START + timedelta(seconds=410)
    )


def test_a_transition_of_another_machine_is_not_the_firing_time() -> None:
    history = [_annotation(410.0, "Alerting", machine="mould-02")]

    assert alerting_time(history, "mould-01", START) is None


def test_a_firing_before_the_reading_does_not_count() -> None:
    history = [_annotation(100.0, "Alerting")]

    assert alerting_time(history, "mould-01", START + timedelta(seconds=400)) is None


def test_a_transition_to_normal_is_not_a_firing() -> None:
    history = [_annotation(410.0, "Normal (MissingSeries)")]

    assert alerting_time(history, "mould-01", START) is None


def test_the_history_may_list_its_entries_in_any_order() -> None:
    history = [_annotation(430.0, "Alerting"), _annotation(410.0, "Alerting")]

    assert alerting_time(history, "mould-01", START) == START + timedelta(seconds=410)


def test_the_notification_delay_is_split_at_the_firing_time_of_the_rule() -> None:
    event = _event()
    alerting = event.measured_at + timedelta(seconds=3.0)
    notified = event.measured_at + timedelta(seconds=28.0)

    times = critical_times([event], None, notified, alerting)

    assert times is not None
    assert times.measured_to_alerting_s == pytest.approx(3.0)
    assert times.alerting_to_notification_s == pytest.approx(25.0)
    assert times.measured_to_notification_s == pytest.approx(28.0)


def test_without_a_firing_time_the_notification_delay_is_not_split() -> None:
    event = _event()

    times = critical_times([event], None, event.measured_at + timedelta(seconds=9.0))

    assert times is not None
    assert times.measured_to_alerting_s is None
    assert times.alerting_to_notification_s is None


def test_the_report_has_a_column_for_each_part_of_the_notification_delay() -> None:
    report = format_report([_result(critical=_times(0.2))], ScenarioSettings(), {})

    assert "measurement to firing state in Grafana" in report
    assert "firing state to notification" in report
