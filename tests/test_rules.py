import re
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from bladewatch.config import (
    GRINDER,
    MOULD,
    OVEN,
    Machine,
    MachineType,
    RuleSettings,
    SensorSettings,
    default_plant,
)
from bladewatch.rules import (
    ALLOWED_TRANSITIONS,
    AlertEvent,
    AlertKind,
    AlertState,
    AlertStatus,
    AlertTracker,
    RateWindow,
    Severity,
    create_trackers,
)
from bladewatch.sensor import TemperatureSensor, spawn_generators

START = datetime(2026, 10, 19, 9, 0, 0, tzinfo=UTC)
DIAGRAM = Path(__file__).parents[1] / "docs" / "diagrams" / "alert-states.puml"


def at(seconds: float) -> datetime:
    return START + timedelta(seconds=seconds)


def make_tracker(machine_type: MachineType = MOULD, **rules: float) -> AlertTracker:
    return AlertTracker(
        Machine(f"{machine_type.name}-01", machine_type),
        RuleSettings(**rules),
        interval_s=1.0,
    )


def feed(
    tracker: AlertTracker, temperatures_c: Iterable[float], start_s: float = 0.0
) -> list[tuple[float, AlertEvent]]:
    """Feeds one reading per second and returns the events with their second."""
    events = []
    for offset, temperature_c in enumerate(temperatures_c):
        second = start_s + offset
        event = tracker.observe(at(second), temperature_c)
        if event is not None:
            events.append((second, event))
    return events


def ramp(start_c: float, rate_c_per_min: float, seconds: int) -> list[float]:
    return [start_c + rate_c_per_min * second / 60.0 for second in range(seconds)]


# Thresholds, which implement FR3.


def test_a_temperature_below_the_warning_threshold_raises_nothing() -> None:
    tracker = make_tracker()

    events = feed(tracker, [70.0, 79.999, 70.0])

    assert events == []
    assert tracker.state is AlertState.NORMAL


@pytest.mark.parametrize("machine_type", [MOULD, OVEN, GRINDER])
def test_a_temperature_at_the_warning_threshold_raises_a_warning(
    machine_type: MachineType,
) -> None:
    tracker = make_tracker(machine_type)

    (second, event), *rest = feed(tracker, [machine_type.warning_c])

    assert rest == []
    assert (event.kind, event.severity, event.status) == (
        AlertKind.THRESHOLD,
        Severity.WARNING,
        AlertStatus.RAISED,
    )
    assert event.limit == machine_type.warning_c
    assert event.temperature_c == machine_type.warning_c
    assert tracker.state is AlertState.WARNING


@pytest.mark.parametrize("machine_type", [MOULD, OVEN, GRINDER])
def test_a_temperature_just_below_the_warning_threshold_raises_nothing(
    machine_type: MachineType,
) -> None:
    tracker = make_tracker(machine_type)

    assert feed(tracker, [machine_type.warning_c - 0.001]) == []


@pytest.mark.parametrize("machine_type", [MOULD, OVEN, GRINDER])
def test_a_temperature_at_the_critical_threshold_raises_a_critical_alert(
    machine_type: MachineType,
) -> None:
    tracker = make_tracker(machine_type)

    events = feed(tracker, [machine_type.warning_c, machine_type.critical_c])

    (_, critical) = events[-1]
    assert len(events) == 2
    assert (critical.severity, critical.status) == (
        Severity.CRITICAL,
        AlertStatus.RAISED,
    )
    assert critical.limit == machine_type.critical_c
    assert tracker.state is AlertState.CRITICAL


def test_the_escalation_to_critical_is_an_event_of_the_same_alert() -> None:
    tracker = make_tracker()

    (_, warning), (_, critical) = feed(tracker, [80.0, 95.0])

    assert critical.alert_id == warning.alert_id
    assert (critical.kind, critical.severity, critical.status) == (
        AlertKind.THRESHOLD,
        Severity.CRITICAL,
        AlertStatus.RAISED,
    )
    assert critical.limit == MOULD.critical_c


def test_a_jump_from_normal_to_critical_raises_one_critical_event() -> None:
    tracker = make_tracker()

    events = feed(tracker, [70.0, 96.0])

    assert [(e.severity, e.status) for _, e in events] == [
        (Severity.CRITICAL, AlertStatus.RAISED)
    ]
    assert tracker.state is AlertState.CRITICAL


def test_the_thresholds_of_each_machine_type_apply_to_its_own_machines() -> None:
    mould, oven = make_tracker(MOULD), make_tracker(OVEN)

    assert feed(mould, [85.0]) != []
    assert feed(oven, [85.0]) == []


def test_the_alert_identifier_names_the_machine_the_kind_and_the_opening_time() -> None:
    tracker = make_tracker()

    (_, event), *_ = feed(tracker, [80.0], start_s=5.0)

    assert event.alert_id == "mould-01:threshold:2026-10-19T09:00:05.000000Z"


# Hysteresis, which implements FR4.


def test_a_critical_alert_stays_critical_while_the_temperature_falls_back() -> None:
    tracker = make_tracker()

    events = feed(tracker, [80.0, 96.0, 90.0, 85.0, 79.0])

    assert [(e.severity, e.status) for _, e in events] == [
        (Severity.WARNING, AlertStatus.RAISED),
        (Severity.CRITICAL, AlertStatus.RAISED),
    ]
    assert tracker.state is AlertState.CRITICAL


def test_an_alert_stays_open_at_exactly_the_closing_level() -> None:
    tracker = make_tracker()

    events = feed(tracker, [80.0, 78.0])

    assert [e.status for _, e in events] == [AlertStatus.RAISED]
    assert tracker.state is AlertState.WARNING


def test_an_alert_closes_just_below_the_closing_level() -> None:
    tracker = make_tracker()

    events = feed(tracker, [80.0, 77.999])

    assert [e.status for _, e in events] == [AlertStatus.RAISED, AlertStatus.CLOSED]
    assert tracker.state is AlertState.RECOVERED


@pytest.mark.parametrize(
    ("machine_type", "closing_level_c"), [(MOULD, 78.0), (OVEN, 108.0), (GRINDER, 58.0)]
)
def test_the_closing_level_is_the_warning_threshold_less_the_margin(
    machine_type: MachineType, closing_level_c: float
) -> None:
    open_at = feed(
        make_tracker(machine_type), [machine_type.warning_c, closing_level_c]
    )
    closed_at = feed(
        make_tracker(machine_type), [machine_type.warning_c, closing_level_c - 0.001]
    )

    assert len(open_at) == 1
    assert len(closed_at) == 2


def test_the_closing_event_carries_the_severity_the_alert_reached() -> None:
    tracker = make_tracker()

    events = feed(tracker, [80.0, 96.0, 77.0])

    closed = events[-1][1]
    assert (closed.status, closed.severity) == (AlertStatus.CLOSED, Severity.CRITICAL)
    assert closed.alert_id == events[0][1].alert_id
    assert closed.temperature_c == 77.0


def test_a_series_that_oscillates_around_the_warning_threshold_raises_one_alert() -> (
    None
):
    tracker = make_tracker()
    series = [79.9 if second % 2 == 0 else 80.1 for second in range(100)]

    events = feed(tracker, series)

    assert [e.status for _, e in events] == [AlertStatus.RAISED]
    assert tracker.state is AlertState.WARNING


def test_a_series_that_oscillates_around_the_closing_level_closes_the_alert_once() -> (
    None
):
    tracker = make_tracker()
    series = [80.0] + [77.9 if second % 2 == 0 else 78.1 for second in range(100)]

    events = feed(tracker, series)

    assert [e.status for _, e in events] == [AlertStatus.RAISED, AlertStatus.CLOSED]
    assert tracker.state is AlertState.NORMAL


def test_the_state_after_a_closing_is_recovered_for_one_reading_and_then_normal() -> (
    None
):
    tracker = make_tracker()
    feed(tracker, [80.0, 77.0])
    assert tracker.state is AlertState.RECOVERED

    tracker.observe(at(2), 70.0)

    assert tracker.state is AlertState.NORMAL


def test_a_new_crossing_after_a_closing_opens_a_new_alert() -> None:
    tracker = make_tracker()

    events = feed(tracker, [80.0, 77.0, 70.0, 80.0])

    assert events[0][1].alert_id != events[-1][1].alert_id
    assert events[-1][1].status is AlertStatus.RAISED


def test_a_crossing_in_the_reading_right_after_a_closing_opens_a_new_alert() -> None:
    tracker = make_tracker()

    events = feed(tracker, [80.0, 77.0, 96.0])

    assert (events[-1][1].severity, events[-1][1].status) == (
        Severity.CRITICAL,
        AlertStatus.RAISED,
    )
    assert tracker.state is AlertState.CRITICAL


# Rate of rise, which implements FR5.


def test_the_rate_of_rise_is_silent_until_the_window_is_full() -> None:
    tracker = make_tracker()

    events = feed(tracker, ramp(60.0, 12.0, 60))  # 60 readings span 59 seconds

    assert events == []


def test_a_rise_above_the_limit_over_a_full_window_raises_a_warning() -> None:
    tracker = make_tracker()

    (second, event), *rest = feed(tracker, ramp(60.0, 12.0, 61))

    assert rest == []
    assert second == 60.0
    assert (event.kind, event.severity, event.status) == (
        AlertKind.RATE_OF_RISE,
        Severity.WARNING,
        AlertStatus.RAISED,
    )
    assert event.limit == 3.0
    assert event.temperature_c < MOULD.warning_c
    assert tracker.state is AlertState.WARNING


def test_a_rise_below_the_limit_raises_nothing() -> None:
    tracker = make_tracker()

    assert feed(tracker, ramp(40.0, 2.99, 600)) == []


def test_a_rise_above_the_limit_by_a_little_raises_a_warning() -> None:
    tracker = make_tracker()

    events = feed(tracker, ramp(40.0, 3.01, 600))

    assert [e.kind for _, e in events] == [AlertKind.RATE_OF_RISE]


def test_a_falling_temperature_raises_nothing() -> None:
    assert feed(make_tracker(), ramp(79.0, -12.0, 300)) == []


def _first_second_with_slope_above(
    temperatures_c: Sequence[float], limit_c_per_min: float, window_s: int
) -> int:
    """Independent oracle: the first second at which a fitted line is too steep."""
    for second in range(window_s, len(temperatures_c)):
        times = np.arange(second - window_s, second + 1)
        values = temperatures_c[second - window_s : second + 1]
        slope_c_per_min = np.polyfit(times, values, 1)[0] * 60.0
        if slope_c_per_min > limit_c_per_min:
            return second
    raise AssertionError("the slope never exceeds the limit")


def test_the_rate_warning_agrees_with_an_independent_least_squares_fit() -> None:
    # The scenario of the demonstration: normal operation, then a rise of
    # 5 degrees Celsius per minute from 100 seconds on.
    series = [70.0] * 100 + [70.0 + 5.0 * s / 60.0 for s in range(1, 400)]
    expected_second = _first_second_with_slope_above(series, 3.0, 60)
    tracker = make_tracker()

    (second, event), *_ = feed(tracker, series)

    assert event.kind is AlertKind.RATE_OF_RISE
    assert second == expected_second


def test_the_rate_warning_comes_before_the_warning_threshold_in_the_scenario() -> None:
    series = [70.0] + [70.0 + 5.0 * s / 60.0 for s in range(1, 400)]
    tracker = make_tracker()

    events = feed(tracker, series)

    rate_second = events[0][0]
    warning_second = next(s for s, t in enumerate(series) if t >= MOULD.warning_c)
    critical_second = next(s for s, t in enumerate(series) if t >= MOULD.critical_c)
    # 70 to 80 degrees Celsius takes 120 seconds and 70 to 95 takes 300 seconds
    # at 5 degrees Celsius per minute.
    assert (warning_second, critical_second) == (120, 300)
    assert events[0][1].kind is AlertKind.RATE_OF_RISE
    assert rate_second < warning_second


def test_the_scenario_raises_the_rate_warning_then_the_critical_alert() -> None:
    series = [70.0] + [70.0 + 5.0 * s / 60.0 for s in range(1, 400)]
    tracker = make_tracker()

    events = feed(tracker, series)

    assert [(e.kind, e.severity, e.status) for _, e in events] == [
        (AlertKind.RATE_OF_RISE, Severity.WARNING, AlertStatus.RAISED),
        (AlertKind.THRESHOLD, Severity.CRITICAL, AlertStatus.RAISED),
    ]
    assert events[0][1].alert_id == events[1][1].alert_id


def test_a_rate_alert_closes_after_a_full_calm_window_below_the_closing_level() -> None:
    series = ramp(60.0, 12.0, 61) + [72.0] * 200
    tracker = make_tracker()
    exceeded = [
        s
        for s in range(60, len(series))
        if np.polyfit(np.arange(s - 60, s + 1), series[s - 60 : s + 1], 1)[0] * 60.0
        > 3.0
    ]

    events = feed(tracker, series)

    assert [e.status for _, e in events] == [AlertStatus.RAISED, AlertStatus.CLOSED]
    closed_second, closed = events[1]
    # The alert closes exactly one window after the last reading with a slope
    # above the limit, and not before.
    assert closed_second == exceeded[-1] + 60
    assert closed.kind is AlertKind.RATE_OF_RISE
    assert closed.limit == 3.0


def test_a_rate_alert_stays_open_while_the_temperature_is_above_the_closing_level() -> (
    None
):
    series = ramp(60.0, 19.0, 61) + [79.0] * 400
    tracker = make_tracker()

    events = feed(tracker, series)

    assert [e.status for _, e in events] == [AlertStatus.RAISED]
    assert tracker.state is AlertState.WARNING


def test_a_rate_alert_closes_when_the_temperature_has_fallen_after_a_calm_window() -> (
    None
):
    series = ramp(60.0, 19.0, 61) + [79.0] * 400 + [77.0]
    tracker = make_tracker()

    events = feed(tracker, series)

    assert [e.status for _, e in events] == [AlertStatus.RAISED, AlertStatus.CLOSED]


def test_normal_operation_with_noise_never_raises_an_alert() -> None:
    for machine_type, seed in ((MOULD, 1), (OVEN, 2), (GRINDER, 3)):
        machine = Machine(f"{machine_type.name}-01", machine_type)
        sensor = TemperatureSensor(
            machine, SensorSettings(), spawn_generators(seed, 1)[0]
        )
        tracker = make_tracker(machine_type)

        events = feed(tracker, [sensor.measure_c(float(s)) for s in range(7200)])

        assert events == []


def test_the_highest_published_heating_rate_with_noise_does_not_warn() -> None:
    # Lux et al. (2020, p. 405) test heating rates of up to 1.5 degrees Celsius
    # per minute, half of the limit of 3.
    sensor = TemperatureSensor(
        Machine("mould-01", MOULD), SensorSettings(), spawn_generators(4, 1)[0]
    )
    tracker = make_tracker()

    events = feed(
        tracker, [sensor.measure_c(0.0) - 10.0 + 1.5 * s / 60.0 for s in range(600)]
    )

    assert events == []


# Robustness against redelivery


def test_a_redelivered_reading_changes_nothing() -> None:
    tracker = make_tracker()
    first = tracker.observe(at(0), 80.0)

    again = tracker.observe(at(0), 80.0)

    assert first is not None
    assert again is None
    assert tracker.state is AlertState.WARNING


def test_a_reading_older_than_the_last_one_is_ignored() -> None:
    tracker = make_tracker()
    tracker.observe(at(10), 70.0)

    assert tracker.observe(at(5), 96.0) is None
    assert tracker.state is AlertState.NORMAL


def test_replaying_a_series_into_a_new_tracker_gives_the_same_events() -> None:
    series = [70.0] * 30 + ramp(70.0, 5.0, 400)

    assert feed(make_tracker(), series) == feed(make_tracker(), series)


def test_a_reading_without_a_time_zone_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone"):
        make_tracker().observe(datetime(2026, 10, 19, 9, 0, 0), 70.0)


def test_create_trackers_returns_one_tracker_per_machine() -> None:
    plant = default_plant()

    trackers = create_trackers(plant)

    assert list(trackers) == [m.machine_id for m in plant.machines]
    assert all(t.state is AlertState.NORMAL for t in trackers.values())


# The rate window


def test_the_slope_of_an_exact_line_is_its_rate_per_minute() -> None:
    window = RateWindow(window_s=60.0, interval_s=1.0)
    for second in range(61):
        window.add(at(second), 50.0 + 0.1 * second)

    assert window.slope_c_per_min() == pytest.approx(6.0)


def test_the_slope_is_not_available_before_the_window_is_full() -> None:
    window = RateWindow(window_s=60.0, interval_s=1.0)
    for second in range(30):
        window.add(at(second), 50.0)

    assert window.slope_c_per_min() is None


def test_readings_older_than_the_window_do_not_influence_the_slope() -> None:
    window = RateWindow(window_s=60.0, interval_s=1.0)
    for second in range(200):
        window.add(at(second), 500.0 if second < 100 else 50.0)

    assert window.slope_c_per_min() == pytest.approx(0.0, abs=1e-9)


def test_the_slope_tolerates_jitter_in_the_times_of_measurement() -> None:
    window = RateWindow(window_s=60.0, interval_s=1.0)
    for second in range(61):
        window.add(
            at(second + (0.2 if second % 2 else -0.2) + 0.2), 50.0 + 0.1 * second
        )

    assert window.slope_c_per_min() is not None


# The state diagram


def _drive(temperature_series: Sequence[Sequence[float]]) -> set[tuple[str, str]]:
    observed: set[tuple[str, str]] = set()
    for series in temperature_series:
        tracker = make_tracker()
        for second, temperature_c in enumerate(series):
            before = tracker.state
            tracker.observe(at(second), temperature_c)
            if tracker.state is not before:
                observed.add((before, tracker.state))
    return observed


def test_the_scenarios_provoke_exactly_the_allowed_transitions() -> None:
    scenarios = [
        [70.0, 80.0, 96.0, 77.0, 70.0],  # normal, warning, critical, recovered, normal
        [70.0, 96.0, 77.0, 80.0, 77.0],  # normal to critical, recovered to warning
        [80.0, 77.0, 96.0],  # warning, recovered, recovered to critical
    ]

    assert _drive(scenarios) == set(ALLOWED_TRANSITIONS)


def test_the_state_diagram_shows_exactly_the_allowed_transitions() -> None:
    text = DIAGRAM.read_text(encoding="utf-8")
    arrows = re.findall(r"^\s*(\w+)\s*-->\s*(\w+)", text, flags=re.MULTILINE)

    drawn = {(source.lower(), target.lower()) for source, target in arrows}

    assert drawn == {(str(a), str(b)) for a, b in ALLOWED_TRANSITIONS}
