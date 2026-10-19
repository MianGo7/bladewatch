"""Alert rules: thresholds, hysteresis, and the rate of rise.

The rules are pure Python without input or output. A tracker holds the state of
one machine and turns each reading into at most one alert event. The logic that
the steering committee cares about lives here, so that it can be tested without
Docker and without a message format. Implements FR3, FR4, and FR5.
"""

from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from bladewatch.config import Machine, PlantSettings, RuleSettings


class AlertState(StrEnum):
    """The state of the alert of one machine."""

    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"
    RECOVERED = "recovered"


class AlertKind(StrEnum):
    """The condition that caused an alert event."""

    THRESHOLD = "threshold"
    RATE_OF_RISE = "rate_of_rise"


class Severity(StrEnum):
    """The severity of an alert, the highest one that it has reached."""

    WARNING = "warning"
    CRITICAL = "critical"


class AlertStatus(StrEnum):
    """Whether an event raises or closes an alert."""

    RAISED = "raised"
    CLOSED = "closed"


# ADR-0018: the transitions of the state diagram in docs/diagrams. The tracker
# refuses any other change of state, and a test compares this set with the
# diagram and with the transitions that scenarios provoke.
ALLOWED_TRANSITIONS: frozenset[tuple[AlertState, AlertState]] = frozenset(
    {
        (AlertState.NORMAL, AlertState.WARNING),
        (AlertState.NORMAL, AlertState.CRITICAL),
        (AlertState.WARNING, AlertState.CRITICAL),
        (AlertState.WARNING, AlertState.RECOVERED),
        (AlertState.CRITICAL, AlertState.RECOVERED),
        (AlertState.RECOVERED, AlertState.NORMAL),
        (AlertState.RECOVERED, AlertState.WARNING),
        (AlertState.RECOVERED, AlertState.CRITICAL),
    }
)


@dataclass(frozen=True, slots=True)
class AlertEvent:
    """A raise, an escalation, or a closing of the alert of one machine.

    Attributes:
        alert_id: Identifier of the alert, derived from the machine, the kind,
            and the time of the reading that opened it, so that a replayed
            reading gives the same identifier (ADR-0012).
        machine_id: Machine that the alert belongs to.
        kind: Condition that caused this event.
        severity: Highest severity that the alert has reached.
        status: Whether the event raises or closes the alert.
        temperature_c: Temperature of the reading that caused the event in
            degrees Celsius.
        limit: Threshold in degrees Celsius for the kind threshold, and rate
            limit in degrees Celsius per minute for the kind rate of rise.
        measured_at: Time of measurement of that reading, in UTC.
    """

    alert_id: str
    machine_id: str
    kind: AlertKind
    severity: Severity
    status: AlertStatus
    temperature_c: float
    limit: float
    measured_at: datetime


class RateWindow:
    """A sliding window of readings that estimates the rate of rise.

    ADR-0018: the rate is the slope of a least squares line through all readings
    of the window, because a difference of two readings would carry the noise of
    both, which is a standard error of 0.7 degrees Celsius per minute for the
    noise of A13, against 0.22 for the slope over 60 seconds.
    """

    def __init__(self, window_s: float, interval_s: float) -> None:
        """Creates an empty window.

        Args:
            window_s: Length of the window in seconds.
            interval_s: Expected time between two readings in seconds. The
                window counts as full when it spans at least the window length
                less half an interval, so that jitter in the times of
                measurement does not keep it from filling.
        """
        self._window_s = window_s
        self._min_span_s = window_s - interval_s / 2.0
        self._points: deque[tuple[datetime, float]] = deque()

    def add(self, measured_at: datetime, temperature_c: float) -> None:
        """Adds a reading and drops those that left the window."""
        self._points.append((measured_at, temperature_c))
        oldest_kept = measured_at - timedelta(seconds=self._window_s)
        while self._points[0][0] < oldest_kept:
            self._points.popleft()

    def slope_c_per_min(self) -> float | None:
        """Returns the slope in degrees Celsius per minute, or None.

        The result is None until the window spans a full window length, so that
        the first seconds after a start never raise a warning.
        """
        if len(self._points) < 2:
            return None
        origin = self._points[0][0]
        span_s = (self._points[-1][0] - origin).total_seconds()
        if span_s < self._min_span_s:
            return None
        times_s = [(moment - origin).total_seconds() for moment, _ in self._points]
        temperatures_c = [temperature for _, temperature in self._points]
        mean_time = sum(times_s) / len(times_s)
        mean_temperature = sum(temperatures_c) / len(temperatures_c)
        covariance = sum(
            (t - mean_time) * (c - mean_temperature)
            for t, c in zip(times_s, temperatures_c, strict=True)
        )
        variance = sum((t - mean_time) ** 2 for t in times_s)
        return covariance / variance * 60.0


class AlertTracker:
    """The alert state of one machine.

    A temperature at or above a threshold raises an alert, and a rise above the
    limit over the window raises a warning before the warning threshold is
    reached. The severity is the highest one that the alert has reached, so that
    a machine that cools from the critical range stays critical until the alert
    closes (ADR-0018). An alert closes when the temperature has fallen below the
    warning threshold by the hysteresis margin, and an alert that a rise caused
    additionally needs a rate below the limit for a full window.
    """

    def __init__(
        self, machine: Machine, rules: RuleSettings, interval_s: float
    ) -> None:
        """Creates a tracker in the state normal.

        Args:
            machine: The machine, which fixes the thresholds.
            rules: The margin, the window, and the limit of the rate of rise.
            interval_s: Expected time between two readings in seconds.
        """
        self._machine = machine
        self._rules = rules
        self._window = RateWindow(rules.rate_window_s, interval_s)
        self._state = AlertState.NORMAL
        self._last_measured_at: datetime | None = None
        self._last_rate_exceeded_at: datetime | None = None
        self._alert_id = ""
        self._kind = AlertKind.THRESHOLD
        self._severity = Severity.WARNING

    @property
    def machine_id(self) -> str:
        """Identifier of the machine that the tracker belongs to."""
        return self._machine.machine_id

    @property
    def state(self) -> AlertState:
        """The current state of the alert."""
        return self._state

    def observe(self, measured_at: datetime, temperature_c: float) -> AlertEvent | None:
        """Evaluates one reading and returns the event that it causes, if any.

        A reading that is not newer than the last one is ignored, because Kafka
        delivers at least once and a redelivered reading must not change the
        state (ADR-0012).

        Args:
            measured_at: Time of measurement, timezone aware.
            temperature_c: Temperature in degrees Celsius.

        Raises:
            ValueError: If the time of measurement has no time zone.
        """
        if measured_at.utcoffset() is None:
            raise ValueError("measured_at must be timezone aware")
        if self._last_measured_at is not None and measured_at <= self._last_measured_at:
            return None
        self._last_measured_at = measured_at

        self._window.add(measured_at, temperature_c)
        slope = self._window.slope_c_per_min()
        rate_exceeded = slope is not None and slope > self._rules.rate_limit_c_per_min
        if rate_exceeded:
            self._last_rate_exceeded_at = measured_at

        if self._state in (AlertState.NORMAL, AlertState.RECOVERED):
            return self._evaluate_without_alert(
                measured_at, temperature_c, rate_exceeded
            )
        return self._evaluate_open_alert(measured_at, temperature_c)

    def _evaluate_without_alert(
        self, measured_at: datetime, temperature_c: float, rate_exceeded: bool
    ) -> AlertEvent | None:
        machine_type = self._machine.machine_type
        if temperature_c >= machine_type.critical_c:
            return self._open(
                AlertKind.THRESHOLD, Severity.CRITICAL, measured_at, temperature_c
            )
        if temperature_c >= machine_type.warning_c:
            return self._open(
                AlertKind.THRESHOLD, Severity.WARNING, measured_at, temperature_c
            )
        if rate_exceeded:
            return self._open(
                AlertKind.RATE_OF_RISE, Severity.WARNING, measured_at, temperature_c
            )
        if self._state is AlertState.RECOVERED:
            self._move(AlertState.NORMAL)
        return None

    def _evaluate_open_alert(
        self, measured_at: datetime, temperature_c: float
    ) -> AlertEvent | None:
        machine_type = self._machine.machine_type
        if (
            self._state is AlertState.WARNING
            and temperature_c >= machine_type.critical_c
        ):
            self._kind = AlertKind.THRESHOLD
            self._severity = Severity.CRITICAL
            self._move(AlertState.CRITICAL)
            return self._event(AlertStatus.RAISED, measured_at, temperature_c)
        if self._may_close(measured_at, temperature_c):
            event = self._event(AlertStatus.CLOSED, measured_at, temperature_c)
            self._move(AlertState.RECOVERED)
            return event
        return None

    def _may_close(self, measured_at: datetime, temperature_c: float) -> bool:
        # The comparison is strict: a temperature exactly at the closing level
        # keeps the alert open, so that the level itself cannot flap.
        closing_level_c = (
            self._machine.machine_type.warning_c - self._rules.hysteresis_margin_c
        )
        if temperature_c >= closing_level_c:
            return False
        if self._kind is AlertKind.THRESHOLD:
            return True
        if self._last_rate_exceeded_at is None:
            return True
        calm_s = (measured_at - self._last_rate_exceeded_at).total_seconds()
        return calm_s >= self._rules.rate_window_s

    def _open(
        self,
        kind: AlertKind,
        severity: Severity,
        measured_at: datetime,
        temperature_c: float,
    ) -> AlertEvent:
        moment = measured_at.astimezone(UTC).isoformat(timespec="microseconds")
        self._alert_id = f"{self.machine_id}:{kind}:{moment.replace('+00:00', 'Z')}"
        self._kind = kind
        self._severity = severity
        self._move(
            AlertState.CRITICAL if severity is Severity.CRITICAL else AlertState.WARNING
        )
        return self._event(AlertStatus.RAISED, measured_at, temperature_c)

    def _event(
        self, status: AlertStatus, measured_at: datetime, temperature_c: float
    ) -> AlertEvent:
        machine_type = self._machine.machine_type
        if self._kind is AlertKind.RATE_OF_RISE:
            limit = self._rules.rate_limit_c_per_min
        elif self._severity is Severity.CRITICAL and status is AlertStatus.RAISED:
            limit = machine_type.critical_c
        else:
            limit = machine_type.warning_c
        return AlertEvent(
            alert_id=self._alert_id,
            machine_id=self.machine_id,
            kind=self._kind,
            severity=self._severity,
            status=status,
            temperature_c=temperature_c,
            limit=limit,
            measured_at=measured_at,
        )

    def _move(self, target: AlertState) -> None:
        if (self._state, target) not in ALLOWED_TRANSITIONS:
            raise AssertionError(f"transition {self._state} to {target} is not allowed")
        self._state = target


def create_trackers(plant: PlantSettings) -> dict[str, AlertTracker]:
    """Creates one tracker per machine of the plant, in the order of the plant."""
    return {
        machine.machine_id: AlertTracker(machine, plant.rules, plant.sensor.interval_s)
        for machine in plant.machines
    }
