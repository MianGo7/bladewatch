"""Statistics and verdicts of the measurements, as pure functions.

Holds the distribution of a latency, the consumer lag of a run, the throughput
that the lag implies, and the criteria by which a load step of M2 passes or fails
(ADR-0026). It imports neither Kafka nor the database, so that the criteria are
tested without the stack. Implements the evaluation of M1 to M3.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np

from bladewatch.config import ThroughputSettings


@dataclass(frozen=True, slots=True)
class Distribution:
    """The distribution of a series of times in seconds.

    Attributes:
        count: Number of values.
        median_s: Median.
        p95_s: 95th percentile, by linear interpolation.
        p99_s: 99th percentile, by linear interpolation.
        maximum_s: Maximum.
    """

    count: int
    median_s: float
    p95_s: float
    p99_s: float
    maximum_s: float


def summarise(values: Sequence[float]) -> Distribution:
    """Summarises a series of times.

    Args:
        values: The times in seconds.

    Raises:
        ValueError: If the series is empty.
    """
    if not values:
        raise ValueError("summarise: no values")
    series = np.asarray(values, dtype=float)
    median, p95, p99 = np.percentile(series, [50.0, 95.0, 99.0])
    return Distribution(
        count=len(series),
        median_s=float(median),
        p95_s=float(p95),
        p99_s=float(p99),
        maximum_s=float(series.max()),
    )


@dataclass(frozen=True, slots=True)
class LagSample:
    """The state of the consumer group at one moment.

    Attributes:
        time_s: Seconds since the start of the run.
        end_offset: Sum over the partitions of the offset that the next message
            of the topic receives, which counts the messages that were appended.
        committed_offset: Sum over the partitions of the committed offset of the
            group, which counts the readings that are stored, because the
            processor commits after the database transaction (ADR-0020).
    """

    time_s: float
    end_offset: int
    committed_offset: int

    @property
    def lag(self) -> int:
        """Returns the messages that are appended and not yet committed."""
        return self.end_offset - self.committed_offset


def _window(
    samples: Sequence[LagSample], start_s: float, end_s: float
) -> list[LagSample]:
    return [s for s in samples if start_s <= s.time_s <= end_s]


def _slope(times: Sequence[float], values: Sequence[float]) -> float:
    if len(times) < 2 or times[-1] == times[0]:
        return 0.0
    slope, _ = np.polyfit(np.asarray(times, float), np.asarray(values, float), 1)
    return float(slope)


def lag_growth_per_s(
    samples: Sequence[LagSample], start_s: float, end_s: float
) -> float:
    """Returns the slope of the lag over a window, in messages per second.

    The slope is the least squares line through the samples of the window, so that
    the sawtooth of the batches does not decide it.

    Args:
        samples: The samples of the run.
        start_s: Start of the window in seconds since the start of the run.
        end_s: End of the window.
    """
    window = _window(samples, start_s, end_s)
    return _slope([s.time_s for s in window], [float(s.lag) for s in window])


def lag_floor_growth_per_s(
    samples: Sequence[LagSample], start_s: float, end_s: float, window_s: float
) -> float:
    """Returns the slope of the floor of the lag over a window, per second.

    The producer publishes the readings of a tick in a burst, so that the lag is a
    sawtooth of the height of one tick, and a line through the samples follows the
    phase of the sampling. The floor, which is the smallest lag in each stretch of
    `window_s` seconds, removes the sawtooth: it stays near zero while the processor
    keeps up and rises when a backlog builds (ADR-0026).

    Args:
        samples: The samples of the run.
        start_s: Start of the window in seconds since the start of the run.
        end_s: End of the window.
        window_s: Length of the stretches whose smallest lag is taken.
    """
    window = _window(samples, start_s, end_s)
    times: list[float] = []
    floors: list[float] = []
    position = start_s
    while position < end_s:
        stretch = [s for s in window if position <= s.time_s < position + window_s]
        if stretch:
            times.append(position + window_s / 2.0)
            floors.append(float(min(s.lag for s in stretch)))
        position += window_s
    return _slope(times, floors)


def rate_per_s(samples: Sequence[LagSample], start_s: float, end_s: float) -> float:
    """Returns the readings that were stored per second over a window.

    Args:
        samples: The samples of the run.
        start_s: Start of the window in seconds since the start of the run.
        end_s: End of the window.
    """
    window = _window(samples, start_s, end_s)
    return _slope(
        [s.time_s for s in window], [float(s.committed_offset) for s in window]
    )


def catch_up_time_s(
    samples: Sequence[LagSample], from_s: float, threshold: int
) -> float | None:
    """Returns the time from a moment until the lag is at most a threshold.

    Args:
        samples: The samples of the run.
        from_s: The moment from which the time is counted, for instance the
            restart of the processor.
        threshold: The lag in messages that counts as caught up.

    Returns:
        The seconds from `from_s` to the first sample at or after it with a lag
        of at most the threshold, or None if there is none.
    """
    for sample in samples:
        if sample.time_s >= from_s and sample.lag <= threshold:
            return sample.time_s - from_s
    return None


@dataclass(frozen=True, slots=True)
class StepOutcome:
    """The verdict on one load step of M2.

    Attributes:
        generator_limited: The producer delivered less than the tolerated
            fraction of the offered load, so that the step does not test the
            processor.
        latency_missed: The 95th percentile of the time from measurement to
            storage exceeded the target of A5.
        lag_growing: The floor of the lag grew continuously over the second half of
            the step.
    """

    generator_limited: bool
    latency_missed: bool
    lag_growing: bool

    @property
    def passed(self) -> bool:
        """Returns whether the step met both criteria and tested the processor."""
        return not (self.generator_limited or self.latency_missed or self.lag_growing)

    @property
    def reason(self) -> str:
        """Names why the step failed, or says that it passed."""
        reasons = []
        if self.generator_limited:
            reasons.append("generator limited")
        if self.latency_missed:
            reasons.append("latency target missed")
        if self.lag_growing:
            reasons.append("lag growing")
        return ", ".join(reasons) if reasons else "passed"


def judge_step(
    offered_per_s: int,
    delivered: int,
    duration_s: float,
    p95_to_stored_s: float,
    samples: Sequence[LagSample],
    settings: ThroughputSettings,
) -> StepOutcome:
    """Judges a load step by the criteria of A22.

    Args:
        offered_per_s: The readings per second that the step offers.
        delivered: The readings that the broker appended during the step.
        duration_s: Length of the load of the step in seconds.
        p95_to_stored_s: 95th percentile of the time from measurement to storage.
        samples: The lag samples of the step, timed from the start of the load.
        settings: The criteria.
    """
    offered = offered_per_s * duration_s
    growth = lag_floor_growth_per_s(
        samples, duration_s / 2.0, duration_s, settings.lag_window_s
    )
    return StepOutcome(
        generator_limited=delivered < settings.generator_tolerance * offered,
        latency_missed=p95_to_stored_s > settings.latency_target_s,
        lag_growing=growth > settings.lag_growth_fraction * offered_per_s,
    )


def first_failure(outcomes: Sequence[StepOutcome]) -> int | None:
    """Returns the index of the first step that did not pass, or None."""
    for index, outcome in enumerate(outcomes):
        if not outcome.passed:
            return index
    return None
