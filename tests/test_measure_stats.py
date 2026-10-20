import pytest

from bladewatch.config import ThroughputSettings
from bladewatch.measure.stats import (
    LagSample,
    StepOutcome,
    catch_up_time_s,
    first_failure,
    judge_step,
    lag_floor_growth_per_s,
    lag_growth_per_s,
    rate_per_s,
    raw_step_passed,
    summarise,
)

SETTINGS = ThroughputSettings()


def _samples(lags: list[int], committed_rate: int = 100) -> list[LagSample]:
    return [
        LagSample(
            float(t),
            end_offset=lag + t * committed_rate,
            committed_offset=t * committed_rate,
        )
        for t, lag in enumerate(lags)
    ]


def test_the_distribution_of_a_series_has_its_median_percentiles_and_maximum() -> None:
    values = [float(n) for n in range(1, 101)]

    distribution = summarise(values)

    assert distribution.count == 100
    assert distribution.median_s == pytest.approx(50.5)
    # Linear interpolation between ranks: 1 + 0.95 * 99 = 95.05, and 99.01 for 0.99.
    assert distribution.p95_s == pytest.approx(95.05)
    assert distribution.p99_s == pytest.approx(99.01)
    assert distribution.maximum_s == 100.0


def test_the_order_of_the_values_does_not_change_the_distribution() -> None:
    assert summarise([3.0, 1.0, 2.0]) == summarise([1.0, 2.0, 3.0])


def test_a_single_value_is_every_statistic() -> None:
    distribution = summarise([0.25])

    assert (distribution.median_s, distribution.p95_s, distribution.maximum_s) == (
        0.25,
        0.25,
        0.25,
    )


def test_an_empty_series_cannot_be_summarised() -> None:
    with pytest.raises(ValueError, match="no values"):
        summarise([])


def test_the_lag_is_the_difference_of_the_two_offsets() -> None:
    assert LagSample(0.0, end_offset=1500, committed_offset=1200).lag == 300


def test_a_lag_that_grows_by_ten_per_second_has_a_slope_of_ten() -> None:
    samples = _samples([100 + 10 * t for t in range(20)])

    assert lag_growth_per_s(samples, 0.0, 19.0) == pytest.approx(10.0)


def test_a_sawtooth_of_the_lag_around_a_constant_has_no_slope() -> None:
    samples = _samples([400 if t % 2 == 0 else 600 for t in range(40)])

    assert abs(lag_growth_per_s(samples, 0.0, 39.0)) < 2.0


def test_the_slope_of_a_window_with_one_sample_is_zero() -> None:
    assert lag_growth_per_s(_samples([5, 6, 7]), 1.0, 1.0) == 0.0


def test_the_rate_is_the_growth_of_the_committed_offset() -> None:
    samples = _samples([0] * 30, committed_rate=250)

    assert rate_per_s(samples, 5.0, 25.0) == pytest.approx(250.0)


def test_the_catch_up_time_runs_from_the_given_moment() -> None:
    samples = _samples([0, 0, 900, 700, 400, 90, 20, 10])

    assert catch_up_time_s(samples, from_s=2.0, threshold=100) == pytest.approx(3.0)


def test_a_lag_that_never_falls_below_the_threshold_has_no_catch_up_time() -> None:
    assert catch_up_time_s(_samples([500, 600, 700]), 0.0, 100) is None


def test_a_lag_below_the_threshold_before_the_moment_does_not_count() -> None:
    samples = _samples([0, 0, 900, 50])

    assert catch_up_time_s(samples, from_s=2.0, threshold=100) == pytest.approx(1.0)


def _judge(
    delivered: int,
    p95: float = 0.2,
    lags: list[int] | None = None,
    offered: int = 1000,
    duration: float = 20.0,
) -> StepOutcome:
    samples = _samples(lags if lags is not None else [300] * 21)
    return judge_step(offered, delivered, duration, p95, samples, SETTINGS)


def test_a_step_with_the_offered_load_a_low_latency_and_a_steady_lag_passes() -> None:
    outcome = _judge(delivered=20_000)

    assert outcome.passed
    assert outcome.reason == "passed"


def test_a_step_misses_the_target_when_the_95th_percentile_exceeds_five_seconds() -> (
    None
):
    outcome = _judge(delivered=20_000, p95=5.1)

    assert outcome.latency_missed
    assert not outcome.passed


def test_a_95th_percentile_of_exactly_the_target_passes() -> None:
    assert _judge(delivered=20_000, p95=5.0).passed


def test_a_step_is_generator_limited_below_the_tolerated_fraction() -> None:
    outcome = _judge(delivered=18_999)

    assert outcome.generator_limited
    assert outcome.reason == "generator limited"


def test_a_step_at_the_tolerated_fraction_is_not_generator_limited() -> None:
    assert not _judge(delivered=19_000).generator_limited


def test_a_lag_that_gains_two_percent_of_the_load_per_second_is_growing() -> None:
    # 1000 readings per second offered, so the limit is 10 messages per second.
    outcome = _judge(delivered=20_000, lags=[300 + 20 * t for t in range(21)])

    assert outcome.lag_growing


def test_a_lag_that_gains_half_a_percent_of_the_load_per_second_is_not_growing() -> (
    None
):
    assert not _judge(
        delivered=20_000, lags=[300 + 5 * t for t in range(21)]
    ).lag_growing


def test_only_the_second_half_of_the_step_decides_whether_the_lag_grows() -> None:
    # Steep growth during the first ten seconds, then constant.
    lags = [100 * t for t in range(11)] + [1000] * 10

    assert not _judge(delivered=20_000, lags=lags).lag_growing


def test_every_reason_of_a_failed_step_is_named() -> None:
    outcome = StepOutcome(True, True, True)

    assert outcome.reason == "generator limited, latency target missed, lag growing"


def test_the_first_failure_of_a_ramp_is_found() -> None:
    ok = StepOutcome(False, False, False)
    bad = StepOutcome(False, True, False)

    assert first_failure([ok, ok, bad, ok]) == 2
    assert first_failure([ok, ok]) is None
    assert first_failure([]) is None


def test_the_floor_of_a_lag_that_builds_a_backlog_rises_with_the_backlog() -> None:
    samples = _samples([100 + 10 * t for t in range(30)])

    assert lag_floor_growth_per_s(samples, 0.0, 29.0, 5.0) == pytest.approx(10.0)


def test_the_floor_of_a_sawtooth_of_bursts_does_not_rise() -> None:
    # A burst of 1000 messages every other second that is processed within the
    # second, so that every stretch of five seconds has a sample at zero.
    samples = _samples([1000 if t % 2 else 0 for t in range(40)])

    assert lag_floor_growth_per_s(samples, 0.0, 39.0, 5.0) == pytest.approx(0.0)


def test_a_burst_sawtooth_can_mimic_growth_in_a_line_but_not_in_the_floor() -> None:
    # Bursts that happen to be sampled more often late in the window tilt a line
    # through the samples, although every stretch of five seconds returns to zero.
    lags = [0] * 10 + [0, 0, 0, 1000, 1000, 1000, 0, 0, 1000, 1000, 1000]
    samples = _samples(lags)

    assert lag_growth_per_s(samples, 10.0, 20.0) > 10.0
    assert lag_floor_growth_per_s(samples, 10.0, 20.0, 5.0) == pytest.approx(0.0)
    assert not _judge(delivered=20_000, lags=lags).lag_growing


def test_the_floor_of_an_empty_window_does_not_grow() -> None:
    assert lag_floor_growth_per_s([], 0.0, 10.0, 5.0) == 0.0


def test_a_raw_step_with_a_verdict_passes_or_fails_by_it() -> None:
    assert raw_step_passed({"outcome": {"passed": True}})
    assert not raw_step_passed({"outcome": {"passed": False}})


def test_a_raw_step_without_a_verdict_is_judged_by_its_three_flags() -> None:
    clean = {"generator_limited": False, "latency_missed": False, "lag_growing": False}

    assert raw_step_passed({"outcome": clean})
    assert not raw_step_passed({"outcome": {**clean, "lag_growing": True}})
    assert not raw_step_passed({"outcome": {**clean, "latency_missed": True}})
    assert not raw_step_passed({"outcome": {**clean, "generator_limited": True}})
