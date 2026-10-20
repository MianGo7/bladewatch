from datetime import UTC, datetime, timedelta

import pytest

from bladewatch.bench.faults import (
    FaultRun,
    count_redelivered,
    first_commit_after,
    lag_at,
)
from bladewatch.bench.latency import LatencyRun, alert_latencies, evaluate
from bladewatch.measure.stats import LagSample
from bladewatch.storage import AlertEventRow

START = datetime(2026, 10, 21, 10, 0, tzinfo=UTC)
STAGES = (
    "measured to broker",
    "broker to processor",
    "processor to stored",
    "measured to stored",
)


def _event(status: str, delay_s: float) -> AlertEventRow:
    return AlertEventRow(
        alert_id="a",
        kind="threshold",
        severity="critical",
        status=status,
        temperature_c=96.0,
        limit=95.0,
        measured_at=START,
        kafka_appended_at=START,
        processed_at=START,
        stored_at=START + timedelta(seconds=delay_s),
    )


def _run(
    values: list[float], alerts: list[float] | None = None, sleep: float = 0.0
) -> LatencyRun:
    return LatencyRun(
        started_at=START,
        suspended_s=sleep,
        samples={name: values for name in STAGES},
        alert_latencies_s=alerts or [],
    )


def test_the_alert_latency_is_the_time_to_storage_of_every_raised_event() -> None:
    events = [_event("raised", 0.2), _event("closed", 9.0), _event("raised", 0.4)]

    assert alert_latencies(events) == pytest.approx([0.2, 0.4])


def test_the_pooled_distribution_covers_the_readings_of_every_run() -> None:
    result = evaluate([_run([1.0, 2.0]), _run([3.0, 4.0])])

    assert result.pooled["measured to stored"].count == 4
    assert result.pooled["measured to stored"].median_s == pytest.approx(2.5)
    assert [d["measured to stored"].count for d in result.per_run] == [2, 2]


def test_the_alerts_of_all_runs_are_pooled_and_absent_without_any() -> None:
    with_alerts = evaluate([_run([1.0], [0.1, 0.3]), _run([1.0], [0.2])])
    without = evaluate([_run([1.0])])

    assert with_alerts.alerts is not None
    assert with_alerts.alerts.count == 3
    assert without.alerts is None


def test_a_run_that_slept_for_two_seconds_is_disturbed() -> None:
    assert _run([1.0], sleep=2.0).disturbed
    assert not _run([1.0], sleep=1.9).disturbed


def test_the_discarded_runs_are_kept_next_to_the_result() -> None:
    result = evaluate([_run([1.0])], discarded=[_run([9.0], sleep=99.0)])

    assert len(result.discarded) == 1


def test_a_measurement_without_a_run_cannot_be_evaluated() -> None:
    with pytest.raises(ValueError, match="no run"):
        evaluate([])


def test_the_readings_that_a_restart_received_again_are_summed_from_the_log() -> None:
    log = (
        "INFO bladewatch.processor: restored the rules of 2000 machines\n"
        "INFO bladewatch.processor: redelivered batch: 120 of 500 readings were "
        "already stored\n"
        "INFO bladewatch.processor: redelivered batch: 30 of 500 readings were "
        "already stored\n"
    )

    assert count_redelivered(log) == 150


def test_a_log_without_a_redelivery_counts_zero() -> None:
    assert count_redelivered("INFO nothing here\n") == 0


def _samples(committed: list[int], end: int = 1000) -> list[LagSample]:
    return [LagSample(float(t), end, c) for t, c in enumerate(committed)]


def test_the_first_commit_after_a_moment_is_the_first_growth_of_the_offset() -> None:
    samples = _samples([100, 100, 100, 100, 100, 600, 900])

    assert first_commit_after(samples, 2.0) == pytest.approx(3.0)


def test_there_is_no_first_commit_when_the_offset_never_grows() -> None:
    assert first_commit_after(_samples([100] * 6), 2.0) is None


def test_the_lag_at_a_moment_is_that_of_the_last_sample_before_it() -> None:
    samples = _samples([0, 100, 400], end=500)

    assert lag_at(samples, 1.5) == 400
    assert lag_at(samples, -1.0) == 0


def _fault_run(delivered: int, stored: int, distinct: int) -> FaultRun:
    return FaultRun(
        mode="killed",
        repetition=1,
        streams=10,
        delivered=delivered,
        stored=stored,
        distinct=distinct,
        redelivered=0,
        backlog_at_restart=0,
        first_commit_s=None,
        catch_up_s=None,
        drain_s=None,
        max_lag=0,
        samples=[],
        restart_s=0.0,
        stop_s=0.0,
    )


def test_a_reading_that_was_sent_and_is_not_stored_is_missing() -> None:
    assert _fault_run(delivered=1000, stored=990, distinct=990).missing == 10


def test_a_row_beyond_the_distinct_readings_is_a_duplicate() -> None:
    run = _fault_run(delivered=1000, stored=1003, distinct=1000)

    assert (run.missing, run.duplicated) == (0, 3)
