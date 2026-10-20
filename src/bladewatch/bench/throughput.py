"""Measurement M2: offered against achieved readings per second.

Runs a ramp of load steps (A9). A producer process offers every step as one stream per
reading and per second, a processor process consumes it, and the lag of the consumer
group is sampled once per second. A step passes when the producer delivers the load,
the 95th percentile of the time from measurement to storage stays within the target
of A5, and the lag does not grow continuously (A22, ADR-0026). The ramp stops at the
first step that fails, because the limit is then found. Implements M2 and NFR6.
"""

import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path

from bladewatch.bench.cluster import (
    ClusterError,
    LagSampler,
    admin_client,
    delete_group,
    empty_topics,
    start_processor,
    start_producer,
    stop_process,
    wait_for_drain,
    wait_until_group_is_stable,
)
from bladewatch.config import PlantSettings, ThroughputSettings
from bladewatch.measure.stats import (
    LagSample,
    StepOutcome,
    first_failure,
    judge_step,
    lag_floor_growth_per_s,
    lag_growth_per_s,
    rate_per_s,
)
from bladewatch.storage import ProfileBucket, StageStats, Storage

logger = logging.getLogger(__name__)

LOAD_PREFIX = "load-"
_STARTUP_TIMEOUT_S = 900.0
_PRODUCER_SLACK_S = 180.0
_SAMPLE_INTERVAL_S = 1.0
PROFILE_BUCKET_S = 10.0


@dataclass(frozen=True, slots=True)
class StepResult:
    """The outcome of one load step.

    Attributes:
        offered_per_s: Readings per second that the step offers.
        repetition: Number of the ramp, from 1.
        delivered: Readings that the broker appended during the step.
        produce_s: Seconds from the first message to the end of the producer.
        achieved_per_s: Readings stored per second over the second half of the
            load, from the committed offsets of the group.
        stored: Rows of the step in the database after the drain.
        distinct: Distinct readings of the step in the database.
        startup_s: Seconds that the processor took to join its group.
        latency: The distribution of every stage from the database, by name.
        max_lag: Largest lag of the samples in messages.
        lag_growth_per_s: Slope of the lag over the second half of the load.
        lag_floor_growth_per_s: Slope of the floor of the lag over the same half,
            which decides whether the lag grows continuously.
        drain_s: Seconds from the end of the load until the lag was zero, or None
            if it did not reach zero within the timeout.
        outcome: The verdict on the step.
        samples: The lag samples, timed from the first message.
        partition_messages: Messages that each partition of the topic received, which
            shows how evenly the default partitioner spreads the streams (A10).
        profile: The stage times of the readings by stretch of the step, which show
            when a stage slows down (ADR-0026).
    """

    offered_per_s: int
    repetition: int
    delivered: int
    produce_s: float
    achieved_per_s: float
    stored: int
    distinct: int
    startup_s: float
    latency: dict[str, StageStats]
    max_lag: int
    lag_growth_per_s: float
    lag_floor_growth_per_s: float
    drain_s: float | None
    outcome: StepOutcome
    samples: list[LagSample]
    partition_messages: dict[int, int]
    profile: list[ProfileBucket]


@dataclass(frozen=True, slots=True)
class ThroughputResult:
    """The outcome of M2.

    Attributes:
        steps: Every step that was run, in the order of the runs.
        limits: For each ramp, the first step that failed, or None if every step
            passed, by repetition.
    """

    steps: list[StepResult]
    limits: dict[int, int | None]


def load_start_s(samples: list[LagSample]) -> float:
    """Returns the time of the first sample that shows a message, or zero.

    The producer needs a moment to start, and the first message marks the start of
    the load.
    """
    return next((s.time_s for s in samples if s.end_offset > 0), 0.0)


def shift_samples(samples: list[LagSample]) -> list[LagSample]:
    """Times the samples from the first one that shows a message."""
    start = load_start_s(samples)
    return [
        LagSample(s.time_s - start, s.end_offset, s.committed_offset) for s in samples
    ]


def run_step(
    base: PlantSettings,
    step: int,
    repetition: int,
    settings: ThroughputSettings,
    workdir: Path,
    clock: Callable[[], float] = time.monotonic,
) -> StepResult:
    """Runs one load step from an empty state and judges it.

    Args:
        base: The plant whose broker, database, and topics are used.
        step: Readings per second to offer, which is the number of streams.
        repetition: Number of the ramp, from 1.
        settings: The criteria and the duration.
        workdir: Directory for the logs of the producer and the processor.
        clock: A monotonic clock. Replaceable in tests.

    Raises:
        ClusterError: If a process fails or the group does not form.
    """
    admin = admin_client(base.kafka)
    workdir.mkdir(parents=True, exist_ok=True)
    tag = f"step{step:06d}-run{repetition}"
    with Storage(base.database) as storage:
        storage.clear_by_prefix(LOAD_PREFIX)
    empty_topics(admin, base.topics)
    group = f"bladewatch-measure-{uuid.uuid4().hex[:8]}"
    processor = start_processor(step, group, workdir / f"{tag}-processor.log")
    sampler: LagSampler | None = None
    try:
        began = clock()
        wait_until_group_is_stable(admin, group, _STARTUP_TIMEOUT_S, processor)
        startup_s = clock() - began
        origin = clock()
        sampler = LagSampler(
            admin, base.topics.readings, group, origin, _SAMPLE_INTERVAL_S
        )
        sampler.start()
        producer = start_producer(
            step,
            settings.seed,
            settings.step_duration_s,
            workdir / f"{tag}-producer.log",
        )
        status = producer.wait(timeout=settings.step_duration_s + _PRODUCER_SLACK_S)
        produced = clock()
        if status != 0:
            raise ClusterError(f"the producer ended with status {status}")
        final = sampler.sample()
        delivered = final.end_offset
        partition_messages = sampler.partition_messages()
        drain_s = wait_for_drain(sampler, produced, settings.drain_timeout_s, clock)
    finally:
        if sampler is not None:
            sampler.stop()
        stop_process(processor, graceful=True, timeout_s=60.0)
        delete_group(admin, group)
    samples = shift_samples(sampler.samples if sampler else [])
    with Storage(base.database) as storage:
        counts = storage.count_by_prefix(LOAD_PREFIX)
        summary = storage.stage_latencies_by_prefix(LOAD_PREFIX)
        profile = storage.stage_profile_by_prefix(LOAD_PREFIX, PROFILE_BUCKET_S)
        storage.clear_by_prefix(LOAD_PREFIX)
    duration_s = settings.step_duration_s
    stored_stage = summary.stages["measured to stored"] if summary.stages else None
    p95 = stored_stage.p95_s if stored_stage else float("inf")
    produce_s = produced - origin - load_start_s(sampler.samples if sampler else [])
    return StepResult(
        offered_per_s=step,
        repetition=repetition,
        delivered=delivered,
        produce_s=produce_s,
        achieved_per_s=rate_per_s(samples, duration_s / 2.0, duration_s),
        stored=counts["readings"],
        distinct=counts["distinct_readings"],
        startup_s=startup_s,
        latency=dict(summary.stages),
        max_lag=max((s.lag for s in samples), default=0),
        lag_growth_per_s=lag_growth_per_s(samples, duration_s / 2.0, duration_s),
        lag_floor_growth_per_s=lag_floor_growth_per_s(
            samples, duration_s / 2.0, duration_s, settings.lag_window_s
        ),
        drain_s=drain_s,
        outcome=judge_step(step, delivered, duration_s, p95, samples, settings),
        samples=samples,
        partition_messages=partition_messages,
        profile=profile,
    )


def run_throughput(
    base: PlantSettings,
    settings: ThroughputSettings,
    workdir: Path,
    on_step: Callable[[StepResult], None] = lambda result: None,
) -> ThroughputResult:
    """Runs the ramp for every repetition, each stopping at its first failure.

    Args:
        base: The plant whose broker, database, and topics are used.
        settings: The steps, the duration, and the criteria.
        workdir: Directory for the logs.
        on_step: Called with every finished step, for the log and the raw file.
    """
    results: list[StepResult] = []
    limits: dict[int, int | None] = {}
    for repetition in range(1, settings.repetitions + 1):
        outcomes: list[StepOutcome] = []
        for step in settings.steps:
            logger.info("M2 run %d, step %d readings per second", repetition, step)
            result = run_step(base, step, repetition, settings, workdir)
            logger.info(
                "offered %d, delivered %d, achieved %.0f per second, %s",
                step,
                result.delivered,
                result.achieved_per_s,
                result.outcome.reason,
            )
            results.append(result)
            outcomes.append(result.outcome)
            on_step(result)
            if not result.outcome.passed:
                break
        index = first_failure(outcomes)
        limits[repetition] = None if index is None else settings.steps[index]
    return ThroughputResult(steps=results, limits=limits)


def as_json(result: ThroughputResult) -> dict[str, object]:
    """Returns the result as a structure of plain values for the JSON file."""
    steps = []
    for step in result.steps:
        entry = asdict(step)
        entry["outcome"] = {
            **asdict(step.outcome),
            "passed": step.outcome.passed,
            "reason": step.outcome.reason,
        }
        entry["partition_messages"] = {
            str(p): n for p, n in step.partition_messages.items()
        }
        entry["profile"] = [
            {
                "start_s": bucket.start_s,
                "readings": bucket.readings,
                "stages": {n: asdict(v) for n, v in bucket.stages.items()},
            }
            for bucket in step.profile
        ]
        entry["samples"] = [
            (s.time_s, s.end_offset, s.committed_offset) for s in step.samples
        ]
        steps.append(entry)
    return {"steps": steps, "limits": {str(k): v for k, v in result.limits.items()}}
