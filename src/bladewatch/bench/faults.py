"""Measurement M3: the processor is stopped and started again under load.

A producer offers a constant load, the processor is stopped after a warm-up and
started again after an outage, and the readings that were sent are compared with
those that were stored. The processor is stopped in two ways: killed at once, which
leaves its batch uncommitted, and asked to finish its batch (A23, ADR-0027). The
times of the group protocol are those of the client defaults and are not tuned.
Implements M3 and NFR2.
"""

import logging
import re
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
from bladewatch.bench.throughput import LOAD_PREFIX
from bladewatch.config import FaultSettings, PlantSettings
from bladewatch.measure.stats import LagSample, catch_up_time_s
from bladewatch.storage import Storage

logger = logging.getLogger(__name__)

_STARTUP_TIMEOUT_S = 900.0
_PRODUCER_SLACK_S = 180.0
_REDELIVERED = re.compile(
    r"redelivered batch: (\d+) of \d+ readings were already stored"
)
MODES = ("killed", "finished")


@dataclass(frozen=True, slots=True)
class FaultRun:
    """The outcome of one stop and restart.

    Attributes:
        mode: `killed` for a stop at once or `finished` for a stop after the batch.
        repetition: Number of the run, from 1.
        streams: Readings per second that were offered.
        delivered: Readings that the broker appended.
        stored: Rows in the database.
        distinct: Distinct readings in the database.
        redelivered: Readings that the restarted processor received again and
            ignored, because they were already stored.
        backlog_at_restart: Messages that were appended and not committed when the
            processor was started again.
        first_commit_s: Seconds from the start of the replacement until the first
            commit, or None.
        catch_up_s: Seconds from the start of the replacement until the lag was at
            most one second of the load, or None.
        drain_s: Seconds from the end of the load until the lag was zero, or None.
        max_lag: Largest lag in messages.
        samples: The lag samples, timed from the start of the run.
        restart_s: Time of the start of the replacement in the samples.
        stop_s: Time of the stop of the first processor in the samples.
    """

    mode: str
    repetition: int
    streams: int
    delivered: int
    stored: int
    distinct: int
    redelivered: int
    backlog_at_restart: int
    first_commit_s: float | None
    catch_up_s: float | None
    drain_s: float | None
    max_lag: int
    samples: list[LagSample]
    restart_s: float
    stop_s: float

    @property
    def missing(self) -> int:
        """Returns the readings that were sent and are not stored."""
        return self.delivered - self.distinct

    @property
    def duplicated(self) -> int:
        """Returns the rows beyond the distinct readings, which are stored twice."""
        return self.stored - self.distinct


def count_redelivered(log_text: str) -> int:
    """Sums the readings that the log reports as received again."""
    return sum(int(match.group(1)) for match in _REDELIVERED.finditer(log_text))


def first_commit_after(samples: list[LagSample], moment_s: float) -> float | None:
    """Returns the seconds from a moment to the first growth of the committed offset."""
    before = max(
        (s.committed_offset for s in samples if s.time_s <= moment_s), default=0
    )
    for sample in samples:
        if sample.time_s > moment_s and sample.committed_offset > before:
            return sample.time_s - moment_s
    return None


def lag_at(samples: list[LagSample], moment_s: float) -> int:
    """Returns the lag of the last sample at or before a moment, or zero."""
    earlier = [s.lag for s in samples if s.time_s <= moment_s]
    return earlier[-1] if earlier else 0


def run_fault(
    base: PlantSettings,
    mode: str,
    repetition: int,
    settings: FaultSettings,
    workdir: Path,
    clock: Callable[[], float] = time.monotonic,
) -> FaultRun:
    """Runs one stop and restart from an empty state.

    Args:
        base: The plant whose broker, database, and topics are used.
        mode: `killed` or `finished`.
        repetition: Number of the run, from 1.
        settings: The load and the timeline.
        workdir: Directory for the logs of the producer and the processors.
        clock: A monotonic clock. Replaceable in tests.

    Raises:
        ClusterError: If a process fails or the group does not form.
    """
    if mode not in MODES:
        raise ValueError(f"run_fault: unknown mode {mode}")
    admin = admin_client(base.kafka)
    workdir.mkdir(parents=True, exist_ok=True)
    tag = f"fault-{mode}-run{repetition}"
    with Storage(base.database) as storage:
        storage.clear_by_prefix(LOAD_PREFIX)
    empty_topics(admin, base.topics)
    group = f"bladewatch-measure-{uuid.uuid4().hex[:8]}"
    first_log = workdir / f"{tag}-processor-1.log"
    second_log = workdir / f"{tag}-processor-2.log"
    first = start_processor(settings.streams, group, first_log)
    second = None
    sampler: LagSampler | None = None
    try:
        wait_until_group_is_stable(admin, group, _STARTUP_TIMEOUT_S, first)
        origin = clock()
        sampler = LagSampler(admin, base.topics.readings, group, origin)
        sampler.start()
        producer = start_producer(
            settings.streams,
            settings.seed,
            settings.load_s,
            workdir / f"{tag}-producer.log",
        )
        _wait_for_first_message(sampler, clock)
        time.sleep(settings.warm_s)
        stop_s = clock() - origin
        stop_process(first, graceful=(mode == "finished"), timeout_s=60.0)
        time.sleep(settings.outage_s)
        restart_s = clock() - origin
        second = start_processor(settings.streams, group, second_log)
        status = producer.wait(timeout=settings.load_s + _PRODUCER_SLACK_S)
        produced = clock()
        if status != 0:
            raise ClusterError(f"the producer ended with status {status}")
        delivered = sampler.sample().end_offset
        drain_s = wait_for_drain(sampler, produced, settings.drain_timeout_s, clock)
    finally:
        if sampler is not None:
            sampler.stop()
        stop_process(first, graceful=False, timeout_s=10.0)
        if second is not None:
            stop_process(second, graceful=True, timeout_s=60.0)
        delete_group(admin, group)
    samples = sampler.samples if sampler else []
    with Storage(base.database) as storage:
        counts = storage.count_by_prefix(LOAD_PREFIX)
        storage.clear_by_prefix(LOAD_PREFIX)
    log_text = "".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in (first_log, second_log)
        if path.exists()
    )
    return FaultRun(
        mode=mode,
        repetition=repetition,
        streams=settings.streams,
        delivered=delivered,
        stored=counts["readings"],
        distinct=counts["distinct_readings"],
        redelivered=count_redelivered(log_text),
        backlog_at_restart=lag_at(samples, restart_s),
        first_commit_s=first_commit_after(samples, restart_s),
        catch_up_s=catch_up_time_s(samples, restart_s, settings.streams),
        drain_s=drain_s,
        max_lag=max((s.lag for s in samples), default=0),
        samples=samples,
        restart_s=restart_s,
        stop_s=stop_s,
    )


def _wait_for_first_message(sampler: LagSampler, clock: Callable[[], float]) -> None:
    deadline = clock() + 120.0
    while clock() < deadline:
        if sampler.sample().end_offset > 0:
            return
        time.sleep(0.2)
    raise ClusterError("the producer did not deliver a message in time")


def run_faults(
    base: PlantSettings,
    settings: FaultSettings,
    workdir: Path,
    on_run: Callable[[FaultRun], None] = lambda run: None,
) -> list[FaultRun]:
    """Runs every mode for the repetitions of the settings.

    Args:
        base: The plant whose broker, database, and topics are used.
        settings: The load and the timeline.
        workdir: Directory for the logs.
        on_run: Called with every finished run, for the log and the raw file.
    """
    runs: list[FaultRun] = []
    for repetition in range(1, settings.repetitions + 1):
        for mode in MODES:
            logger.info("M3 run %d, processor %s", repetition, mode)
            run = run_fault(base, mode, repetition, settings, workdir)
            logger.info(
                "delivered %d, stored %d, missing %d, duplicated %d, redelivered %d",
                run.delivered,
                run.distinct,
                run.missing,
                run.duplicated,
                run.redelivered,
            )
            runs.append(run)
            on_run(run)
    return runs


def as_json(runs: list[FaultRun]) -> list[dict[str, object]]:
    """Returns the runs as plain values for the JSON file."""
    entries = []
    for run in runs:
        entry = asdict(run)
        entry["missing"] = run.missing
        entry["duplicated"] = run.duplicated
        entry["samples"] = [
            (s.time_s, s.end_offset, s.committed_offset) for s in run.samples
        ]
        entries.append(entry)
    return entries
