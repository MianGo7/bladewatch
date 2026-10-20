"""Access to the broker and to the processes of a measurement.

Reads the offsets and the consumer lag of a group, empties the topics between runs,
waits for a consumer group to become stable, and starts the producer and the
processor as processes of their own, so that the load generator and the processor
do not share a Python interpreter (ADR-0024). Implements the observation of M2 and
M3.
"""

import logging
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from bladewatch.config import KafkaSettings, TopicSettings
from bladewatch.measure.stats import LagSample

logger = logging.getLogger(__name__)

_NO_OFFSET = -1001
_ADMIN_TIMEOUT_S = 30.0


class ClusterError(RuntimeError):
    """The broker or a process of the measurement did not behave as needed."""


def admin_client(kafka: KafkaSettings) -> AdminClient:
    """Creates the administration client of the broker."""
    return AdminClient(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "broker.address.family": "v4",
        }
    )


def partitions_of(admin: AdminClient, topic: str) -> list[int]:
    """Returns the partition numbers of a topic in ascending order."""
    metadata = admin.list_topics(topic, timeout=_ADMIN_TIMEOUT_S)
    return sorted(metadata.topics[topic].partitions)


def end_offsets(
    admin: AdminClient, topic: str, partitions: list[int]
) -> dict[int, int]:
    """Returns the offset that the next message of each partition receives."""
    futures = admin.list_offsets(
        {TopicPartition(topic, p): OffsetSpec.latest() for p in partitions}
    )
    return {
        tp.partition: future.result(timeout=_ADMIN_TIMEOUT_S).offset
        for tp, future in futures.items()
    }


def message_count(admin: AdminClient, topic: str) -> int:
    """Returns the number of messages that a topic holds."""
    partitions = partitions_of(admin, topic)
    earliest = admin.list_offsets(
        {TopicPartition(topic, p): OffsetSpec.earliest() for p in partitions}
    )
    first = {
        tp.partition: future.result(timeout=_ADMIN_TIMEOUT_S).offset
        for tp, future in earliest.items()
    }
    last = end_offsets(admin, topic, partitions)
    return sum(last[p] - first[p] for p in partitions)


def committed_offsets(
    admin: AdminClient, group: str, topic: str, partitions: list[int]
) -> dict[int, int]:
    """Returns the committed offset of a group for the partitions that have one."""
    request = ConsumerGroupTopicPartitions(
        group, [TopicPartition(topic, p) for p in partitions]
    )
    result = admin.list_consumer_group_offsets([request])[group].result(
        timeout=_ADMIN_TIMEOUT_S
    )
    return {
        tp.partition: tp.offset
        for tp in result.topic_partitions
        if tp.offset != _NO_OFFSET
    }


def empty_topics(admin: AdminClient, topics: TopicSettings) -> None:
    """Deletes every message of the readings topic and of the alert topic.

    The offsets keep counting, so the measurements work with differences.
    """
    ends = []
    for topic in (topics.readings, topics.alerts):
        for partition, offset in end_offsets(
            admin, topic, partitions_of(admin, topic)
        ).items():
            if offset > 0:
                ends.append(TopicPartition(topic, partition, offset))
    if not ends:
        return
    for future in admin.delete_records(ends).values():
        future.result(timeout=_ADMIN_TIMEOUT_S)


def wait_until_group_is_stable(
    admin: AdminClient, group: str, timeout_s: float, process: subprocess.Popen[bytes]
) -> None:
    """Waits until a consumer group has a member and is stable.

    Args:
        admin: The administration client.
        group: The consumer group.
        timeout_s: Longest wait in seconds.
        process: The processor, which is checked so that a crash ends the wait.

    Raises:
        ClusterError: If the group does not become stable or the processor ends.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise ClusterError(f"the processor ended with status {process.returncode}")
        description = admin.describe_consumer_groups([group])[group].result(
            timeout=_ADMIN_TIMEOUT_S
        )
        if description.members and str(description.state).endswith("STABLE"):
            return
        time.sleep(0.25)
    raise ClusterError("the processor did not join its consumer group in time")


def delete_group(admin: AdminClient, group: str) -> None:
    """Deletes a consumer group, which has to be empty."""
    try:
        admin.delete_consumer_groups([group])[group].result(timeout=_ADMIN_TIMEOUT_S)
    except Exception as error:  # the group may already be gone
        logger.debug("the group %s was not deleted: %s", group, error)


class LagSampler:
    """Samples the offsets of a topic and of a consumer group once per interval.

    The offsets are counted from the moment of creation, so that the messages of
    earlier runs, which the topic keeps counting, do not enter the lag.
    """

    def __init__(
        self,
        admin: AdminClient,
        topic: str,
        group: str,
        origin: float,
        interval_s: float = 1.0,
    ) -> None:
        """Creates a sampler and takes the baseline.

        Args:
            admin: The administration client.
            topic: The topic that the group reads.
            group: The consumer group.
            origin: The `time.monotonic()` value that counts as time zero.
            interval_s: Seconds between two samples.
        """
        self._admin = admin
        self._topic = topic
        self._group = group
        self._origin = origin
        self._interval_s = interval_s
        self._partitions = partitions_of(admin, topic)
        self._baseline = end_offsets(admin, topic, self._partitions)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._samples: list[LagSample] = []

    def start(self) -> None:
        """Starts sampling."""
        self._thread.start()

    def stop(self) -> None:
        """Stops sampling and waits for the thread."""
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=10.0)

    @property
    def samples(self) -> list[LagSample]:
        """Returns every sample taken so far, in the order of their time."""
        return sorted(self._samples, key=lambda sample: sample.time_s)

    def sample(self) -> LagSample:
        """Reads the offsets once, keeps the sample, and returns it."""
        ends = end_offsets(self._admin, self._topic, self._partitions)
        committed = committed_offsets(
            self._admin, self._group, self._topic, self._partitions
        )
        base = sum(self._baseline.values())
        taken = LagSample(
            time_s=time.monotonic() - self._origin,
            end_offset=sum(ends.values()) - base,
            # A partition without a commit has consumed nothing since the baseline.
            committed_offset=sum(
                committed.get(p, self._baseline[p]) for p in self._partitions
            )
            - base,
        )
        self._samples.append(taken)
        return taken

    def partition_messages(self) -> dict[int, int]:
        """Returns the messages that each partition received since the baseline."""
        ends = end_offsets(self._admin, self._topic, self._partitions)
        return {p: ends[p] - self._baseline[p] for p in self._partitions}

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self.sample()
            except Exception as error:  # a failed sample is a gap, not a failure
                logger.warning("a lag sample failed: %s", error)
            self._stop.wait(max(0.0, self._interval_s - (time.monotonic() - started)))


def wait_for_drain(
    sampler: LagSampler,
    produced: float,
    timeout_s: float,
    clock: Callable[[], float] = time.monotonic,
) -> float | None:
    """Waits until the lag is zero and returns the seconds since the load ended.

    Args:
        sampler: The sampler of the group.
        produced: The time of the end of the load on the clock.
        timeout_s: Longest wait in seconds.
        clock: A monotonic clock. Replaceable in tests.

    Returns:
        The seconds from the end of the load to the first zero lag, or None if the
        lag did not reach zero within the timeout.
    """
    deadline = clock() + timeout_s
    while clock() < deadline:
        if sampler.sample().lag == 0:
            return clock() - produced
        time.sleep(0.5)
    return None


def _command(arguments: list[str]) -> list[str]:
    return [sys.executable, "-m", "bladewatch", *arguments]


def start_processor(
    streams: int, group: str, log_path: Path
) -> subprocess.Popen[bytes]:
    """Starts a processor for a number of streams as a process of its own.

    Args:
        streams: Number of streams, which has to equal the producer's.
        group: The consumer group.
        log_path: File that receives the log of the processor.
    """
    log = log_path.open("ab")
    try:
        return subprocess.Popen(
            _command(["process", "--machines", str(streams), "--group", group]),
            stdout=log,
            stderr=log,
        )
    finally:
        log.close()


def start_producer(
    streams: int, seed: int, duration_s: float, log_path: Path
) -> subprocess.Popen[bytes]:
    """Starts a producer for a number of streams as a process of its own.

    Args:
        streams: Number of streams, one reading per second each.
        seed: Seed of the simulation.
        duration_s: Seconds of load.
        log_path: File that receives the log of the producer.
    """
    log = log_path.open("ab")
    try:
        return subprocess.Popen(
            _command(
                [
                    "produce",
                    "--machines",
                    str(streams),
                    "--seed",
                    str(seed),
                    "--duration-s",
                    str(duration_s),
                ]
            ),
            stdout=log,
            stderr=log,
        )
    finally:
        log.close()


def stop_process(
    process: subprocess.Popen[bytes], graceful: bool, timeout_s: float
) -> int:
    """Stops a process and returns its exit status.

    Args:
        process: The process.
        graceful: True to ask it to finish its batch (SIGTERM), False to kill it
            at once (SIGKILL), which leaves its batch uncommitted.
        timeout_s: Longest wait for a graceful stop in seconds.
    """
    if process.poll() is None:
        if graceful:
            process.terminate()
        else:
            process.kill()
    try:
        return process.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=10.0)
