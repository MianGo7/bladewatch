"""Shared parts of the integration tests that run the whole pipeline.

An isolated environment with topics, a consumer group, and machine identifiers of
its own, helpers that publish readings and run a processor, and the expected
events that the pure rules give for the same readings.
"""

import contextlib
import dataclasses
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from itertools import islice

from confluent_kafka import Consumer

from bladewatch.config import (
    GRINDER,
    MOULD,
    OVEN,
    DatabaseSettings,
    Machine,
    PlantSettings,
    ProcessorSettings,
    TopicSettings,
)
from bladewatch.messages import Alert, Reading
from bladewatch.processor import (
    StreamProcessor,
    create_kafka_consumer,
)
from bladewatch.producer import (
    ReadingPublisher,
    create_kafka_producer,
    generate_ticks,
)
from bladewatch.rules import create_trackers
from bladewatch.sensor import Overheating
from bladewatch.storage import Storage

from .stack import compose, delete_consumer_group, sql

KAFKA_TOPICS = "/opt/kafka/bin/kafka-topics.sh"
BROKER_IN_CONTAINER = "localhost:29092"
SEED = 20261019
START = (datetime.now(UTC) - timedelta(hours=2)).replace(microsecond=0)
# Normal operation for 100 seconds, then a rise of 5 degrees Celsius per
# minute, as in the demonstration scenario (A16).
OVERHEATING = Overheating(start_s=100.0, rate_c_per_min=5.0)
TICKS = 400


@dataclasses.dataclass
class Environment:
    plant: PlantSettings
    prefix: str
    mould_id: str


def make_ticks(environment: Environment, first: int, last: int) -> list[list[Reading]]:
    """The readings of the ticks from first up to, not including, last."""
    generated = generate_ticks(
        environment.plant, SEED, START, {environment.mould_id: OVERHEATING}
    )
    return list(islice(generated, first, last))


def publish(environment: Environment, ticks: Sequence[Sequence[Reading]]) -> None:
    publisher = ReadingPublisher(
        create_kafka_producer(environment.plant.kafka),
        environment.plant.topics.readings,
    )
    for tick in ticks:
        for reading in tick:
            publisher.publish(reading)
    publisher.flush()


def expected_events(
    environment: Environment, ticks: Sequence[Sequence[Reading]]
) -> list[tuple[str, str, str, str]]:
    """What the rules alone make of the readings, as the oracle of the tests."""
    trackers = create_trackers(environment.plant)
    events = []
    for tick in ticks:
        for reading in tick:
            event = trackers[reading.machine_id].observe(
                reading.measured_at, reading.temperature_c
            )
            if event is not None:
                events.append(
                    (event.alert_id, event.kind, event.severity, event.status)
                )
    return [(a, str(k), str(s), str(t)) for a, k, s, t in events]


def stored_events(environment: Environment) -> list[tuple[str, str, str, str]]:
    rows = sql(
        "SELECT alert_id, kind, severity, status FROM alerts "
        f"WHERE machine_id LIKE '{environment.prefix}%' ORDER BY measured_at, status"
    )
    return [(a, k, s, t) for a, k, s, t in rows]


def stored_count(environment: Environment) -> int:
    return int(
        sql(
            "SELECT count(*) FROM readings "
            f"WHERE machine_id LIKE '{environment.prefix}%'"
        )[0][0]
    )


def wait_for(condition: Callable[[], bool], timeout_s: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError("the condition was not met in time")


def alert_messages(environment: Environment, expected: int) -> list[Alert]:
    group_id = f"check-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "broker.address.family": "v4",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    alerts: list[Alert] = []
    quiet_until = time.monotonic() + 30.0
    try:
        consumer.subscribe([environment.plant.topics.alerts])
        while time.monotonic() < quiet_until:
            message = consumer.poll(1.0)
            if message is None:
                if len(alerts) >= expected:
                    break
                continue
            assert message.error() is None
            alerts.append(Alert.from_json(message.value()))
            if len(alerts) >= expected:
                # One more second shows whether more messages follow.
                quiet_until = min(quiet_until, time.monotonic() + 2.0)
    finally:
        consumer.close()
        delete_consumer_group(group_id)
    return alerts


class RunningProcessor:
    """A processor in a thread of its own, with its own connections."""

    def __init__(
        self,
        environment: Environment,
        wrap_consumer: Callable[[object], object] = lambda consumer: consumer,
    ) -> None:
        self.error: BaseException | None = None
        self._storage = Storage(DatabaseSettings())
        self._storage.connect()
        self._processor = StreamProcessor(
            environment.plant,
            wrap_consumer(create_kafka_consumer(environment.plant)),
            create_kafka_producer(environment.plant.kafka),
            self._storage,
        )
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=30.0)
        self._storage.close()

    @property
    def finished(self) -> bool:
        return not self._thread.is_alive()

    def _run(self) -> None:
        try:
            self._processor.run(self._stop.is_set)
        except BaseException as error:  # the test reports it
            self.error = error


def run_until_stored(environment: Environment, expected_rows: int) -> None:
    processor = RunningProcessor(environment)
    processor.start()
    try:
        wait_for(lambda: stored_count(environment) >= expected_rows)
    finally:
        processor.stop()
    assert processor.error is None


@contextlib.contextmanager
def isolated_environment() -> Iterator[Environment]:
    """Creates topics, a consumer group, and machines of their own and removes them."""
    run_id = uuid.uuid4().hex[:8]
    prefix = f"it-{run_id}"
    readings_topic, alerts_topic = f"readings-{run_id}", f"alerts-{run_id}"
    for topic in (readings_topic, alerts_topic):
        compose(
            "exec",
            "-T",
            "kafka",
            KAFKA_TOPICS,
            "--bootstrap-server",
            BROKER_IN_CONTAINER,
            "--create",
            "--topic",
            topic,
            "--partitions",
            "3",
            "--config",
            "message.timestamp.type=LogAppendTime",
        )
    plant = PlantSettings(
        machines=(
            Machine(f"{prefix}-mould-01", MOULD),
            Machine(f"{prefix}-oven-01", OVEN),
            Machine(f"{prefix}-grinder-01", GRINDER),
        ),
        topics=TopicSettings(readings=readings_topic, alerts=alerts_topic),
        processor=ProcessorSettings(
            consumer_group=f"group-{run_id}", batch_size=100, poll_timeout_s=0.1
        ),
    )
    try:
        yield Environment(plant, prefix, f"{prefix}-mould-01")
    finally:
        for topic in (readings_topic, alerts_topic):
            compose(
                "exec",
                "-T",
                "kafka",
                KAFKA_TOPICS,
                "--bootstrap-server",
                BROKER_IN_CONTAINER,
                "--delete",
                "--topic",
                topic,
                check=False,
            )
        delete_consumer_group(f"group-{run_id}")
        sql(f"DELETE FROM readings WHERE machine_id LIKE '{prefix}%'")
        sql(f"DELETE FROM alerts WHERE machine_id LIKE '{prefix}%'")
        sql(f"DELETE FROM machines WHERE machine_id LIKE '{prefix}%'")
