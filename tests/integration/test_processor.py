import dataclasses
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest
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
from bladewatch.storage import BatchResult, Storage

from .stack import compose, delete_consumer_group, sql

pytestmark = pytest.mark.integration

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


@pytest.fixture
def environment() -> Iterator[Environment]:
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
        compose(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            BROKER_IN_CONTAINER,
            "--delete",
            "--group",
            f"group-{run_id}",
            check=False,
        )
        sql(f"DELETE FROM readings WHERE machine_id LIKE '{prefix}%'")
        sql(f"DELETE FROM alerts WHERE machine_id LIKE '{prefix}%'")


def _ticks(environment: Environment, first: int, last: int) -> list[list[Reading]]:
    """The readings of the ticks from first up to, not including, last."""
    generated = generate_ticks(
        environment.plant, SEED, START, {environment.mould_id: OVERHEATING}
    )
    return list(islice(generated, first, last))


def _publish(environment: Environment, ticks: Sequence[Sequence[Reading]]) -> None:
    publisher = ReadingPublisher(
        create_kafka_producer(environment.plant.kafka),
        environment.plant.topics.readings,
    )
    for tick in ticks:
        for reading in tick:
            publisher.publish(reading)
    publisher.flush()


def _expected_events(
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


def _stored_events(environment: Environment) -> list[tuple[str, str, str, str]]:
    rows = sql(
        "SELECT alert_id, kind, severity, status FROM alerts "
        f"WHERE machine_id LIKE '{environment.prefix}%' ORDER BY measured_at, status"
    )
    return [(a, k, s, t) for a, k, s, t in rows]


def _stored_count(environment: Environment) -> int:
    return int(
        sql(
            "SELECT count(*) FROM readings "
            f"WHERE machine_id LIKE '{environment.prefix}%'"
        )[0][0]
    )


def _wait_for(condition: Callable[[], bool], timeout_s: float = 90.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    raise AssertionError("the condition was not met in time")


def _alert_messages(environment: Environment, expected: int) -> list[Alert]:
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


def _run_until_stored(environment: Environment, expected_rows: int) -> None:
    processor = RunningProcessor(environment)
    processor.start()
    try:
        _wait_for(lambda: _stored_count(environment) >= expected_rows)
    finally:
        processor.stop()
    assert processor.error is None


def test_every_published_reading_is_stored_exactly_once(
    environment: Environment,
) -> None:
    ticks = _ticks(environment, 0, 5)
    _publish(environment, ticks)

    _run_until_stored(environment, 15)

    rows = sql(
        "SELECT count(*), count(DISTINCT (machine_id, sequence)) FROM readings "
        f"WHERE machine_id LIKE '{environment.prefix}%'"
    )
    assert rows == [["15", "15"]]


def test_the_four_timestamps_of_a_stored_reading_follow_the_stages(
    environment: Environment,
) -> None:
    now = datetime.now(UTC) - timedelta(seconds=5)
    publisher = ReadingPublisher(
        create_kafka_producer(environment.plant.kafka),
        environment.plant.topics.readings,
    )
    publisher.publish(Reading(environment.mould_id, 0, now, 70.0))
    publisher.flush()

    _run_until_stored(environment, 1)

    ordered = sql(
        "SELECT measured_at <= kafka_appended_at AND kafka_appended_at <= processed_at "
        "AND processed_at <= stored_at FROM readings "
        f"WHERE machine_id = '{environment.mould_id}'"
    )
    assert ordered == [["t"]]


def test_an_overheating_series_produces_the_expected_alerts_in_the_table_and_topic(
    environment: Environment,
) -> None:
    ticks = _ticks(environment, 0, TICKS)
    expected = _expected_events(environment, ticks)
    _publish(environment, ticks)

    _run_until_stored(environment, TICKS * 3)

    assert [e[1:] for e in expected] == [
        ("rate_of_rise", "warning", "raised"),
        ("threshold", "critical", "raised"),
    ]
    assert _stored_events(environment) == expected
    messages = _alert_messages(environment, expected=len(expected))
    assert [(a.alert_id, a.kind, a.severity, a.status) for a in messages] == expected
    assert all(a.machine_id == environment.mould_id for a in messages)


def test_a_restart_in_the_middle_of_an_open_alert_loses_and_duplicates_nothing(
    environment: Environment,
) -> None:
    ticks = _ticks(environment, 0, TICKS)
    expected = _expected_events(environment, ticks)
    # The rate warning opens at second 134 and the critical alert at second 399,
    # so the alert is open when the processor stops after 250 seconds.
    _publish(environment, ticks[:250])
    _run_until_stored(environment, 250 * 3)
    assert len(_stored_events(environment)) == 1
    _publish(environment, ticks[250:])

    _run_until_stored(environment, TICKS * 3)

    rows = sql(
        "SELECT count(*), count(DISTINCT (machine_id, sequence)) FROM readings "
        f"WHERE machine_id LIKE '{environment.prefix}%'"
    )
    assert rows == [[str(TICKS * 3), str(TICKS * 3)]]
    assert _stored_events(environment) == expected
    messages = _alert_messages(environment, expected=len(expected))
    assert [(a.alert_id, a.kind, a.severity, a.status) for a in messages] == expected


class _CrashOnCommitAfterAlert:
    """Fails the commit of the offsets of the batch whose alert was published."""

    def __init__(self, consumer: object, published: Callable[[], bool]) -> None:
        self._consumer = consumer
        self._published = published
        self.crashed = False

    def commit(self, asynchronous: bool) -> object:
        if self._published() and not self.crashed:
            self.crashed = True
            raise RuntimeError("simulated crash after the database commit")
        return self._consumer.commit(asynchronous=asynchronous)

    def __getattr__(self, name: str) -> object:
        return getattr(self._consumer, name)


def test_a_crash_between_the_database_commit_and_the_offset_commit_changes_nothing(
    environment: Environment,
) -> None:
    ticks = _ticks(environment, 0, TICKS)
    expected = _expected_events(environment, ticks)
    _publish(environment, ticks)
    crashing = RunningProcessor(
        environment,
        wrap_consumer=lambda consumer: _CrashOnCommitAfterAlert(
            consumer, lambda: len(_stored_events(environment)) > 0
        ),
    )
    crashing.start()
    _wait_for(lambda: crashing.finished)
    crashing.stop()
    stored_before_restart = _stored_count(environment)

    _run_until_stored(environment, TICKS * 3)

    assert isinstance(crashing.error, RuntimeError)
    assert 0 < stored_before_restart < TICKS * 3
    rows = sql(
        "SELECT count(*), count(DISTINCT (machine_id, sequence)) FROM readings "
        f"WHERE machine_id LIKE '{environment.prefix}%'"
    )
    assert rows == [[str(TICKS * 3), str(TICKS * 3)]]
    assert _stored_events(environment) == expected
    # The alert was published before the crash and is not published again,
    # because the restored rules have already seen its reading.
    messages = _alert_messages(environment, expected=len(expected))
    assert [(a.alert_id, a.kind, a.severity, a.status) for a in messages] == expected


class _FailFirstStoreWithAlert:
    """A storage that rejects the first batch that carries an alert event."""

    def __init__(self, storage: Storage) -> None:
        self._storage = storage
        self.failed = False

    def store_batch(
        self, readings: Sequence[object], alerts: Sequence[object]
    ) -> BatchResult:
        if alerts and not self.failed:
            self.failed = True
            raise RuntimeError("simulated database failure")
        return self._storage.store_batch(readings, alerts)

    def __getattr__(self, name: str) -> object:
        return getattr(self._storage, name)


def test_an_alert_published_before_a_failed_database_write_is_published_again(
    environment: Environment,
) -> None:
    ticks = _ticks(environment, 0, TICKS)
    expected = _expected_events(environment, ticks)
    _publish(environment, ticks)
    storage = Storage(DatabaseSettings())
    storage.connect()
    failing = _FailFirstStoreWithAlert(storage)
    crashed = StreamProcessor(
        environment.plant,
        create_kafka_consumer(environment.plant),
        create_kafka_producer(environment.plant.kafka),
        failing,
    )
    thread_error: list[BaseException] = []

    def run_until_failure() -> None:
        try:
            crashed.run(lambda: False)
        except BaseException as error:  # the test reports it
            thread_error.append(error)

    thread = threading.Thread(target=run_until_failure, daemon=True)
    thread.start()
    thread.join(timeout=60.0)
    storage.close()

    _run_until_stored(environment, TICKS * 3)

    assert thread_error and isinstance(thread_error[0], RuntimeError)
    assert _stored_events(environment) == expected
    # At least once: the alert of the failed batch reached the topic twice, and
    # a consumer recognises the repeat by its identifier and its severity.
    messages = _alert_messages(environment, expected=len(expected) + 1)
    assert {(a.alert_id, a.kind, a.severity, a.status) for a in messages} == set(
        expected
    )
    assert len(messages) >= len(expected)
