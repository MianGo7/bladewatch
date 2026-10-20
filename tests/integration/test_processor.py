import threading
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

import pytest

from bladewatch.config import DatabaseSettings
from bladewatch.messages import Reading
from bladewatch.processor import StreamProcessor, create_kafka_consumer
from bladewatch.producer import ReadingPublisher, create_kafka_producer
from bladewatch.storage import BatchResult, Storage

from .pipeline import (
    TICKS,
    Environment,
    RunningProcessor,
    alert_messages,
    expected_events,
    make_ticks,
    publish,
    run_until_stored,
    stored_count,
    stored_events,
    wait_for,
)
from .stack import sql

pytestmark = pytest.mark.integration


def test_every_published_reading_is_stored_exactly_once(
    environment: Environment,
) -> None:
    ticks = make_ticks(environment, 0, 5)
    publish(environment, ticks)

    run_until_stored(environment, 15)

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

    run_until_stored(environment, 1)

    ordered = sql(
        "SELECT measured_at <= kafka_appended_at AND kafka_appended_at <= processed_at "
        "AND processed_at <= stored_at FROM readings "
        f"WHERE machine_id = '{environment.mould_id}'"
    )
    assert ordered == [["t"]]


def test_an_overheating_series_produces_the_expected_alerts_in_the_table_and_topic(
    environment: Environment,
) -> None:
    ticks = make_ticks(environment, 0, TICKS)
    expected = expected_events(environment, ticks)
    publish(environment, ticks)

    run_until_stored(environment, TICKS * 3)

    assert [e[1:] for e in expected] == [
        ("rate_of_rise", "warning", "raised"),
        ("threshold", "critical", "raised"),
    ]
    assert stored_events(environment) == expected
    messages = alert_messages(environment, expected=len(expected))
    assert [(a.alert_id, a.kind, a.severity, a.status) for a in messages] == expected
    assert all(a.machine_id == environment.mould_id for a in messages)


def test_a_restart_in_the_middle_of_an_open_alert_loses_and_duplicates_nothing(
    environment: Environment,
) -> None:
    ticks = make_ticks(environment, 0, TICKS)
    expected = expected_events(environment, ticks)
    # The rate warning opens at second 134 and the critical alert at second 399,
    # so the alert is open when the processor stops after 250 seconds.
    publish(environment, ticks[:250])
    run_until_stored(environment, 250 * 3)
    assert len(stored_events(environment)) == 1
    publish(environment, ticks[250:])

    run_until_stored(environment, TICKS * 3)

    rows = sql(
        "SELECT count(*), count(DISTINCT (machine_id, sequence)) FROM readings "
        f"WHERE machine_id LIKE '{environment.prefix}%'"
    )
    assert rows == [[str(TICKS * 3), str(TICKS * 3)]]
    assert stored_events(environment) == expected
    messages = alert_messages(environment, expected=len(expected))
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
    ticks = make_ticks(environment, 0, TICKS)
    expected = expected_events(environment, ticks)
    publish(environment, ticks)
    crashing = RunningProcessor(
        environment,
        wrap_consumer=lambda consumer: _CrashOnCommitAfterAlert(
            consumer, lambda: len(stored_events(environment)) > 0
        ),
    )
    crashing.start()
    wait_for(lambda: crashing.finished)
    crashing.stop()
    stored_before_restart = stored_count(environment)

    run_until_stored(environment, TICKS * 3)

    assert isinstance(crashing.error, RuntimeError)
    assert 0 < stored_before_restart < TICKS * 3
    rows = sql(
        "SELECT count(*), count(DISTINCT (machine_id, sequence)) FROM readings "
        f"WHERE machine_id LIKE '{environment.prefix}%'"
    )
    assert rows == [[str(TICKS * 3), str(TICKS * 3)]]
    assert stored_events(environment) == expected
    # The alert was published before the crash and is not published again,
    # because the restored rules have already seen its reading.
    messages = alert_messages(environment, expected=len(expected))
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
    ticks = make_ticks(environment, 0, TICKS)
    expected = expected_events(environment, ticks)
    publish(environment, ticks)
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

    run_until_stored(environment, TICKS * 3)

    assert thread_error and isinstance(thread_error[0], RuntimeError)
    assert stored_events(environment) == expected
    # At least once: the alert of the failed batch reached the topic twice, and
    # a consumer recognises the repeat by its identifier and its severity.
    messages = alert_messages(environment, expected=len(expected) + 1)
    assert {(a.alert_id, a.kind, a.severity, a.status) for a in messages} == set(
        expected
    )
    assert len(messages) >= len(expected)
