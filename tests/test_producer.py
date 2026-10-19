from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest

from bladewatch.config import KafkaSettings, default_plant
from bladewatch.messages import Reading
from bladewatch.producer import (
    PublishError,
    ReadingPublisher,
    generate_ticks,
    producer_config,
    run,
)
from bladewatch.sensor import Overheating

START = datetime(2026, 10, 19, 9, 0, 0, tzinfo=UTC)
SEED = 20261019


class _Message:
    pass


class FakeClient:
    """Replaces the Kafka producer and delivers on poll, as the real one does."""

    def __init__(self, buffer_errors: int = 0, fail_delivery: bool = False) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.polls: list[float] = []
        self._pending: list[Callable[[object, object], None]] = []
        self._buffer_errors = buffer_errors
        self._fail_delivery = fail_delivery
        self.undelivered_on_flush = 0

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[object, object], None],
    ) -> None:
        if self._buffer_errors:
            self._buffer_errors -= 1
            raise BufferError("queue full")
        self.sent.append((topic, key, value))
        self._pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        error = "broker down" if self._fail_delivery else None
        callbacks, self._pending = self._pending, []
        for callback in callbacks:
            callback(error, _Message())
        return len(callbacks)

    def flush(self, timeout: float) -> int:
        self.poll(0.0)
        return self.undelivered_on_flush


class FakeClock:
    """A clock that only advances when the code sleeps."""

    def __init__(self) -> None:
        self.now = 100.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _ticks(count: int, seed: int = SEED, **kwargs: object) -> list[list[Reading]]:
    return list(islice(generate_ticks(default_plant(), seed, START, **kwargs), count))


def test_every_tick_holds_one_reading_for_each_of_the_twelve_machines() -> None:
    tick = _ticks(1)[0]

    assert len(tick) == 12
    assert len({reading.machine_id for reading in tick}) == 12


def test_the_sequence_of_every_machine_counts_up_from_zero_without_a_gap() -> None:
    ticks = _ticks(5)

    for machine_index in range(12):
        assert [tick[machine_index].sequence for tick in ticks] == [0, 1, 2, 3, 4]


def test_the_time_of_measurement_advances_by_the_sensor_interval() -> None:
    ticks = _ticks(3)

    assert [tick[0].measured_at for tick in ticks] == [
        START,
        START + timedelta(seconds=1),
        START + timedelta(seconds=2),
    ]


def test_two_runs_with_the_same_seed_produce_identical_readings() -> None:
    assert _ticks(20) == _ticks(20)


def test_two_runs_with_different_seeds_produce_different_readings() -> None:
    assert _ticks(5, seed=1) != _ticks(5, seed=2)


def test_the_readings_of_a_machine_do_not_depend_on_an_overheating_elsewhere() -> None:
    plain = _ticks(10)
    injected = _ticks(
        10, overheating={"oven-02": Overheating(start_s=0.0, rate_c_per_min=60.0)}
    )

    oven_01 = 4  # position of oven-01 in the plant
    assert [t[oven_01] for t in plain] == [t[oven_01] for t in injected]


def test_an_injected_overheating_raises_the_temperature_of_its_machine_only() -> None:
    ticks = _ticks(
        120, overheating={"mould-01": Overheating(start_s=0.0, rate_c_per_min=60.0)}
    )

    # One degree Celsius per second, so about 119 degrees above normal at the
    # last tick. The noise has a standard deviation of 0.5 degrees Celsius.
    assert ticks[-1][0].temperature_c > 70.0 + 110.0
    assert ticks[-1][1].temperature_c < 70.0 + 5.0


def test_the_publisher_sends_the_reading_to_its_topic_keyed_by_machine() -> None:
    client = FakeClient()
    reading = _ticks(1)[0][0]

    ReadingPublisher(client, "machine-temperature").publish(reading)

    assert client.sent == [
        ("machine-temperature", b"mould-01", reading.to_json().encode("utf-8"))
    ]


def test_the_publisher_counts_delivered_readings() -> None:
    client = FakeClient()
    publisher = ReadingPublisher(client, "machine-temperature")

    for reading in _ticks(1)[0]:
        publisher.publish(reading)
    publisher.flush()

    assert (publisher.delivered, publisher.failed) == (12, 0)


def test_flush_reports_a_failed_delivery() -> None:
    publisher = ReadingPublisher(FakeClient(fail_delivery=True), "machine-temperature")
    publisher.publish(_ticks(1)[0][0])

    with pytest.raises(PublishError, match="1 failed"):
        publisher.flush()


def test_flush_reports_readings_that_the_broker_did_not_acknowledge() -> None:
    client = FakeClient()
    client.undelivered_on_flush = 3
    publisher = ReadingPublisher(client, "machine-temperature")

    with pytest.raises(PublishError, match="3 readings undelivered"):
        publisher.flush()


def test_the_publisher_serves_callbacks_and_retries_when_the_queue_is_full() -> None:
    client = FakeClient(buffer_errors=2)
    publisher = ReadingPublisher(client, "machine-temperature")

    publisher.publish(_ticks(1)[0][0])

    assert len(client.sent) == 1
    assert client.polls[:2] == [1.0, 1.0]


def test_the_publisher_gives_up_when_the_queue_never_empties() -> None:
    publisher = ReadingPublisher(FakeClient(buffer_errors=100), "machine-temperature")

    with pytest.raises(PublishError, match="queue"):
        publisher.publish(_ticks(1)[0][0])


def test_the_producer_is_configured_for_acknowledgement_by_all_and_idempotence() -> (
    None
):
    config = producer_config(KafkaSettings(bootstrap_servers="broker:9092"))

    assert config["bootstrap.servers"] == "broker:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["broker.address.family"] == "v4"


def test_a_run_publishes_the_whole_ticks_that_fit_into_its_duration() -> None:
    client = FakeClient()
    clock = FakeClock()

    ticks = run(
        default_plant(),
        SEED,
        ReadingPublisher(client, "machine-temperature"),
        START,
        duration_s=5.5,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    assert ticks == 5
    assert len(client.sent) == 5 * 12


def test_a_run_waits_one_interval_between_two_ticks() -> None:
    clock = FakeClock()

    run(
        default_plant(),
        SEED,
        ReadingPublisher(FakeClient(), "machine-temperature"),
        START,
        duration_s=5.0,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    assert clock.sleeps == [1.0, 1.0, 1.0, 1.0]


def test_a_run_flushes_even_when_publishing_fails() -> None:
    client = FakeClient(buffer_errors=100)

    with pytest.raises(PublishError):
        run(
            default_plant(),
            SEED,
            ReadingPublisher(client, "machine-temperature"),
            START,
            duration_s=2.0,
            sleep=FakeClock().sleep,
            monotonic=FakeClock().monotonic,
        )

    assert client.polls[-1] == 0.0
