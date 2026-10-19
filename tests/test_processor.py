import dataclasses
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, TIMESTAMP_NOT_AVAILABLE

from bladewatch.config import (
    MOULD,
    Machine,
    PlantSettings,
    ProcessorSettings,
    SensorSettings,
    TopicSettings,
    default_plant,
)
from bladewatch.messages import Alert, Reading
from bladewatch.processor import (
    ProcessorError,
    StreamProcessor,
    consumer_config,
)
from bladewatch.storage import (
    AlertRow,
    BatchResult,
    OpenAlertRow,
    ReadingRow,
    StorageError,
)

START = datetime(2026, 10, 19, 9, 0, 0, tzinfo=UTC)
PROCESSED_AT = datetime(2026, 10, 19, 9, 5, 0, tzinfo=UTC)
SINGLE = PlantSettings(
    machines=(Machine("mould-01", MOULD),),
    sensor=SensorSettings(),
    processor=ProcessorSettings(batch_size=100, poll_timeout_s=0.1),
)


class FakeMessage:
    def __init__(
        self,
        payload: bytes | None,
        offset: int = 0,
        timestamp: tuple[int, int] = (TIMESTAMP_LOG_APPEND_TIME, 1_792_400_000_123),
        error: object = None,
    ) -> None:
        self._payload = payload
        self._offset = offset
        self._timestamp = timestamp
        self._error = error

    def value(self) -> bytes | None:
        return self._payload

    def error(self) -> object:
        return self._error

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return 0

    def topic(self) -> str:
        return "machine-temperature"


class Journal:
    """Records the order of the calls of the three collaborators."""

    def __init__(self) -> None:
        self.calls: list[str] = []


class FakeConsumer:
    def __init__(
        self, journal: Journal, batches: Sequence[Sequence[FakeMessage]]
    ) -> None:
        self._journal = journal
        self._batches = list(batches)
        self.subscribed: list[str] = []
        self.consume_arguments: list[tuple[int, float]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def consume(self, num_messages: int, timeout: float) -> list[FakeMessage]:
        self.consume_arguments.append((num_messages, timeout))
        return list(self._batches.pop(0)) if self._batches else []

    def commit(self, asynchronous: bool) -> None:
        assert asynchronous is False
        self._journal.calls.append("commit")

    def close(self) -> None:
        self.closed = True


class FakeAlertClient:
    def __init__(self, journal: Journal, undelivered: int = 0) -> None:
        self._journal = journal
        self._undelivered = undelivered
        self.sent: list[tuple[str, bytes, bytes]] = []

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[object, object], None],
    ) -> None:
        self.sent.append((topic, key, value))
        on_delivery(None, object())

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        self._journal.calls.append("flush")
        return self._undelivered


class FakeStorage:
    def __init__(
        self,
        journal: Journal,
        history: dict[str, list[tuple[datetime, float]]] | None = None,
        open_alerts: dict[str, OpenAlertRow] | None = None,
        fail: bool = False,
    ) -> None:
        self._journal = journal
        self._history = history or {}
        self._open_alerts = open_alerts or {}
        self._fail = fail
        self.readings: list[ReadingRow] = []
        self.alerts: list[AlertRow] = []
        self.history_requests: list[tuple[str, float]] = []

    def store_batch(
        self, readings: Sequence[ReadingRow], alerts: Sequence[AlertRow]
    ) -> BatchResult:
        self._journal.calls.append("store")
        if self._fail:
            raise StorageError("database down")
        self.readings.extend(readings)
        self.alerts.extend(alerts)
        return BatchResult(len(readings), 0, len(alerts), 0)

    def recent_readings(
        self, machine_id: str, span_s: float
    ) -> list[tuple[datetime, float]]:
        self.history_requests.append((machine_id, span_s))
        return self._history.get(machine_id, [])

    def open_alerts(self) -> dict[str, OpenAlertRow]:
        return self._open_alerts


def reading_message(
    second: int, temperature_c: float, machine: str = "mould-01", offset: int = 0
) -> FakeMessage:
    reading = Reading(machine, second, START + timedelta(seconds=second), temperature_c)
    return FakeMessage(reading.to_json().encode("utf-8"), offset)


@dataclasses.dataclass
class Rig:
    processor: StreamProcessor
    consumer: FakeConsumer
    storage: FakeStorage
    alerts: FakeAlertClient
    journal: Journal


def build(
    batches: Sequence[Sequence[FakeMessage]] = (),
    plant: PlantSettings = SINGLE,
    fail_storage: bool = False,
    undelivered_alerts: int = 0,
    history: dict[str, list[tuple[datetime, float]]] | None = None,
    open_alerts: dict[str, OpenAlertRow] | None = None,
) -> Rig:
    journal = Journal()
    consumer = FakeConsumer(journal, batches)
    storage = FakeStorage(journal, history, open_alerts, fail_storage)
    alerts = FakeAlertClient(journal, undelivered_alerts)
    processor = StreamProcessor(
        plant, consumer, alerts, storage, clock=lambda: PROCESSED_AT
    )
    return Rig(processor, consumer, storage, alerts, journal)


def test_the_readings_of_a_batch_are_stored_with_their_timestamps() -> None:
    rig = build()

    rig.processor.process_batch([reading_message(0, 70.0), reading_message(1, 70.5)])

    stored = [(r.machine_id, r.sequence, r.temperature_c) for r in rig.storage.readings]
    assert stored == [("mould-01", 0, 70.0), ("mould-01", 1, 70.5)]
    first = rig.storage.readings[0]
    assert first.measured_at == START
    assert first.processed_at == PROCESSED_AT
    assert first.kafka_appended_at == datetime.fromtimestamp(1_792_400_000.123, UTC)


def test_a_reading_that_reaches_a_threshold_stores_and_publishes_an_alert() -> None:
    rig = build()

    rig.processor.process_batch([reading_message(0, 70.0), reading_message(1, 81.0)])

    (row,) = rig.storage.alerts
    assert (row.kind, row.severity, row.status, row.limit) == (
        "threshold",
        "warning",
        "raised",
        80.0,
    )
    ((topic, key, value),) = rig.alerts.sent
    message = Alert.from_json(value)
    assert (topic, key) == ("machine-alerts", b"mould-01")
    assert message.alert_id == row.alert_id
    assert message.alert_id == "mould-01:threshold:2026-10-19T09:00:01.000000Z"
    assert message.processed_at == PROCESSED_AT
    assert message.temperature_c == 81.0


def test_alerts_are_published_before_the_database_commit_and_offsets_after_it() -> None:
    rig = build()

    rig.processor.process_batch([reading_message(0, 70.0), reading_message(1, 81.0)])

    assert rig.journal.calls == ["flush", "store", "commit"]


def test_a_batch_without_an_alert_is_stored_and_committed() -> None:
    rig = build()

    rig.processor.process_batch([reading_message(0, 70.0)])

    assert rig.alerts.sent == []
    assert rig.journal.calls == ["flush", "store", "commit"]


def test_the_offsets_are_not_committed_when_the_database_fails() -> None:
    rig = build(fail_storage=True)

    with pytest.raises(StorageError):
        rig.processor.process_batch([reading_message(0, 70.0)])

    assert "commit" not in rig.journal.calls


def test_nothing_is_stored_when_an_alert_cannot_be_published() -> None:
    rig = build(undelivered_alerts=1)

    with pytest.raises(ProcessorError, match="alerts"):
        rig.processor.process_batch([reading_message(0, 81.0)])

    assert rig.storage.readings == []
    assert "commit" not in rig.journal.calls


def test_a_message_that_is_not_valid_is_rejected_and_the_rest_is_processed() -> None:
    rig = build()

    stats = rig.processor.process_batch(
        [FakeMessage(b"not json", 0), reading_message(1, 70.0, offset=1)]
    )

    assert (stats.consumed, stats.rejected) == (2, 1)
    assert len(rig.storage.readings) == 1
    assert rig.journal.calls[-1] == "commit"


def test_a_message_without_a_value_is_rejected() -> None:
    rig = build()

    stats = rig.processor.process_batch([FakeMessage(None)])

    assert stats.rejected == 1
    assert rig.storage.readings == []


def test_a_reading_of_an_unknown_machine_is_rejected() -> None:
    rig = build()

    stats = rig.processor.process_batch([reading_message(0, 70.0, machine="unknown")])

    assert stats.rejected == 1
    assert rig.storage.readings == []


def test_a_record_without_a_broker_timestamp_is_rejected() -> None:
    rig = build()
    message = FakeMessage(
        reading_message(0, 70.0).value(), timestamp=(TIMESTAMP_NOT_AVAILABLE, -1)
    )

    stats = rig.processor.process_batch([message])

    assert stats.rejected == 1
    assert rig.storage.readings == []


def test_a_redelivered_reading_reaches_the_database_but_raises_no_second_alert() -> (
    None
):
    rig = build()

    rig.processor.process_batch([reading_message(0, 70.0), reading_message(1, 81.0)])
    rig.processor.process_batch([reading_message(0, 70.0), reading_message(1, 81.0)])

    assert len(rig.storage.readings) == 4
    assert len(rig.storage.alerts) == 1
    assert len(rig.alerts.sent) == 1


def test_the_stats_report_what_the_batch_stored() -> None:
    rig = build()

    stats = rig.processor.process_batch(
        [reading_message(0, 70.0), reading_message(1, 81.0)]
    )

    assert (
        stats.consumed,
        stats.rejected,
        stats.readings_inserted,
        stats.alerts_inserted,
    ) == (2, 0, 2, 1)


def test_start_restores_the_rules_from_stored_readings_and_the_open_alert() -> None:
    open_alert = OpenAlertRow(
        "mould-01:threshold:2026-10-19T08:59:00.000000Z",
        "mould-01",
        "threshold",
        "warning",
    )
    rig = build(
        history={
            "mould-01": [
                (START - timedelta(seconds=2), 85.0),
                (START - timedelta(seconds=1), 85.0),
            ]
        },
        open_alerts={"mould-01": open_alert},
    )

    rig.processor.restore()
    rig.processor.process_batch([reading_message(0, 70.0)])

    (row,) = rig.storage.alerts
    assert (row.alert_id, row.status, row.severity) == (
        open_alert.alert_id,
        "closed",
        "warning",
    )
    # Two windows of 60 seconds and one interval of 1 second.
    assert rig.storage.history_requests == [("mould-01", 121.0)]


def test_the_run_subscribes_consumes_in_batches_and_closes_the_consumer() -> None:
    rig = build([[reading_message(0, 70.0)], []])
    stops = iter([False, False, True])

    rig.processor.run(lambda: next(stops))

    assert rig.consumer.subscribed == ["machine-temperature"]
    assert rig.consumer.consume_arguments == [(100, 0.1), (100, 0.1)]
    assert len(rig.storage.readings) == 1
    assert rig.consumer.closed is True
    assert rig.journal.calls.count("commit") == 1


def test_the_consumer_is_closed_when_a_batch_fails() -> None:
    rig = build([[reading_message(0, 70.0)]], fail_storage=True)

    with pytest.raises(StorageError):
        rig.processor.run(lambda: False)

    assert rig.consumer.closed is True


def test_the_consumer_reads_from_the_start_and_commits_only_by_hand() -> None:
    config = consumer_config(default_plant())

    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["group.id"] == "bladewatch-processor"
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"
    assert config["broker.address.family"] == "v4"


def test_the_consumer_group_follows_the_settings() -> None:
    plant = dataclasses.replace(
        SINGLE,
        topics=TopicSettings(readings="r", alerts="a"),
        processor=ProcessorSettings(consumer_group="g"),
    )

    assert consumer_config(plant)["group.id"] == "g"
