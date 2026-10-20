"""Stream processor of the prototype.

Consumes the readings in batches, applies the alert rules, publishes the alerts,
writes readings and alert events in one database transaction, and only then
commits the offsets, so that a stop at any point loses no reading and stores none
twice (ADR-0012, ADR-0020). Implements FR3 to FR5 in operation and FR7 and
NFR2.
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE, Consumer

from bladewatch.config import PlantSettings
from bladewatch.messages import Alert, MessageError, Reading
from bladewatch.rules import (
    AlertEvent,
    AlertKind,
    AlertTracker,
    OpenAlert,
    Severity,
    create_trackers,
)
from bladewatch.storage import (
    AlertRow,
    BatchResult,
    MachineRow,
    OpenAlertRow,
    ReadingRow,
)

logger = logging.getLogger(__name__)

_FLUSH_TIMEOUT_S = 30.0


class ProcessorError(RuntimeError):
    """The processor cannot continue without risking a loss of readings."""


class MessageLike(Protocol):
    """The part of a Kafka message that the processor reads."""

    def value(self) -> bytes | None:
        """Returns the payload."""

    def error(self) -> object:
        """Returns the error of the message, or None."""

    def timestamp(self) -> tuple[int, int]:
        """Returns the type of the timestamp and the time in milliseconds."""

    def offset(self) -> int:
        """Returns the offset in the partition."""

    def partition(self) -> int:
        """Returns the partition."""


class ConsumerClient(Protocol):
    """The part of the Kafka consumer that the processor uses."""

    def subscribe(self, topics: list[str]) -> None:
        """Joins the consumer group for the given topics."""

    def consume(self, num_messages: int, timeout: float) -> Sequence[MessageLike]:
        """Returns up to the given number of messages, waiting at most timeout."""

    def commit(self, asynchronous: bool) -> object:
        """Commits the offsets of the messages that were consumed."""

    def close(self) -> None:
        """Leaves the group."""


class AlertProducerClient(Protocol):
    """The part of the Kafka producer that publishes the alerts."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[object, object], None],
    ) -> None:
        """Queues one message for delivery."""

    def poll(self, timeout: float) -> int:
        """Serves the delivery callbacks."""

    def flush(self, timeout: float) -> int:
        """Waits for the queued messages and returns how many are undelivered."""


class StorageClient(Protocol):
    """The part of the storage that the processor uses."""

    def store_batch(
        self, readings: Sequence[ReadingRow], alerts: Sequence[AlertRow]
    ) -> BatchResult:
        """Stores readings and alert events in one transaction."""

    def register_machines(self, machines: Sequence[MachineRow]) -> None:
        """Writes the machines and their thresholds."""

    def recent_readings(
        self, machine_id: str, span_s: float
    ) -> list[tuple[datetime, float]]:
        """Returns the latest stored readings of a machine."""

    def open_alerts(self) -> dict[str, OpenAlertRow]:
        """Returns the open alert of every machine that has one."""


@dataclass(frozen=True, slots=True)
class BatchStats:
    """What the processing of one batch did.

    Attributes:
        consumed: Messages in the batch.
        rejected: Messages that were skipped because they were not valid.
        readings_inserted: Readings that were new in the database.
        readings_ignored: Readings that were already stored.
        alerts_inserted: Alert events that were new in the database.
    """

    consumed: int
    rejected: int
    readings_inserted: int
    readings_ignored: int
    alerts_inserted: int


def consumer_config(plant: PlantSettings) -> dict[str, str | bool]:
    """Returns the configuration of the Kafka consumer.

    ADR-0020: the offsets are committed by hand after the database transaction,
    and a group without an offset starts at the beginning of the retained log, so
    that a new group replays the stream.

    Args:
        plant: The settings of the plant.
    """
    return {
        "bootstrap.servers": plant.kafka.bootstrap_servers,
        "group.id": plant.processor.consumer_group,
        "client.id": "bladewatch-processor",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # localhost resolves to the IPv6 address first, on which the stack does
        # not listen.
        "broker.address.family": "v4",
    }


def create_kafka_consumer(plant: PlantSettings) -> Consumer:
    """Creates the Kafka consumer of the processor."""
    return Consumer(consumer_config(plant))


class StreamProcessor:
    """Applies the alert rules to the stream of readings."""

    def __init__(
        self,
        plant: PlantSettings,
        consumer: ConsumerClient,
        alert_producer: AlertProducerClient,
        storage: StorageClient,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Creates a processor.

        Args:
            plant: The monitored plant, with its thresholds and settings.
            consumer: The Kafka consumer, or a replacement with the same methods.
            alert_producer: The Kafka producer for the alert topic.
            storage: The database access.
            clock: Returns the current time in UTC. Replaceable in tests.
        """
        self._plant = plant
        self._consumer = consumer
        self._alert_producer = alert_producer
        self._storage = storage
        self._clock = clock
        self._trackers: dict[str, AlertTracker] = create_trackers(plant)
        self._alert_failures = 0

    def restore(self) -> None:
        """Registers the machines and rebuilds the rules from the database.

        The machines and their thresholds are written first, so that the
        dashboard shows them (ADR-0021). Then, for each machine, the stored
        readings of two window lengths and one interval fill the rate window, and
        the open alert, if any, is taken over.
        """
        self._storage.register_machines(
            [
                MachineRow(
                    machine.machine_id,
                    machine.machine_type.name,
                    machine.machine_type.normal_c,
                    machine.machine_type.warning_c,
                    machine.machine_type.critical_c,
                )
                for machine in self._plant.machines
            ]
        )
        window_s = self._plant.rules.rate_window_s
        span_s = 2.0 * window_s + self._plant.sensor.interval_s
        open_alerts = self._storage.open_alerts()
        for machine_id, tracker in self._trackers.items():
            history = self._storage.recent_readings(machine_id, span_s)
            stored = open_alerts.get(machine_id)
            open_alert = (
                None
                if stored is None
                else OpenAlert(
                    stored.alert_id, AlertKind(stored.kind), Severity(stored.severity)
                )
            )
            tracker.restore(history, open_alert)
        logger.info("restored the rules of %d machines", len(self._trackers))

    def run(self, should_stop: Callable[[], bool]) -> None:
        """Restores the state and processes batches until asked to stop.

        Args:
            should_stop: Checked between batches, so that a batch is always
                processed completely.
        """
        try:
            self.restore()
            self._consumer.subscribe([self._plant.topics.readings])
            settings = self._plant.processor
            while not should_stop():
                messages = self._consumer.consume(
                    settings.batch_size, settings.poll_timeout_s
                )
                if messages:
                    self.process_batch(messages)
        finally:
            self._consumer.close()

    def process_batch(self, messages: Sequence[MessageLike]) -> BatchStats:
        """Processes one batch.

        The order is fixed: evaluate the rules, publish the alerts, write the
        database transaction, and commit the offsets (ADR-0020). A failure of any
        step before the commit of the offsets leaves them uncommitted, so that the
        batch is delivered again.

        Args:
            messages: The messages of the batch in the order of consumption.

        Raises:
            ProcessorError: If a message carries an error that is not recoverable
                or an alert could not be delivered.
            StorageError: If the database did not accept the batch.
        """
        readings: list[ReadingRow] = []
        events: list[tuple[AlertEvent, datetime]] = []
        rejected = 0
        for message in messages:
            row = self._evaluate(message, events)
            if row is None:
                rejected += 1
                continue
            readings.append(row)
        alert_rows = [_alert_row(event, processed_at) for event, processed_at in events]
        self._publish(events)
        result = self._storage.store_batch(readings, alert_rows)
        self._consumer.commit(asynchronous=False)
        return BatchStats(
            consumed=len(messages),
            rejected=rejected,
            readings_inserted=result.readings_inserted,
            readings_ignored=result.readings_ignored,
            alerts_inserted=result.alerts_inserted,
        )

    def _evaluate(
        self, message: MessageLike, events: list[tuple[AlertEvent, datetime]]
    ) -> ReadingRow | None:
        error = message.error()
        if error is not None:
            raise ProcessorError(f"the consumer reported an error: {error}")
        payload = message.value()
        if payload is None:
            logger.warning(
                "skipped a message without a value at offset %d", message.offset()
            )
            return None
        try:
            reading = Reading.from_json(payload)
        except MessageError as problem:
            logger.warning(
                "skipped an invalid message at offset %d: %s", message.offset(), problem
            )
            return None
        tracker = self._trackers.get(reading.machine_id)
        if tracker is None:
            logger.warning(
                "skipped a reading of the unknown machine %s", reading.machine_id
            )
            return None
        timestamp_type, timestamp_ms = message.timestamp()
        if timestamp_type == TIMESTAMP_NOT_AVAILABLE:
            logger.warning(
                "skipped a record without a timestamp at offset %d", message.offset()
            )
            return None
        processed_at = self._clock()
        event = tracker.observe(reading.measured_at, reading.temperature_c)
        if event is not None:
            events.append((event, processed_at))
        return ReadingRow(
            machine_id=reading.machine_id,
            sequence=reading.sequence,
            measured_at=reading.measured_at,
            temperature_c=reading.temperature_c,
            kafka_appended_at=datetime.fromtimestamp(timestamp_ms / 1000.0, UTC),
            processed_at=processed_at,
        )

    def _publish(self, events: Sequence[tuple[AlertEvent, datetime]]) -> None:
        topic = self._plant.topics.alerts
        for event, processed_at in events:
            alert = _alert_message(event, processed_at)
            self._alert_producer.produce(
                topic,
                value=alert.to_json().encode("utf-8"),
                key=alert.key(),
                on_delivery=self._on_alert_delivery,
            )
            self._alert_producer.poll(0.0)
        undelivered = self._alert_producer.flush(_FLUSH_TIMEOUT_S)
        failed, self._alert_failures = self._alert_failures, 0
        if undelivered or failed:
            raise ProcessorError(
                f"alerts not delivered: {undelivered} undelivered, {failed} failed"
            )

    def _on_alert_delivery(self, error: object, message: object) -> None:
        if error is not None:
            logger.error("alert delivery failed: %s", error)
            self._alert_failures += 1


def _alert_message(event: AlertEvent, processed_at: datetime) -> Alert:
    return Alert(
        alert_id=event.alert_id,
        machine_id=event.machine_id,
        kind=str(event.kind),
        severity=str(event.severity),
        status=str(event.status),
        temperature_c=event.temperature_c,
        limit=event.limit,
        measured_at=event.measured_at,
        processed_at=processed_at,
    )


def _alert_row(event: AlertEvent, processed_at: datetime) -> AlertRow:
    return AlertRow(
        alert_id=event.alert_id,
        machine_id=event.machine_id,
        kind=str(event.kind),
        severity=str(event.severity),
        status=str(event.status),
        temperature_c=event.temperature_c,
        limit=event.limit,
        measured_at=event.measured_at,
        processed_at=processed_at,
    )
