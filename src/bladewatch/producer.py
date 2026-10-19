"""Producer of the simulated readings.

Generates the readings of every machine from the sensor model and publishes them
to the Kafka topic, keyed by machine. The generation is deterministic for a seed
and a start time, and the publication is separated from it, so that the
generator runs in a unit test without a broker. Implements FR1 and the keying
of FR2.
"""

import logging
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timedelta
from itertools import count
from typing import Protocol

from confluent_kafka import Producer

from bladewatch.config import KafkaSettings, PlantSettings
from bladewatch.messages import Reading
from bladewatch.sensor import Overheating, create_sensors, spawn_generators

logger = logging.getLogger(__name__)

_MAX_BUFFER_RETRIES = 10


class PublishError(RuntimeError):
    """A reading could not be delivered to the broker."""


class ProducerClient(Protocol):
    """The part of the Kafka producer that the publisher uses."""

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


def producer_config(settings: KafkaSettings) -> dict[str, str | bool]:
    """Returns the configuration of the Kafka producer.

    ADR-0012: `acks=all` and idempotence, so that a retry of the client does not
    leave a duplicate in the partition. Batching and linger are left at the
    defaults of the client, because tuning them is part of the measurements.

    Args:
        settings: The connection to the broker.
    """
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": "bladewatch-producer",
        "acks": "all",
        "enable.idempotence": True,
        # localhost resolves to the IPv6 address first, on which the stack does
        # not listen, and every connection would log a refused attempt.
        "broker.address.family": "v4",
    }


def create_kafka_producer(settings: KafkaSettings) -> Producer:
    """Creates the Kafka producer for the given broker."""
    return Producer(producer_config(settings))


def generate_ticks(
    plant: PlantSettings,
    seed: int,
    start: datetime,
    overheating: Mapping[str, Overheating] | None = None,
) -> Iterator[list[Reading]]:
    """Generates the readings of every machine, one list per measurement time.

    The sequence number of a reading is the index of the tick, because every
    machine delivers one reading per tick. The noise of each machine comes from
    its own generator, so that the readings of one machine do not change when
    another machine is added.

    Args:
        plant: The monitored plant.
        seed: The seed of the simulation. Equal seeds give equal readings.
        start: The time of measurement of the first tick, timezone aware in UTC.
        overheating: Overheatings to inject, by machine identifier.
    """
    sensors = create_sensors(
        plant, spawn_generators(seed, len(plant.machines)), overheating
    )
    interval_s = plant.sensor.interval_s
    for tick in count():
        elapsed_s = tick * interval_s
        measured_at = start + timedelta(seconds=elapsed_s)
        yield [
            Reading(
                machine_id=machine_id,
                sequence=tick,
                measured_at=measured_at,
                temperature_c=sensor.measure_c(elapsed_s),
            )
            for machine_id, sensor in sensors.items()
        ]


class ReadingPublisher:
    """Publishes readings to one topic and counts the outcome of the delivery."""

    def __init__(self, client: ProducerClient, topic: str) -> None:
        """Creates a publisher.

        Args:
            client: The Kafka producer or a replacement with the same methods.
            topic: The topic that receives the readings.
        """
        self._client = client
        self._topic = topic
        self.delivered = 0
        self.failed = 0

    def publish(self, reading: Reading) -> None:
        """Queues a reading, keyed by its machine identifier.

        Raises:
            PublishError: If the local queue of the client stays full.
        """
        for _ in range(_MAX_BUFFER_RETRIES):
            try:
                self._client.produce(
                    self._topic,
                    value=reading.to_json().encode("utf-8"),
                    key=reading.key(),
                    on_delivery=self._on_delivery,
                )
            except BufferError:
                # The local queue is full; serving the callbacks frees space.
                self._client.poll(1.0)
                continue
            self._client.poll(0.0)
            return
        raise PublishError("the local queue of the producer stayed full")

    def flush(self, timeout_s: float = 30.0) -> None:
        """Waits for all queued readings and checks that every one was delivered.

        Args:
            timeout_s: Longest wait in seconds.

        Raises:
            PublishError: If a reading was not delivered.
        """
        undelivered = self._client.flush(timeout_s)
        if undelivered or self.failed:
            raise PublishError(
                f"{undelivered} readings undelivered and {self.failed} failed"
            )

    def _on_delivery(self, error: object, message: object) -> None:
        if error is not None:
            logger.error("delivery failed: %s", error)
            self.failed += 1
            return
        self.delivered += 1


def run(
    plant: PlantSettings,
    seed: int,
    publisher: ReadingPublisher,
    start: datetime,
    duration_s: float | None = None,
    overheating: Mapping[str, Overheating] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    """Publishes the readings in real time at the interval of the sensors.

    Args:
        plant: The monitored plant.
        seed: The seed of the simulation.
        publisher: The publisher that receives the readings.
        start: The time of measurement of the first tick.
        duration_s: Length of the run in seconds, or None to run until it is
            interrupted. A run publishes the whole ticks that fit into it.
        overheating: Overheatings to inject, by machine identifier.
        sleep: Waits for the given number of seconds. Replaceable in tests.
        monotonic: Returns a monotonic time in seconds. Replaceable in tests.

    Returns:
        The number of ticks that were published.
    """
    interval_s = plant.sensor.interval_s
    ticks = None if duration_s is None else int(duration_s // interval_s)
    origin = monotonic()
    published = 0
    try:
        for tick, readings in enumerate(
            generate_ticks(plant, seed, start, overheating)
        ):
            if ticks is not None and tick >= ticks:
                break
            delay_s = origin + tick * interval_s - monotonic()
            if delay_s > 0.0:
                sleep(delay_s)
            elif delay_s < -interval_s:
                logger.warning("the producer is %.1f s behind schedule", -delay_s)
            for reading in readings:
                publisher.publish(reading)
            published += 1
    finally:
        publisher.flush()
    logger.info(
        "published %d ticks, %d readings delivered", published, publisher.delivered
    )
    return published
