import dataclasses
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from itertools import islice

import pytest
from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, Message

from bladewatch.config import TopicSettings, default_plant
from bladewatch.messages import Reading
from bladewatch.producer import (
    ReadingPublisher,
    create_kafka_producer,
    generate_ticks,
)

from .stack import compose

pytestmark = pytest.mark.integration

KAFKA_TOPICS = "/opt/kafka/bin/kafka-topics.sh"
BROKER_IN_CONTAINER = "localhost:29092"
START = datetime(2026, 10, 19, 9, 0, 0, tzinfo=UTC)
TICKS = 3


@pytest.fixture(scope="module")
def topic() -> Iterator[str]:
    # A topic of its own with the configuration of the real one, so that the
    # test leaves the topics of the stack untouched.
    name = f"readings-check-{uuid.uuid4().hex[:8]}"
    compose(
        "exec",
        "-T",
        "kafka",
        KAFKA_TOPICS,
        "--bootstrap-server",
        BROKER_IN_CONTAINER,
        "--create",
        "--topic",
        name,
        "--partitions",
        "6",
        "--config",
        "message.timestamp.type=LogAppendTime",
    )
    try:
        yield name
    finally:
        compose(
            "exec",
            "-T",
            "kafka",
            KAFKA_TOPICS,
            "--bootstrap-server",
            BROKER_IN_CONTAINER,
            "--delete",
            "--topic",
            name,
            check=False,
        )


def _consume_all(topic: str, expected: int) -> list[Message]:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "broker.address.family": "v4",
            # A new group reads from the start, as a replay would (FR2).
            "group.id": f"check-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    messages: list[Message] = []
    try:
        consumer.subscribe([topic])
        for _ in range(60):
            message = consumer.poll(1.0)
            if message is None:
                continue
            assert message.error() is None
            messages.append(message)
            if len(messages) == expected:
                break
    finally:
        consumer.close()
    return messages


@pytest.fixture(scope="module")
def published(topic: str) -> list[Message]:
    plant = dataclasses.replace(
        default_plant(), topics=TopicSettings(readings=topic, alerts="unused-alerts")
    )
    publisher = ReadingPublisher(create_kafka_producer(plant.kafka), topic)
    for tick in islice(generate_ticks(plant, 20261019, START), TICKS):
        for reading in tick:
            publisher.publish(reading)
    publisher.flush()
    return _consume_all(topic, expected=TICKS * len(plant.machines))


def test_every_published_reading_is_found_in_the_topic(
    published: list[Message],
) -> None:
    assert len(published) == TICKS * 12


def test_every_machine_has_consecutive_sequence_numbers_in_the_topic(
    published: list[Message],
) -> None:
    readings = [Reading.from_json(message.value()) for message in published]

    sequences: dict[str, list[int]] = {}
    for reading in readings:
        sequences.setdefault(reading.machine_id, []).append(reading.sequence)
    assert len(sequences) == 12
    assert all(values == [0, 1, 2] for values in sequences.values())


def test_the_message_key_is_the_machine_identifier(published: list[Message]) -> None:
    for message in published:
        assert message.key() == Reading.from_json(message.value()).machine_id.encode()


def test_all_readings_of_a_machine_are_in_one_partition(
    published: list[Message],
) -> None:
    partitions: dict[bytes, set[int]] = {}
    for message in published:
        partitions.setdefault(message.key(), set()).add(message.partition())

    assert all(len(found) == 1 for found in partitions.values())


def test_the_broker_sets_the_time_of_arrival_of_every_reading(
    published: list[Message],
) -> None:
    kinds = {message.timestamp()[0] for message in published}

    assert kinds == {TIMESTAMP_LOG_APPEND_TIME}
