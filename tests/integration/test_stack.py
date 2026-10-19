import base64
import json
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from .stack import compose, container_states, delete_consumer_group, sql

pytestmark = pytest.mark.integration

KAFKA_TOPICS = "/opt/kafka/bin/kafka-topics.sh"
KAFKA_CONSOLE_PRODUCER = "/opt/kafka/bin/kafka-console-producer.sh"
KAFKA_CONSOLE_CONSUMER = "/opt/kafka/bin/kafka-console-consumer.sh"
BROKER = "localhost:29092"
SEVEN_DAYS_IN_MILLISECONDS = "604800000"


def _grafana_get(path: str) -> dict[str, object]:
    credentials = base64.b64encode(b"admin:bladewatch").decode()
    request = urllib.request.Request(
        f"http://localhost:3000{path}",
        headers={"Authorization": f"Basic {credentials}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def test_every_long_running_container_is_healthy() -> None:
    states = container_states()

    unhealthy = {
        service: (row["State"], row.get("Health"))
        for service, row in states.items()
        if service != "kafka-init"
        and (row["State"], row.get("Health")) != ("running", "healthy")
    }
    assert set(states) == {"kafka", "kafka-init", "timescaledb", "grafana"}
    assert unhealthy == {}


def test_the_topic_creation_job_has_completed_successfully() -> None:
    row = container_states()["kafka-init"]

    assert (row["State"], row["ExitCode"]) == ("exited", 0)


def test_grafana_reports_a_working_database_connection() -> None:
    health = _grafana_get("/api/health")

    assert health["database"] == "ok"


def test_the_provisioned_data_source_reaches_timescaledb() -> None:
    result = _grafana_get("/api/datasources/uid/bladewatch-timescaledb/health")

    assert result["status"] == "OK"


@pytest.mark.parametrize(
    ("topic", "partitions"),
    [("machine-temperature", 6), ("machine-alerts", 3)],
)
def test_topic_exists_with_its_partitions_and_retention(
    topic: str, partitions: int
) -> None:
    description = compose(
        "exec",
        "-T",
        "kafka",
        KAFKA_TOPICS,
        "--bootstrap-server",
        BROKER,
        "--describe",
        "--topic",
        topic,
    ).stdout

    assert f"PartitionCount: {partitions}" in description
    assert f"retention.ms={SEVEN_DAYS_IN_MILLISECONDS}" in description
    assert "message.timestamp.type=LogAppendTime" in description


def test_a_new_consumer_group_replays_a_topic_from_its_start() -> None:
    topic = f"replay-check-{uuid.uuid4().hex[:8]}"
    compose(
        "exec",
        "-T",
        "kafka",
        KAFKA_TOPICS,
        "--bootstrap-server",
        BROKER,
        "--create",
        "--topic",
        topic,
    )
    try:
        compose(
            "exec",
            "-T",
            "kafka",
            KAFKA_CONSOLE_PRODUCER,
            "--bootstrap-server",
            BROKER,
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=:",
            stdin="mould-01:first\nmould-01:second\noven-01:third\n",
        )

        replay_group = f"replay-{uuid.uuid4().hex[:8]}"
        replayed = compose(
            "exec",
            "-T",
            "kafka",
            KAFKA_CONSOLE_CONSUMER,
            "--bootstrap-server",
            BROKER,
            "--topic",
            topic,
            "--group",
            replay_group,
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "20000",
        ).stdout.split()
        delete_consumer_group(replay_group)
    finally:
        compose(
            "exec",
            "-T",
            "kafka",
            KAFKA_TOPICS,
            "--bootstrap-server",
            BROKER,
            "--delete",
            "--topic",
            topic,
            check=False,
        )

    assert sorted(replayed) == ["first", "second", "third"]


def test_readings_is_a_hypertable_with_a_one_day_chunk_interval() -> None:
    rows = sql(
        "SELECT hypertable_name FROM timescaledb_information.hypertables",
        "SELECT time_interval FROM timescaledb_information.dimensions "
        "WHERE hypertable_name = 'readings'",
    )

    assert rows == [["readings"], ["1 day"]]


def test_the_alert_table_and_the_hourly_aggregate_exist() -> None:
    tables = sql("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    aggregates = sql(
        "SELECT view_name FROM timescaledb_information.continuous_aggregates"
    )

    assert ["alerts"] in tables
    assert aggregates == [["readings_hourly"]]


def test_refresh_compression_and_retention_policies_are_scheduled() -> None:
    jobs = sql(
        "SELECT proc_name, hypertable_name FROM timescaledb_information.jobs "
        "WHERE proc_name IN ('policy_refresh_continuous_aggregate', "
        "'policy_compression', 'policy_retention') ORDER BY proc_name"
    )

    assert jobs == [
        ["policy_compression", "readings"],
        ["policy_refresh_continuous_aggregate", "readings_hourly"],
        ["policy_retention", "readings"],
    ]


def test_a_stored_reading_is_ignored_when_it_is_inserted_twice() -> None:
    # One fixed timestamp for both statements, because now() differs between
    # two statements and would make them two different readings.
    measured_at = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    insert = (
        "INSERT INTO readings VALUES ('smoke-idempotence', 0, "
        f"'{measured_at}', 50.0, now(), now(), now()) "
        "ON CONFLICT (machine_id, measured_at) DO NOTHING"
    )
    try:
        sql(insert, insert)

        stored = sql(
            "SELECT count(*) FROM readings WHERE machine_id = 'smoke-idempotence'"
        )
    finally:
        sql("DELETE FROM readings WHERE machine_id = 'smoke-idempotence'")

    assert stored == [["1"]]


def test_the_hourly_aggregate_equals_the_aggregate_of_the_raw_readings() -> None:
    machine = "smoke-aggregate"
    bucket = "date_trunc('hour', now() - interval '2 days')"
    values = [60.0, 65.0, 70.0, 80.0]
    inserts = [
        f"INSERT INTO readings VALUES ('{machine}', {index}, "
        f"{bucket} + interval '{index} minutes', {value}, now(), now(), now())"
        for index, value in enumerate(values)
    ]
    refresh = (
        "CALL refresh_continuous_aggregate("
        f"'readings_hourly', {bucket}, {bucket} + interval '1 hour')"
    )
    try:
        sql(*inserts)
        sql(refresh)

        aggregate = sql(
            "SELECT min_temperature_c, mean_temperature_c, max_temperature_c, "
            "reading_count "
            f"FROM readings_hourly WHERE machine_id = '{machine}'"
        )
        raw = sql(
            "SELECT min(temperature_c), avg(temperature_c), max(temperature_c), "
            "count(*) "
            f"FROM readings WHERE machine_id = '{machine}'"
        )
    finally:
        sql(f"DELETE FROM readings WHERE machine_id = '{machine}'")
        sql(refresh)

    # Minimum, mean, and maximum of 60, 65, 70, and 80 degrees Celsius.
    assert aggregate == [["60", "68.75", "80", "4"]]
    assert raw == aggregate
