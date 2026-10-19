import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from bladewatch.config import DatabaseSettings
from bladewatch.storage import (
    AlertRow,
    ReadingRow,
    Storage,
    StorageError,
)

from .stack import sql

pytestmark = pytest.mark.integration

START = datetime.now(UTC) - timedelta(hours=1)


@pytest.fixture
def prefix() -> Iterator[str]:
    name = f"it-{uuid.uuid4().hex[:8]}"
    try:
        yield name
    finally:
        sql(f"DELETE FROM readings WHERE machine_id LIKE '{name}%'")
        sql(f"DELETE FROM alerts WHERE machine_id LIKE '{name}%'")


@pytest.fixture
def storage() -> Iterator[Storage]:
    with Storage(DatabaseSettings()) as connected:
        yield connected


def _reading(machine: str, sequence: int, temperature_c: float = 70.0) -> ReadingRow:
    moment = START + timedelta(seconds=sequence)
    return ReadingRow(
        machine_id=machine,
        sequence=sequence,
        measured_at=moment,
        temperature_c=temperature_c,
        kafka_appended_at=moment + timedelta(milliseconds=5),
        processed_at=moment + timedelta(milliseconds=20),
    )


def _alert(
    machine: str,
    alert_id: str,
    status: str = "raised",
    severity: str = "warning",
    second: int = 0,
) -> AlertRow:
    moment = START + timedelta(seconds=second)
    return AlertRow(
        alert_id=alert_id,
        machine_id=machine,
        kind="threshold",
        severity=severity,
        status=status,
        temperature_c=81.0,
        limit=80.0,
        measured_at=moment,
        processed_at=moment + timedelta(milliseconds=20),
    )


def test_a_stored_batch_is_counted_as_inserted(prefix: str, storage: Storage) -> None:
    machine = f"{prefix}-mould"

    result = storage.store_batch([_reading(machine, n) for n in range(5)], [])

    assert (result.readings_inserted, result.readings_ignored) == (5, 0)
    assert sql(f"SELECT count(*) FROM readings WHERE machine_id = '{machine}'") == [
        ["5"]
    ]


def test_storing_the_same_batch_again_ignores_every_reading(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    batch = [_reading(machine, n) for n in range(5)]
    storage.store_batch(batch, [])

    result = storage.store_batch(batch, [])

    assert (result.readings_inserted, result.readings_ignored) == (0, 5)
    assert sql(f"SELECT count(*) FROM readings WHERE machine_id = '{machine}'") == [
        ["5"]
    ]


def test_a_duplicate_inside_one_batch_is_stored_once(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"

    result = storage.store_batch([_reading(machine, 0), _reading(machine, 0)], [])

    assert (result.readings_inserted, result.readings_ignored) == (1, 1)


def test_the_four_timestamps_of_a_reading_are_stored_in_the_order_of_the_stages(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, 0)], [])

    ordered = sql(
        "SELECT measured_at <= kafka_appended_at AND kafka_appended_at <= processed_at "
        f"AND processed_at <= stored_at FROM readings WHERE machine_id = '{machine}'"
    )

    assert ordered == [["t"]]


def test_a_stored_alert_event_is_ignored_when_it_is_stored_again(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    event = _alert(machine, f"{machine}:threshold:a")
    storage.store_batch([], [event])

    result = storage.store_batch([], [event])

    assert (result.alerts_inserted, result.alerts_ignored) == (0, 1)
    assert sql(f"SELECT count(*) FROM alerts WHERE machine_id = '{machine}'") == [["1"]]


def test_an_alert_keeps_its_three_events_under_one_identifier(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    alert_id = f"{machine}:threshold:a"

    result = storage.store_batch(
        [],
        [
            _alert(machine, alert_id, "raised", "warning", 0),
            _alert(machine, alert_id, "raised", "critical", 10),
            _alert(machine, alert_id, "closed", "critical", 20),
        ],
    )

    assert result.alerts_inserted == 3


def test_a_batch_is_stored_completely_or_not_at_all(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    invalid = _alert(machine, f"{machine}:x", status="open")

    with pytest.raises(StorageError):
        storage.store_batch([_reading(machine, 0)], [invalid])

    assert sql(f"SELECT count(*) FROM readings WHERE machine_id = '{machine}'") == [
        ["0"]
    ]


def test_recent_readings_returns_the_latest_span_in_ascending_order(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, n, 60.0 + n) for n in range(100)], [])

    readings = storage.recent_readings(machine, span_s=10.0)

    assert [t for _, t in readings] == [60.0 + n for n in range(89, 100)]
    assert [m for m, _ in readings] == sorted(m for m, _ in readings)


def test_recent_readings_of_an_unknown_machine_is_empty(
    prefix: str, storage: Storage
) -> None:
    assert storage.recent_readings(f"{prefix}-none", span_s=60.0) == []


def test_an_alert_without_a_closing_event_is_open(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    alert_id = f"{machine}:threshold:a"
    storage.store_batch(
        [],
        [
            _alert(machine, alert_id, "raised", "warning", 0),
            _alert(machine, alert_id, "raised", "critical", 10),
        ],
    )

    open_alert = storage.open_alerts()[machine]

    assert (open_alert.alert_id, open_alert.kind, open_alert.severity) == (
        alert_id,
        "threshold",
        "critical",
    )


def test_a_closed_alert_is_not_open(prefix: str, storage: Storage) -> None:
    machine = f"{prefix}-mould"
    alert_id = f"{machine}:threshold:a"
    storage.store_batch(
        [],
        [
            _alert(machine, alert_id, "raised", "warning", 0),
            _alert(machine, alert_id, "closed", "warning", 20),
        ],
    )

    assert machine not in storage.open_alerts()


def test_an_unconnected_storage_reports_an_error() -> None:
    with pytest.raises(StorageError, match="not connected"):
        Storage(DatabaseSettings()).open_alerts()


def test_a_database_that_is_not_reachable_reports_an_error() -> None:
    unreachable = Storage(DatabaseSettings(port=1))

    with pytest.raises(StorageError, match="cannot connect"):
        unreachable.connect()
