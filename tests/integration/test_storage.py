import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from bladewatch.config import DatabaseSettings
from bladewatch.storage import (
    AlertRow,
    MachineRow,
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
        sql(f"DELETE FROM machines WHERE machine_id LIKE '{name}%'")


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


def _machine_row(machine: str) -> MachineRow:
    return MachineRow(machine, "mould", 70.0, 80.0, 95.0)


def test_row_counts_are_limited_to_the_named_machines(
    prefix: str, storage: Storage
) -> None:
    ours, other = f"{prefix}-mould", f"{prefix}-oven"
    storage.register_machines([_machine_row(ours), _machine_row(other)])
    storage.store_batch(
        [_reading(ours, n) for n in range(4)] + [_reading(other, 0)],
        [_alert(ours, f"{ours}:threshold:a")],
    )

    counts = storage.count_rows([ours])

    assert counts == {"readings": 4, "alerts": 1, "machines": 1}


def test_clearing_machines_removes_only_their_rows(
    prefix: str, storage: Storage
) -> None:
    ours, other = f"{prefix}-mould", f"{prefix}-oven"
    storage.register_machines([_machine_row(ours), _machine_row(other)])
    storage.store_batch(
        [_reading(ours, 0), _reading(other, 0)],
        [_alert(ours, f"{ours}:threshold:a"), _alert(other, f"{other}:threshold:a")],
    )

    storage.clear_machines([ours])

    assert storage.count_rows([ours]) == {"readings": 0, "alerts": 0, "machines": 0}
    assert storage.count_rows([other]) == {"readings": 1, "alerts": 1, "machines": 1}


def test_clearing_machines_removes_their_hourly_aggregate_as_well(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, n) for n in range(10)], [])
    sql("CALL refresh_continuous_aggregate('readings_hourly', NULL, NULL)")
    assert sql(f"SELECT count(*) FROM readings_hourly WHERE machine_id = '{machine}'")

    storage.clear_machines([machine])

    assert sql(
        f"SELECT count(*) FROM readings_hourly WHERE machine_id = '{machine}'"
    ) == [["0"]]


def test_alert_events_carry_the_stage_timestamps_of_their_reading(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    alert_id = f"{machine}:threshold:a"
    reading = _reading(machine, 5, 81.0)
    storage.store_batch([reading], [_alert(machine, alert_id, second=5)])

    (event,) = storage.alert_events(machine)

    assert event.alert_id == alert_id
    assert event.kafka_appended_at == reading.kafka_appended_at
    assert event.measured_at <= event.processed_at <= event.stored_at


def test_the_latency_summary_covers_every_stage_of_the_stored_readings(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, n) for n in range(20)], [])

    summary = storage.stage_latencies([machine])

    assert summary.readings == 20
    # The helper gives every reading 5 ms to the broker and a further 15 ms to
    # the processor, and the database adds the delay of the insert.
    assert summary.stages["measured to broker"].median_s == pytest.approx(
        0.005, abs=1e-6
    )
    assert summary.stages["broker to processor"].median_s == pytest.approx(
        0.015, abs=1e-6
    )
    assert summary.stages["measured to stored"].maximum_s >= 0.02


def test_the_latency_summary_of_machines_without_readings_is_empty(
    prefix: str, storage: Storage
) -> None:
    summary = storage.stage_latencies([f"{prefix}-none"])

    assert summary.readings == 0
    assert summary.stages == {}


def test_counting_by_prefix_covers_the_machines_that_start_with_it(
    prefix: str, storage: Storage
) -> None:
    storage.register_machines(
        [_machine_row(f"{prefix}-a"), _machine_row(f"{prefix}-b"), _machine_row("x")]
    )
    storage.store_batch(
        [_reading(f"{prefix}-a", n) for n in range(3)] + [_reading(f"{prefix}-b", 0)],
        [],
    )

    counts = storage.count_by_prefix(prefix)

    assert counts == {"readings": 4, "distinct_readings": 4, "machines": 2}
    storage.clear_machines(["x"])


def test_a_prefix_is_not_a_pattern(prefix: str, storage: Storage) -> None:
    storage.store_batch(
        [_reading(f"{prefix}-a_b", 0), _reading(f"{prefix}-axb", 0)], []
    )

    counts = storage.count_by_prefix(f"{prefix}-a_b")

    assert counts["readings"] == 1


def test_clearing_by_prefix_leaves_other_machines_alone(
    prefix: str, storage: Storage
) -> None:
    other = f"{prefix.replace('it-', 'ot-')}-oven"
    ours = f"{prefix}-mould"
    storage.register_machines([_machine_row(ours), _machine_row(other)])
    storage.store_batch(
        [_reading(ours, 0), _reading(other, 0)],
        [_alert(ours, f"{ours}:threshold:a")],
    )

    storage.clear_by_prefix(prefix)

    assert storage.count_rows([ours]) == {"readings": 0, "alerts": 0, "machines": 0}
    assert storage.count_rows([other]) == {"readings": 1, "alerts": 0, "machines": 1}
    storage.clear_machines([other])


def test_the_latency_by_prefix_equals_the_latency_by_machines(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, n) for n in range(20)], [])

    by_prefix = storage.stage_latencies_by_prefix(prefix)

    assert by_prefix.readings == 20
    assert by_prefix.stages["broker to processor"].median_s == pytest.approx(
        0.015, abs=1e-6
    )


def test_the_samples_hold_one_value_per_reading_and_stage(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, n) for n in range(7)], [])

    samples = storage.stage_samples([machine])

    assert set(samples) == {
        "measured to broker",
        "broker to processor",
        "processor to stored",
        "measured to stored",
    }
    assert all(len(values) == 7 for values in samples.values())
    assert samples["measured to broker"][0] == pytest.approx(0.005, abs=1e-6)


def test_the_profile_groups_the_readings_by_the_time_of_their_measurement(
    prefix: str, storage: Storage
) -> None:
    machine = f"{prefix}-mould"
    storage.store_batch([_reading(machine, n) for n in range(25)], [])

    profile = storage.stage_profile_by_prefix(prefix, 10.0)

    assert [(b.start_s, b.readings) for b in profile] == [
        (0.0, 10),
        (10.0, 10),
        (20.0, 5),
    ]
    assert profile[0].stages["broker to processor"].median_s == pytest.approx(
        0.015, abs=1e-6
    )


def test_the_profile_of_a_family_without_readings_is_empty(
    prefix: str, storage: Storage
) -> None:
    assert storage.stage_profile_by_prefix(f"{prefix}-none", 10.0) == []
