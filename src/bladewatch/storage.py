"""Database access of the prototype.

The only module that imports the database driver. It writes readings and alert
events idempotently in one transaction per batch (ADR-0012) and reads back what a
restarted processor needs to rebuild the state of its rules (ADR-0020).
Implements FR7 and NFR8.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Any, Self

import psycopg
from psycopg.conninfo import make_conninfo

from bladewatch.config import DatabaseSettings

logger = logging.getLogger(__name__)

# stored_at is the time at which the statement runs and not the start of the
# transaction, which now() would give, so that it shows the delay of a batch
# that waited for a slow insert (ADR-0019).
_INSERT_READINGS = """
INSERT INTO readings (
    machine_id, sequence, measured_at, temperature_c,
    kafka_appended_at, processed_at, stored_at
)
SELECT m, s, t, c, k, p, clock_timestamp()
FROM unnest(
    %s::text[], %s::bigint[], %s::timestamptz[], %s::float8[],
    %s::timestamptz[], %s::timestamptz[]
) AS u(m, s, t, c, k, p)
ON CONFLICT (machine_id, measured_at) DO NOTHING
"""

_INSERT_ALERT = """
INSERT INTO alerts (
    alert_id, machine_id, kind, severity, status, temperature_c, "limit",
    measured_at, processed_at, stored_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, clock_timestamp())
ON CONFLICT (alert_id, severity, status) DO NOTHING
"""

_RECENT_READINGS = """
SELECT measured_at, temperature_c
FROM readings
WHERE machine_id = %(machine_id)s
  AND measured_at >= (
      SELECT max(measured_at) FROM readings WHERE machine_id = %(machine_id)s
  ) - make_interval(secs => %(span_s)s)
ORDER BY measured_at
"""

# An alert is open when it has no closing event. Its latest event carries the
# kind and the severity that the tracker of the rules has to take over.
_OPEN_ALERTS = """
SELECT DISTINCT ON (a.alert_id) a.alert_id, a.machine_id, a.kind, a.severity
FROM alerts AS a
WHERE NOT EXISTS (
    SELECT 1 FROM alerts AS c WHERE c.alert_id = a.alert_id AND c.status = 'closed'
)
ORDER BY a.alert_id, a.measured_at DESC
"""


_UPSERT_MACHINE = """
INSERT INTO machines (machine_id, machine_type, normal_c, warning_c, critical_c)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (machine_id) DO UPDATE SET
    machine_type = EXCLUDED.machine_type,
    normal_c = EXCLUDED.normal_c,
    warning_c = EXCLUDED.warning_c,
    critical_c = EXCLUDED.critical_c
"""


_COUNT_ROWS = """
SELECT
    (SELECT count(*) FROM readings WHERE machine_id = ANY(%(ids)s)),
    (SELECT count(*) FROM alerts WHERE machine_id = ANY(%(ids)s)),
    (SELECT count(*) FROM machines WHERE machine_id = ANY(%(ids)s))
"""

_ALERT_EVENTS = """
SELECT a.alert_id, a.kind, a.severity, a.status, a.temperature_c, a."limit",
       a.measured_at, r.kafka_appended_at, a.processed_at, a.stored_at
FROM alerts AS a
LEFT JOIN readings AS r
    ON r.machine_id = a.machine_id AND r.measured_at = a.measured_at
WHERE a.machine_id = %s
ORDER BY a.measured_at, a.stored_at
"""

# One expression per stage, each the time between two stage timestamps.
_STAGES = {
    "measured to broker": "kafka_appended_at - measured_at",
    "broker to processor": "processed_at - kafka_appended_at",
    "processor to stored": "stored_at - processed_at",
    "measured to stored": "stored_at - measured_at",
}


_PREFIX = "machine_id LIKE %(prefix)s"


def _stage_columns() -> str:
    columns = []
    for expression in _STAGES.values():
        seconds = f"extract(epoch FROM {expression})"
        columns.append(
            f"percentile_cont(0.5) WITHIN GROUP (ORDER BY {seconds}), "
            f"percentile_cont(0.95) WITHIN GROUP (ORDER BY {seconds}), "
            f"max({seconds})"
        )
    return ", ".join(columns)


def _stage_query(where: str = "machine_id = ANY(%(ids)s)") -> str:
    return f"SELECT count(*), {_stage_columns()} FROM readings WHERE {where}"


# The readings of a family are grouped by the time of their measurement, counted
# from the first one, so that a stage that slows down at some moment of a run shows
# at that moment (ADR-0026).
_PROFILE_QUERY = f"""
SELECT floor(extract(epoch FROM measured_at - f.first) / %(bucket_s)s) * %(bucket_s)s,
       count(*), {_stage_columns()}
FROM readings
CROSS JOIN (
    SELECT min(measured_at) AS first FROM readings WHERE {_PREFIX}
) AS f
WHERE {_PREFIX}
GROUP BY 1
ORDER BY 1
"""


def _like(prefix: str) -> str:
    escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "%"


class StorageError(RuntimeError):
    """The database did not accept a batch or a query."""


@dataclass(frozen=True, slots=True)
class ReadingRow:
    """A reading as it is stored, without the time of storage.

    Attributes:
        machine_id: Identifier of the machine.
        sequence: Counter per machine.
        measured_at: Time of measurement, set by the sensor.
        temperature_c: Temperature in degrees Celsius.
        kafka_appended_at: Time at which the broker appended the record.
        processed_at: Time at which the processor evaluated the reading.
    """

    machine_id: str
    sequence: int
    measured_at: datetime
    temperature_c: float
    kafka_appended_at: datetime
    processed_at: datetime


@dataclass(frozen=True, slots=True)
class AlertRow:
    """An alert event as it is stored, without the time of storage.

    Attributes:
        alert_id: Identifier of the alert.
        machine_id: Identifier of the machine.
        kind: `threshold` or `rate_of_rise`.
        severity: `warning` or `critical`.
        status: `raised` or `closed`.
        temperature_c: Temperature of the reading that caused the event.
        limit: Threshold in degrees Celsius or rate limit in degrees Celsius per
            minute.
        measured_at: Time of measurement of that reading.
        processed_at: Time at which the processor evaluated it.
    """

    alert_id: str
    machine_id: str
    kind: str
    severity: str
    status: str
    temperature_c: float
    limit: float
    measured_at: datetime
    processed_at: datetime


@dataclass(frozen=True, slots=True)
class OpenAlertRow:
    """An alert without a closing event.

    Attributes:
        alert_id: Identifier of the alert.
        machine_id: Identifier of the machine.
        kind: Kind of the latest event of the alert.
        severity: Severity of the latest event of the alert.
    """

    alert_id: str
    machine_id: str
    kind: str
    severity: str


@dataclass(frozen=True, slots=True)
class MachineRow:
    """A machine with its thresholds, as the dashboard shows it.

    Attributes:
        machine_id: Identifier of the machine.
        machine_type: Name of the machine type.
        normal_c: Normal operating temperature in degrees Celsius.
        warning_c: Warning threshold in degrees Celsius.
        critical_c: Critical threshold in degrees Celsius.
    """

    machine_id: str
    machine_type: str
    normal_c: float
    warning_c: float
    critical_c: float


@dataclass(frozen=True, slots=True)
class AlertEventRow:
    """A stored alert event with the stage timestamps of its reading.

    Attributes:
        alert_id: Identifier of the alert.
        kind: `threshold` or `rate_of_rise`.
        severity: `warning` or `critical`.
        status: `raised` or `closed`.
        temperature_c: Temperature of the reading that caused the event.
        limit: Threshold or rate limit.
        measured_at: Time of measurement of that reading.
        kafka_appended_at: Time at which the broker appended the reading, or None
            if the reading is not stored.
        processed_at: Time at which the processor evaluated it.
        stored_at: Time at which the database stored the event.
    """

    alert_id: str
    kind: str
    severity: str
    status: str
    temperature_c: float
    limit: float
    measured_at: datetime
    kafka_appended_at: datetime | None
    processed_at: datetime
    stored_at: datetime


@dataclass(frozen=True, slots=True)
class StageStats:
    """The distribution of the time that one stage took, in seconds.

    Attributes:
        median_s: Median.
        p95_s: 95th percentile.
        maximum_s: Maximum.
    """

    median_s: float
    p95_s: float
    maximum_s: float


@dataclass(frozen=True, slots=True)
class LatencySummary:
    """The latency of every stage over the stored readings of some machines.

    Attributes:
        readings: Number of readings.
        stages: Statistics by the name of the stage, empty without readings.
    """

    readings: int
    stages: dict[str, StageStats]


def _stage_summary(row: tuple[Any, ...]) -> LatencySummary:
    readings = row[0]
    if readings == 0:
        return LatencySummary(0, {})
    stages = {}
    for index, name in enumerate(_STAGES):
        median, p95, maximum = row[1 + 3 * index : 4 + 3 * index]
        stages[name] = StageStats(float(median), float(p95), float(maximum))
    return LatencySummary(readings, stages)


@dataclass(frozen=True, slots=True)
class ProfileBucket:
    """The stage times of the readings that were measured in one stretch of a run.

    Attributes:
        start_s: Seconds from the first measurement to the start of the stretch.
        readings: Number of readings measured in the stretch.
        stages: Statistics by the name of the stage.
    """

    start_s: float
    readings: int
    stages: dict[str, StageStats]


@dataclass(frozen=True, slots=True)
class BatchResult:
    """The outcome of storing a batch.

    Attributes:
        readings_inserted: Readings that were new.
        readings_ignored: Readings that were already stored.
        alerts_inserted: Alert events that were new.
        alerts_ignored: Alert events that were already stored.
    """

    readings_inserted: int
    readings_ignored: int
    alerts_inserted: int
    alerts_ignored: int


class Storage:
    """A connection to TimescaleDB with the operations of the prototype."""

    def __init__(self, settings: DatabaseSettings) -> None:
        """Creates a storage; the connection opens with `connect`.

        Args:
            settings: The connection to the database.
        """
        self._conninfo = make_conninfo(
            host=settings.host,
            port=settings.port,
            dbname=settings.dbname,
            user=settings.user,
            password=settings.password,
        )
        self._connection: psycopg.Connection | None = None

    def connect(self) -> None:
        """Opens the connection.

        Raises:
            StorageError: If the database is not reachable.
        """
        try:
            self._connection = psycopg.connect(self._conninfo)
        except psycopg.Error as error:
            raise StorageError(f"cannot connect to the database: {error}") from error

    def close(self) -> None:
        """Closes the connection, if it is open."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> Self:
        """Opens the connection for use in a with statement."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Closes the connection."""
        self.close()

    def store_batch(
        self, readings: Sequence[ReadingRow], alerts: Sequence[AlertRow]
    ) -> BatchResult:
        """Stores readings and alert events in one transaction.

        A reading or an event that is already stored is ignored, which makes the
        call safe to repeat after a redelivery.

        Args:
            readings: The readings of the batch.
            alerts: The alert events that the readings caused.

        Raises:
            StorageError: If the transaction fails. Nothing of the batch is
                stored then.
        """
        connection = self._require_connection()
        try:
            with connection.transaction(), connection.cursor() as cursor:
                readings_inserted = 0
                if readings:
                    cursor.execute(
                        _INSERT_READINGS,
                        [
                            [r.machine_id for r in readings],
                            [r.sequence for r in readings],
                            [r.measured_at for r in readings],
                            [r.temperature_c for r in readings],
                            [r.kafka_appended_at for r in readings],
                            [r.processed_at for r in readings],
                        ],
                    )
                    readings_inserted = cursor.rowcount
                alerts_inserted = 0
                for alert in alerts:
                    cursor.execute(
                        _INSERT_ALERT,
                        (
                            alert.alert_id,
                            alert.machine_id,
                            alert.kind,
                            alert.severity,
                            alert.status,
                            alert.temperature_c,
                            alert.limit,
                            alert.measured_at,
                            alert.processed_at,
                        ),
                    )
                    alerts_inserted += cursor.rowcount
        except psycopg.Error as error:
            raise StorageError(f"the batch was not stored: {error}") from error
        return BatchResult(
            readings_inserted=readings_inserted,
            readings_ignored=len(readings) - readings_inserted,
            alerts_inserted=alerts_inserted,
            alerts_ignored=len(alerts) - alerts_inserted,
        )

    def register_machines(self, machines: Sequence[MachineRow]) -> None:
        """Writes the machines and their thresholds, replacing earlier values.

        The configuration is the source of the thresholds, so a change there
        reaches the dashboard at the next start of the processor (ADR-0021).

        Args:
            machines: The monitored machines.

        Raises:
            StorageError: If the table could not be written.
        """
        connection = self._require_connection()
        try:
            with connection.transaction(), connection.cursor() as cursor:
                for machine in machines:
                    cursor.execute(
                        _UPSERT_MACHINE,
                        (
                            machine.machine_id,
                            machine.machine_type,
                            machine.normal_c,
                            machine.warning_c,
                            machine.critical_c,
                        ),
                    )
        except psycopg.Error as error:
            raise StorageError(f"machines could not be written: {error}") from error

    def count_rows(self, machine_ids: Sequence[str]) -> dict[str, int]:
        """Counts the rows of readings, alerts, and machines of some machines.

        Args:
            machine_ids: The machines to count.
        """
        rows = self._fetch(_COUNT_ROWS, {"ids": list(machine_ids)})
        readings, alerts, machines = rows[0]
        return {"readings": readings, "alerts": alerts, "machines": machines}

    def clear_machines(self, machine_ids: Sequence[str]) -> None:
        """Deletes the readings, alerts, and machine rows of some machines.

        Only the named machines are touched. The hourly aggregate is refreshed
        afterwards, so that no bucket of deleted readings remains.

        Args:
            machine_ids: The machines to clear.

        Raises:
            StorageError: If a statement fails.
        """
        connection = self._require_connection()
        ids = list(machine_ids)
        try:
            with connection.transaction(), connection.cursor() as cursor:
                for table in ("readings", "alerts", "machines"):
                    cursor.execute(
                        f"DELETE FROM {table} WHERE machine_id = ANY(%s)", (ids,)
                    )
            # A refresh of a continuous aggregate cannot run in a transaction.
            connection.autocommit = True
            try:
                connection.execute(
                    "CALL refresh_continuous_aggregate('readings_hourly', NULL, NULL)"
                )
            finally:
                connection.autocommit = False
        except psycopg.Error as error:
            raise StorageError(f"machines could not be cleared: {error}") from error

    def alert_events(self, machine_id: str) -> list[AlertEventRow]:
        """Returns the alert events of a machine with their stage timestamps.

        Args:
            machine_id: Identifier of the machine.
        """
        rows = self._fetch(_ALERT_EVENTS, (machine_id,))
        return [AlertEventRow(*row) for row in rows]

    def stage_latencies(self, machine_ids: Sequence[str]) -> LatencySummary:
        """Summarises the time of every stage over the stored readings.

        Args:
            machine_ids: The machines whose readings are summarised.
        """
        (row,) = self._fetch(_stage_query(), {"ids": list(machine_ids)})
        return _stage_summary(row)

    def stage_latencies_by_prefix(self, prefix: str) -> LatencySummary:
        """Summarises the time of every stage over a family of machines.

        Args:
            prefix: The start of the machine identifiers, which is not a pattern.
        """
        (row,) = self._fetch(_stage_query(_PREFIX), {"prefix": _like(prefix)})
        return _stage_summary(row)

    def stage_profile_by_prefix(
        self, prefix: str, bucket_s: float
    ) -> list[ProfileBucket]:
        """Summarises the stage times of a family over stretches of its run.

        Args:
            prefix: The start of the machine identifiers, which is not a pattern.
            bucket_s: Length of a stretch in seconds, counted from the first
                measurement of the family.
        """
        rows = self._fetch(
            _PROFILE_QUERY, {"prefix": _like(prefix), "bucket_s": bucket_s}
        )
        buckets = []
        for row in rows:
            summary = _stage_summary((row[1], *row[2:]))
            buckets.append(ProfileBucket(float(row[0]), row[1], summary.stages))
        return buckets

    def stage_samples(self, machine_ids: Sequence[str]) -> dict[str, list[float]]:
        """Returns the time of every stage for every stored reading, in seconds.

        Args:
            machine_ids: The machines whose readings are returned.
        """
        columns = ", ".join(
            f"extract(epoch FROM {expression})" for expression in _STAGES.values()
        )
        rows = self._fetch(
            f"SELECT {columns} FROM readings WHERE machine_id = ANY(%(ids)s)",
            {"ids": list(machine_ids)},
        )
        return {
            name: [float(row[index]) for row in rows]
            for index, name in enumerate(_STAGES)
        }

    def count_by_prefix(self, prefix: str) -> dict[str, int]:
        """Counts the readings, the distinct readings, and the machines of a family.

        A distinct reading is a pair of machine and sequence number, so that the
        difference to the count of rows shows a reading that is stored twice.

        Args:
            prefix: The start of the machine identifiers, which is not a pattern.
        """
        rows = self._fetch(
            "SELECT count(*), count(DISTINCT (machine_id, sequence)) "
            f"FROM readings WHERE {_PREFIX}",
            {"prefix": _like(prefix)},
        )
        machines = self._fetch(
            f"SELECT count(*) FROM machines WHERE {_PREFIX}",
            {"prefix": _like(prefix)},
        )
        readings, distinct = rows[0]
        return {
            "readings": readings,
            "distinct_readings": distinct,
            "machines": machines[0][0],
        }

    def clear_by_prefix(self, prefix: str) -> None:
        """Deletes the readings, alerts, and machine rows of a family of machines.

        Only machines whose identifier starts with the prefix are touched. The
        hourly aggregate is refreshed afterwards.

        Args:
            prefix: The start of the machine identifiers, which is not a pattern.

        Raises:
            StorageError: If a statement fails.
        """
        connection = self._require_connection()
        try:
            with connection.transaction(), connection.cursor() as cursor:
                for table in ("readings", "alerts", "machines"):
                    cursor.execute(
                        f"DELETE FROM {table} WHERE {_PREFIX}",
                        {"prefix": _like(prefix)},
                    )
            connection.autocommit = True
            try:
                connection.execute(
                    "CALL refresh_continuous_aggregate('readings_hourly', NULL, NULL)"
                )
            finally:
                connection.autocommit = False
        except psycopg.Error as error:
            raise StorageError(f"machines could not be cleared: {error}") from error

    def _fetch(self, statement: str, parameters: object) -> list[tuple[Any, ...]]:
        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                rows = cursor.fetchall()
            connection.commit()
        except psycopg.Error as error:
            raise StorageError(f"the query failed: {error}") from error
        return rows

    def recent_readings(
        self, machine_id: str, span_s: float
    ) -> list[tuple[datetime, float]]:
        """Returns the latest stored readings of a machine in ascending order.

        Args:
            machine_id: Identifier of the machine.
            span_s: Length of the time span in seconds that ends at the latest
                stored reading of the machine.
        """
        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    _RECENT_READINGS, {"machine_id": machine_id, "span_s": span_s}
                )
                rows = cursor.fetchall()
            connection.commit()
        except psycopg.Error as error:
            raise StorageError(f"readings could not be read: {error}") from error
        return [(measured_at, temperature_c) for measured_at, temperature_c in rows]

    def open_alerts(self) -> dict[str, OpenAlertRow]:
        """Returns the open alert of every machine that has one, by machine."""
        connection = self._require_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(_OPEN_ALERTS)
                rows = cursor.fetchall()
            connection.commit()
        except psycopg.Error as error:
            raise StorageError(f"open alerts could not be read: {error}") from error
        latest: dict[str, OpenAlertRow] = {}
        for alert_id, machine_id, kind, severity in rows:
            latest[machine_id] = OpenAlertRow(alert_id, machine_id, kind, severity)
        return latest

    def _require_connection(self) -> psycopg.Connection:
        if self._connection is None:
            raise StorageError("the storage is not connected")
        return self._connection
