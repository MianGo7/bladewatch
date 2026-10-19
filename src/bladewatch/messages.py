"""Format and serialisation of the messages on the Kafka topics.

A reading and an alert are JSON documents with an explicit schema version, so
that a change of the format adds a version and never edits version 1
(ADR-0011). The module imports neither the Kafka client nor the database
driver. Implements the format of FR1, the sequence number of NFR2, and the
alert of FR6.
"""

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Self

SCHEMA_VERSION = 1
_FIELDS = ("schema_version", "machine_id", "sequence", "measured_at", "temperature_c")


class MessageError(ValueError):
    """A message violates the format."""


class UnsupportedSchemaVersionError(MessageError):
    """A message carries a schema version that this code does not know."""


@dataclass(frozen=True, slots=True)
class Reading:
    """One temperature reading of one machine.

    Attributes:
        machine_id: Identifier of the machine, also the message key.
        sequence: Counter per machine, starting at 0 and increased by one per
            reading, so that a gap shows a loss and a repeat shows a duplicate
            independently of the clocks (ADR-0011).
        measured_at: Time of measurement in UTC, set by the sensor simulator.
        temperature_c: Temperature in degrees Celsius.
        schema_version: Version of the message format.
    """

    machine_id: str
    sequence: int
    measured_at: datetime
    temperature_c: float
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Validates the fields."""
        if self.schema_version != SCHEMA_VERSION:
            raise UnsupportedSchemaVersionError(
                f"schema_version {self.schema_version} is not supported"
            )
        if not self.machine_id:
            raise MessageError("machine_id must not be empty")
        if self.sequence < 0:
            raise MessageError(f"sequence must not be negative, got {self.sequence}")
        if self.measured_at.utcoffset() != timedelta(0):
            raise MessageError("measured_at must be a timezone aware time in UTC")
        if not math.isfinite(self.temperature_c):
            raise MessageError(
                f"temperature_c must be finite, got {self.temperature_c}"
            )

    def key(self) -> bytes:
        """Returns the message key, so that one machine stays in one partition."""
        return self.machine_id.encode("utf-8")

    def to_json(self) -> str:
        """Serialises the reading to the JSON document of the format."""
        document = {
            "schema_version": self.schema_version,
            "machine_id": self.machine_id,
            "sequence": self.sequence,
            "measured_at": _format_time(self.measured_at),
            "temperature_c": self.temperature_c,
        }
        return json.dumps(document, allow_nan=False, separators=(",", ":"))

    @classmethod
    def from_json(cls, payload: str | bytes) -> Self:
        """Parses and validates a message.

        Args:
            payload: The JSON document as text or UTF-8 bytes.

        Raises:
            UnsupportedSchemaVersionError: If the schema version is unknown.
            MessageError: If the payload is not JSON or violates the format.
        """
        document = _load_object(payload)
        for name in _FIELDS:
            if name not in document:
                raise MessageError(f"field {name} is missing")
        return cls(
            machine_id=_string(document, "machine_id"),
            sequence=_integer(document, "sequence"),
            measured_at=_parse_time(_string(document, "measured_at")),
            temperature_c=_number(document, "temperature_c"),
            schema_version=_integer(document, "schema_version"),
        )


_KINDS = frozenset({"threshold", "rate_of_rise"})
_SEVERITIES = frozenset({"warning", "critical"})
_STATUSES = frozenset({"raised", "closed"})
_ALERT_FIELDS = (
    "schema_version",
    "alert_id",
    "machine_id",
    "kind",
    "severity",
    "status",
    "temperature_c",
    "limit",
    "measured_at",
    "processed_at",
)


@dataclass(frozen=True, slots=True)
class Alert:
    """A raise, an escalation, or a closing of the alert of one machine.

    Attributes:
        alert_id: Identifier of the alert, the same for every event of one alert
            and for a replayed reading, so that a consumer can recognise a
            repeated message.
        machine_id: Machine that raised the alert, also the message key.
        kind: Condition that caused the event, `threshold` or `rate_of_rise`.
        severity: Highest severity that the alert has reached, `warning` or
            `critical`.
        status: `raised` or `closed`.
        temperature_c: Temperature of the reading that caused the event in
            degrees Celsius.
        limit: Threshold in degrees Celsius, or rate limit in degrees Celsius per
            minute for the kind `rate_of_rise`.
        measured_at: Time of measurement of that reading, in UTC.
        processed_at: Time at which the processor evaluated it, in UTC.
        schema_version: Version of the message format.
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
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Validates the fields."""
        if self.schema_version != SCHEMA_VERSION:
            raise UnsupportedSchemaVersionError(
                f"schema_version {self.schema_version} is not supported"
            )
        if not self.alert_id:
            raise MessageError("alert_id must not be empty")
        if not self.machine_id:
            raise MessageError("machine_id must not be empty")
        for name, value, allowed in (
            ("kind", self.kind, _KINDS),
            ("severity", self.severity, _SEVERITIES),
            ("status", self.status, _STATUSES),
        ):
            if value not in allowed:
                raise MessageError(f"{name} {value!r} is not one of {sorted(allowed)}")
        for name, moment in (
            ("measured_at", self.measured_at),
            ("processed_at", self.processed_at),
        ):
            if moment.utcoffset() != timedelta(0):
                raise MessageError(f"{name} must be a timezone aware time in UTC")
        for name, number in (
            ("temperature_c", self.temperature_c),
            ("limit", self.limit),
        ):
            if not math.isfinite(number):
                raise MessageError(f"{name} must be finite, got {number}")

    def key(self) -> bytes:
        """Returns the message key, so that one machine stays in one partition."""
        return self.machine_id.encode("utf-8")

    def to_json(self) -> str:
        """Serialises the alert to the JSON document of the format."""
        document = {
            "schema_version": self.schema_version,
            "alert_id": self.alert_id,
            "machine_id": self.machine_id,
            "kind": self.kind,
            "severity": self.severity,
            "status": self.status,
            "temperature_c": self.temperature_c,
            "limit": self.limit,
            "measured_at": _format_time(self.measured_at),
            "processed_at": _format_time(self.processed_at),
        }
        return json.dumps(document, allow_nan=False, separators=(",", ":"))

    @classmethod
    def from_json(cls, payload: str | bytes) -> Self:
        """Parses and validates a message.

        Args:
            payload: The JSON document as text or UTF-8 bytes.

        Raises:
            UnsupportedSchemaVersionError: If the schema version is unknown.
            MessageError: If the payload is not JSON or violates the format.
        """
        document = _load_object(payload)
        for name in _ALERT_FIELDS:
            if name not in document:
                raise MessageError(f"field {name} is missing")
        return cls(
            alert_id=_string(document, "alert_id"),
            machine_id=_string(document, "machine_id"),
            kind=_string(document, "kind"),
            severity=_string(document, "severity"),
            status=_string(document, "status"),
            temperature_c=_number(document, "temperature_c"),
            limit=_number(document, "limit"),
            measured_at=_parse_time(_string(document, "measured_at"), "measured_at"),
            processed_at=_parse_time(_string(document, "processed_at"), "processed_at"),
            schema_version=_integer(document, "schema_version"),
        )


def _load_object(payload: str | bytes) -> dict[str, object]:
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as error:
        raise MessageError(f"payload is not valid JSON: {error}") from error
    if not isinstance(document, dict):
        raise MessageError("payload must be a JSON object")
    return document


def _format_time(moment: datetime) -> str:
    # The fixed number of digits keeps the documents of equal length and sorts
    # as text in the same order as in time.
    return (
        moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _parse_time(text: str, name: str = "measured_at") -> datetime:
    try:
        return datetime.fromisoformat(text)
    except ValueError as error:
        raise MessageError(f"{name} is not an ISO 8601 time: {text}") from error


def _string(document: dict[str, object], name: str) -> str:
    value = document[name]
    if not isinstance(value, str):
        raise MessageError(f"field {name} must be a string")
    return value


def _integer(document: dict[str, object], name: str) -> int:
    value = document[name]
    # bool is a subclass of int and must not pass as a counter.
    if isinstance(value, bool) or not isinstance(value, int):
        raise MessageError(f"field {name} must be an integer")
    return value


def _number(document: dict[str, object], name: str) -> float:
    value = document[name]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise MessageError(f"field {name} must be a number")
    return float(value)
