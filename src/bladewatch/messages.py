"""Format and serialisation of the messages on the Kafka topics.

A reading is a JSON document with an explicit schema version, so that a change
of the format adds a version and never edits version 1 (ADR-0011). The module
imports neither the Kafka client nor the database driver. Implements the format
of FR1 and the sequence number of NFR2.
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
        try:
            document = json.loads(payload)
        except json.JSONDecodeError as error:
            raise MessageError(f"payload is not valid JSON: {error}") from error
        if not isinstance(document, dict):
            raise MessageError("payload must be a JSON object")
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


def _format_time(moment: datetime) -> str:
    # The fixed number of digits keeps the documents of equal length and sorts
    # as text in the same order as in time.
    return (
        moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _parse_time(text: str) -> datetime:
    try:
        return datetime.fromisoformat(text)
    except ValueError as error:
        raise MessageError(f"measured_at is not an ISO 8601 time: {text}") from error


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
