import json
import math
from datetime import UTC, datetime, timedelta, timezone

import pytest

from bladewatch.messages import (
    SCHEMA_VERSION,
    MessageError,
    Reading,
    UnsupportedSchemaVersionError,
)

MEASURED_AT = datetime(2026, 10, 19, 8, 30, 15, 123456, tzinfo=UTC)


def _reading(**changes: object) -> Reading:
    fields: dict[str, object] = {
        "machine_id": "mould-01",
        "sequence": 7,
        "measured_at": MEASURED_AT,
        "temperature_c": 70.25,
    }
    fields.update(changes)
    return Reading(**fields)


def test_a_reading_carries_the_current_schema_version() -> None:
    assert _reading().schema_version == SCHEMA_VERSION == 1


def test_a_reading_is_serialised_with_the_fields_of_the_format() -> None:
    document = json.loads(_reading().to_json())

    assert document == {
        "schema_version": 1,
        "machine_id": "mould-01",
        "sequence": 7,
        "measured_at": "2026-10-19T08:30:15.123456Z",
        "temperature_c": 70.25,
    }


def test_a_reading_survives_a_round_trip_through_json() -> None:
    reading = _reading()

    assert Reading.from_json(reading.to_json()) == reading


def test_a_reading_can_be_parsed_from_bytes() -> None:
    reading = _reading()

    assert Reading.from_json(reading.to_json().encode("utf-8")) == reading


def test_the_message_key_is_the_machine_identifier_as_utf8() -> None:
    assert _reading(machine_id="oven-02").key() == b"oven-02"


def test_a_reading_with_microsecond_zero_keeps_its_fraction_in_the_format() -> None:
    reading = _reading(measured_at=datetime(2026, 10, 19, 8, 30, 15, tzinfo=UTC))

    assert json.loads(reading.to_json())["measured_at"] == "2026-10-19T08:30:15.000000Z"


def test_a_reading_rejects_a_time_without_a_time_zone() -> None:
    with pytest.raises(MessageError, match="UTC"):
        _reading(measured_at=datetime(2026, 10, 19, 8, 30, 15))


def test_a_reading_rejects_a_time_in_another_time_zone() -> None:
    local = datetime(2026, 10, 19, 10, 30, 15, tzinfo=timezone(timedelta(hours=2)))

    with pytest.raises(MessageError, match="UTC"):
        _reading(measured_at=local)


@pytest.mark.parametrize("temperature_c", [math.nan, math.inf, -math.inf])
def test_a_reading_rejects_a_temperature_that_is_not_finite(
    temperature_c: float,
) -> None:
    with pytest.raises(MessageError, match="temperature_c"):
        _reading(temperature_c=temperature_c)


def test_a_reading_rejects_a_negative_sequence() -> None:
    with pytest.raises(MessageError, match="sequence"):
        _reading(sequence=-1)


def test_a_reading_rejects_an_empty_machine_identifier() -> None:
    with pytest.raises(MessageError, match="machine_id"):
        _reading(machine_id="")


def test_a_message_with_an_unknown_schema_version_is_rejected() -> None:
    document = json.loads(_reading().to_json())
    document["schema_version"] = 2

    with pytest.raises(UnsupportedSchemaVersionError, match="2"):
        Reading.from_json(json.dumps(document))


@pytest.mark.parametrize(
    "field",
    ["schema_version", "machine_id", "sequence", "measured_at", "temperature_c"],
)
def test_a_message_with_a_missing_field_is_rejected(field: str) -> None:
    document = json.loads(_reading().to_json())
    del document[field]

    with pytest.raises(MessageError, match=field):
        Reading.from_json(json.dumps(document))


def test_a_message_that_is_not_json_is_rejected() -> None:
    with pytest.raises(MessageError, match="JSON"):
        Reading.from_json("not json")


def test_a_message_that_is_not_an_object_is_rejected() -> None:
    with pytest.raises(MessageError, match="object"):
        Reading.from_json("[1, 2]")


def test_a_message_with_a_text_temperature_is_rejected() -> None:
    document = json.loads(_reading().to_json())
    document["temperature_c"] = "70.25"

    with pytest.raises(MessageError, match="temperature_c"):
        Reading.from_json(json.dumps(document))


def test_a_message_with_a_malformed_time_is_rejected() -> None:
    document = json.loads(_reading().to_json())
    document["measured_at"] = "yesterday"

    with pytest.raises(MessageError, match="measured_at"):
        Reading.from_json(json.dumps(document))


def test_a_message_with_a_boolean_sequence_is_rejected() -> None:
    document = json.loads(_reading().to_json())
    document["sequence"] = True

    with pytest.raises(MessageError, match="sequence"):
        Reading.from_json(json.dumps(document))


def test_a_message_with_an_integer_temperature_is_accepted_as_a_float() -> None:
    document = json.loads(_reading().to_json())
    document["temperature_c"] = 70

    assert Reading.from_json(json.dumps(document)).temperature_c == 70.0
