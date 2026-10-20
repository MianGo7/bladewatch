import json

import pytest

from bladewatch.notifier import (
    Notification,
    NotificationError,
    format_notification,
    parse_notifications,
)


def _payload(**changes: object) -> bytes:
    document: dict[str, object] = {
        "receiver": "operator-webhook",
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "Critical machine alert",
                    "machine_id": "mould-01",
                    "severity": "critical",
                },
                "annotations": {"summary": "Machine mould-01 has a critical alert"},
                "startsAt": "2026-10-20T09:00:10Z",
            }
        ],
    }
    document.update(changes)
    return json.dumps(document).encode("utf-8")


def test_a_grafana_notification_is_parsed_into_one_entry_per_alert() -> None:
    notifications = parse_notifications(_payload())

    assert notifications == [
        Notification(
            status="firing",
            alertname="Critical machine alert",
            machine_id="mould-01",
            severity="critical",
            summary="Machine mould-01 has a critical alert",
            starts_at="2026-10-20T09:00:10Z",
        )
    ]


def test_a_notification_with_several_alerts_gives_several_entries() -> None:
    alert = {"status": "resolved", "labels": {"machine_id": "oven-02"}}

    notifications = parse_notifications(_payload(alerts=[alert, alert]))

    assert [n.machine_id for n in notifications] == ["oven-02", "oven-02"]
    assert [n.status for n in notifications] == ["resolved", "resolved"]


def test_an_alert_without_labels_and_annotations_gets_empty_fields() -> None:
    (notification,) = parse_notifications(_payload(alerts=[{"status": "firing"}]))

    assert (notification.machine_id, notification.severity, notification.summary) == (
        "",
        "",
        "",
    )


def test_a_notification_without_alerts_gives_no_entries() -> None:
    assert parse_notifications(_payload(alerts=[])) == []


def test_a_payload_that_is_not_json_is_rejected() -> None:
    with pytest.raises(NotificationError, match="JSON"):
        parse_notifications(b"not json")


def test_a_payload_that_is_not_an_object_is_rejected() -> None:
    with pytest.raises(NotificationError, match="object"):
        parse_notifications(b"[1, 2]")


def test_a_payload_without_a_list_of_alerts_is_rejected() -> None:
    with pytest.raises(NotificationError, match="alerts"):
        parse_notifications(_payload(alerts="none"))


def test_an_alert_that_is_not_an_object_is_rejected() -> None:
    with pytest.raises(NotificationError, match="alert"):
        parse_notifications(_payload(alerts=["x"]))


def test_a_payload_that_is_not_utf8_is_rejected() -> None:
    with pytest.raises(NotificationError, match="JSON"):
        parse_notifications(b"\xff\xfe")


def test_a_firing_notification_is_formatted_in_one_line() -> None:
    (notification,) = parse_notifications(_payload())

    line = format_notification(notification)

    assert "\n" not in line
    assert line == (
        "FIRING    critical  mould-01  Machine mould-01 has a critical alert "
        "(since 2026-10-20T09:00:10Z)"
    )


def test_a_resolved_notification_is_formatted_with_its_status() -> None:
    notification = Notification(
        "resolved", "x", "oven-02", "critical", "back to normal", ""
    )

    assert format_notification(notification).startswith("RESOLVED  critical  oven-02")
