import socket
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from bladewatch.config import DatabaseSettings, NotifierSettings
from bladewatch.notifier import Notification, NotificationServer
from bladewatch.storage import AlertRow, Storage

from .stack import grafana, sql

pytestmark = pytest.mark.integration

# The contact point of Grafana names this port, so the receiver of the test has to
# use it as well.
PORT = NotifierSettings().port
# The rule is evaluated every 10 seconds, and a notification follows at once, so a
# minute leaves room for one missed evaluation.
WAIT_S = 60.0


def _port_is_free() -> bool:
    with socket.socket() as probe:
        return probe.connect_ex(("127.0.0.1", PORT)) != 0


class Receiver:
    """The notifier of the package with a list that the test can read."""

    def __init__(self) -> None:
        self.received: list[Notification] = []
        self._server = NotificationServer(
            NotifierSettings(port=PORT), self.received.append
        )
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> "Receiver":
        self._thread.start()
        return self

    def __exit__(self, *exception: object) -> None:
        self._server.shutdown()
        self._thread.join(timeout=10.0)

    def wait_for(self, machine_id: str, status: str) -> Notification:
        deadline = time.monotonic() + WAIT_S
        while time.monotonic() < deadline:
            for notification in list(self.received):
                if (notification.machine_id, notification.status) == (
                    machine_id,
                    status,
                ):
                    return notification
            time.sleep(0.5)
        raise AssertionError(
            f"no {status} notification for {machine_id}; received {self.received}"
        )


@pytest.fixture
def machine_id() -> Iterator[str]:
    name = f"it-{uuid.uuid4().hex[:8]}-mould-01"
    try:
        yield name
    finally:
        sql(f"DELETE FROM alerts WHERE machine_id = '{name}'")


def _event(machine_id: str, status: str) -> AlertRow:
    now = datetime.now(UTC)
    return AlertRow(
        alert_id=f"{machine_id}:threshold:test",
        machine_id=machine_id,
        kind="threshold",
        severity="critical",
        status=status,
        temperature_c=96.0,
        limit=95.0,
        measured_at=now,
        processed_at=now,
    )


def test_the_contact_point_the_rule_and_the_policy_are_provisioned() -> None:
    contact_points = grafana("/api/v1/provisioning/contact-points")
    rules = grafana("/api/v1/provisioning/alert-rules")
    policy = grafana("/api/v1/provisioning/policies")

    assert [(c["name"], c["type"]) for c in contact_points] == [
        ("operator-webhook", "webhook")
    ]
    assert [(r["title"], r["ruleGroup"]) for r in rules] == [
        ("Critical machine alert", "machine-alerts")
    ]
    assert policy["receiver"] == "operator-webhook"


@pytest.mark.skipif(
    not _port_is_free(), reason=f"port {PORT} of the notifier is in use"
)
def test_a_critical_alert_reaches_the_notifier_and_its_closing_resolves_it(
    machine_id: str,
) -> None:
    with Receiver() as receiver, Storage(DatabaseSettings()) as storage:
        storage.store_batch([], [_event(machine_id, "raised")])

        firing = receiver.wait_for(machine_id, "firing")

        assert (firing.severity, firing.alertname) == (
            "critical",
            "Critical machine alert",
        )
        assert firing.summary == f"Machine {machine_id} has a critical alert"

        storage.store_batch([], [_event(machine_id, "closed")])

        resolved = receiver.wait_for(machine_id, "resolved")

        assert resolved.severity == "critical"
