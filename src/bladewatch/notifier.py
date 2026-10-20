"""Webhook receiver for the notifications of the Grafana contact point.

Grafana posts a document to the receiver when an alert rule fires and when it
resolves, and the receiver writes one line per alert to the log. It stands for the
notification outside the dashboard of A6, in place of an e-mail or a text message
(ADR-0022). The module imports only the standard library and the settings.
Implements FR6.
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bladewatch.config import NotifierSettings

logger = logging.getLogger(__name__)

NOTIFY_PATH = "/notify"
# A notification of a few alerts is a few kilobytes. The limit keeps a stray
# request from filling memory, because the receiver has no authentication.
MAX_BODY_BYTES = 1_048_576


class NotificationError(ValueError):
    """A request body is not a notification of Grafana."""


@dataclass(frozen=True, slots=True)
class Notification:
    """One alert of a notification of Grafana.

    Attributes:
        status: `firing` or `resolved`.
        alertname: Name of the alert rule.
        machine_id: Machine that the alert belongs to, from the labels.
        severity: Severity from the labels.
        summary: Text of the annotation `summary`.
        starts_at: Time at which the alert began, as Grafana reports it.
    """

    status: str
    alertname: str
    machine_id: str
    severity: str
    summary: str
    starts_at: str


def parse_notifications(body: bytes) -> list[Notification]:
    """Parses the body of a webhook request into its alerts.

    Args:
        body: The request body, a JSON document in the format of the webhook
            contact point of Grafana.

    Raises:
        NotificationError: If the body is not such a document.
    """
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NotificationError(f"the body is not valid JSON: {error}") from error
    if not isinstance(document, dict):
        raise NotificationError("the body must be a JSON object")
    alerts = document.get("alerts")
    if not isinstance(alerts, list):
        raise NotificationError("the field alerts must be a list")
    notifications = []
    for alert in alerts:
        if not isinstance(alert, dict):
            raise NotificationError("every alert must be a JSON object")
        labels = alert.get("labels")
        annotations = alert.get("annotations")
        labels = labels if isinstance(labels, dict) else {}
        annotations = annotations if isinstance(annotations, dict) else {}
        notifications.append(
            Notification(
                status=str(alert.get("status", "")),
                alertname=str(labels.get("alertname", "")),
                machine_id=str(labels.get("machine_id", "")),
                severity=str(labels.get("severity", "")),
                summary=str(annotations.get("summary", "")),
                starts_at=str(alert.get("startsAt", "")),
            )
        )
    return notifications


def format_notification(notification: Notification) -> str:
    """Formats an alert as one line for the log."""
    line = (
        f"{notification.status.upper():<8}  {notification.severity:<8}  "
        f"{notification.machine_id}  {notification.summary}"
    )
    if notification.starts_at:
        line += f" (since {notification.starts_at})"
    return line


def log_notification(notification: Notification) -> None:
    """Writes an alert to the log, which is the default action of the receiver."""
    logger.warning("NOTIFICATION %s", format_notification(notification))


class NotificationServer:
    """An HTTP server that receives the notifications of Grafana."""

    def __init__(
        self,
        settings: NotifierSettings,
        on_notification: Callable[[Notification], None] = log_notification,
    ) -> None:
        """Binds the server to the address of the settings.

        Args:
            settings: Address and port to listen on. Port 0 selects a free port.
            on_notification: Called once per alert of every accepted request.
        """
        handler = _make_handler(on_notification)
        self._server = ThreadingHTTPServer((settings.host, settings.port), handler)

    @property
    def port(self) -> int:
        """The port that the server listens on."""
        return int(self._server.server_address[1])

    def serve_forever(self) -> None:
        """Serves requests until `shutdown` is called."""
        self._server.serve_forever()

    def shutdown(self) -> None:
        """Stops `serve_forever` and releases the port."""
        self._server.shutdown()
        self._server.server_close()


def _make_handler(
    on_notification: Callable[[Notification], None],
) -> type[BaseHTTPRequestHandler]:
    def reply(handler: BaseHTTPRequestHandler, status: HTTPStatus, text: str) -> None:
        body = text.encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "text/plain; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def receive(handler: BaseHTTPRequestHandler) -> None:
        if handler.path != NOTIFY_PATH:
            reply(handler, HTTPStatus.NOT_FOUND, "unknown path")
            return
        try:
            length = int(handler.headers.get("Content-Length", ""))
        except ValueError:
            reply(handler, HTTPStatus.LENGTH_REQUIRED, "Content-Length is required")
            return
        if not 0 <= length <= MAX_BODY_BYTES:
            reply(handler, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "body too large")
            return
        try:
            notifications = parse_notifications(handler.rfile.read(length))
        except NotificationError as error:
            reply(handler, HTTPStatus.BAD_REQUEST, str(error))
            return
        for notification in notifications:
            on_notification(notification)
        reply(handler, HTTPStatus.OK, f"received {len(notifications)}")

    def log_message(
        handler: BaseHTTPRequestHandler, format: str, *args: object
    ) -> None:
        logger.debug("http: " + format, *args)

    # The server finds the method for a request by the name do_POST, which the
    # naming rules of the linter do not accept for a method. The class is
    # therefore built with the name as a string.
    return type(
        "NotificationHandler",
        (BaseHTTPRequestHandler,),
        {"do_POST": receive, "log_message": log_message},
    )
