"""Demonstration scenario for the steering committee and measurement M4.

One command runs the whole stack through an incident: normal operation, one mould
that begins to overheat, the warning, the critical alert, and the minute after it.
The run starts from an empty state, uses a seeded sensor model, and measures the time
from the reading that crosses the critical threshold to the alert in the database, on
the dashboard, and in the notification (ADR-0023). Implements FR11 and M4.
"""

import base64
import json
import logging
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from bladewatch.config import (
    GrafanaSettings,
    NotifierSettings,
    PlantSettings,
    ScenarioSettings,
)
from bladewatch.notifier import Notification, NotificationServer, log_notification
from bladewatch.processor import StreamProcessor, create_kafka_consumer
from bladewatch.producer import ReadingPublisher, create_kafka_producer, run
from bladewatch.sensor import Overheating
from bladewatch.storage import AlertEventRow, LatencySummary, Storage

logger = logging.getLogger(__name__)

_POLL_INTERVAL_S = 0.1
_READY_TIMEOUT_S = 60.0
_DRAIN_TIMEOUT_S = 90.0
# The notification can follow the alert in the database by up to the evaluation
# interval of the rule and the interval of the group (ADR-0022), so a run waits
# this long after the last reading for it and for the dashboard.
_GRACE_S = 40.0
# While the machine sleeps, the wall clock advances and the monotonic clock that
# paces the producer does not, which shifts every later reading against its time of
# measurement. A run in which the two clocks differ by this much is disturbed.
_SUSPENSION_TOLERANCE_S = 2.0
# A disturbed run is repeated, up to this many times beyond the planned repetitions.
_EXTRA_ATTEMPTS = 2


class ScenarioError(RuntimeError):
    """The scenario cannot start or did not complete."""


@dataclass(frozen=True, slots=True)
class Options:
    """What a run does beyond the processing of the readings.

    Attributes:
        pace: Publish in real time. Without it the readings are published as fast
            as possible with their simulated times, and no latency is meaningful.
        watch_dashboard: Poll Grafana to find when the critical alert is visible.
        show_notifications: Receive and log the notification of the contact point.
    """

    pace: bool = True
    watch_dashboard: bool = True
    show_notifications: bool = True


@dataclass(frozen=True, slots=True)
class EventSignature:
    """An alert event without the clock of the run, for comparing runs.

    Attributes:
        kind: `threshold` or `rate_of_rise`.
        severity: `warning` or `critical`.
        status: `raised` or `closed`.
        temperature_c: Temperature of the reading that caused the event.
        limit: Threshold or rate limit.
        offset_s: Seconds from the start of the run to that reading.
    """

    kind: str
    severity: str
    status: str
    temperature_c: float
    limit: float
    offset_s: float


@dataclass(frozen=True, slots=True)
class CriticalAlertTimes:
    """The time that each stage took for the critical alert, in seconds.

    The columns are the stages of the path from the reading that crossed the
    threshold to the visible alert.

    Attributes:
        measured_to_broker_s: From the measurement to the append at the broker.
        broker_to_processor_s: From the append to the evaluation by the processor.
        processor_to_stored_s: From the evaluation to the storage of the event.
        measured_to_stored_s: From the measurement to the alert in the database.
        measured_to_dashboard_s: From the measurement to the first answer of the
            dashboard query that contains the alert, or None if not watched.
        measured_to_notification_s: From the measurement to the notification, or
            None if it did not arrive.
        measured_to_alerting_s: From the measurement to the evaluation at which the
            alert rule of Grafana changed to firing, taken from its state history,
            or None if the history has no such change.
        alerting_to_notification_s: From that evaluation to the notification, or
            None if either is missing.
    """

    measured_to_broker_s: float
    broker_to_processor_s: float
    processor_to_stored_s: float
    measured_to_stored_s: float
    measured_to_dashboard_s: float | None
    measured_to_notification_s: float | None
    measured_to_alerting_s: float | None = None
    alerting_to_notification_s: float | None = None


@dataclass(frozen=True, slots=True)
class RunResult:
    """The outcome of one run of the scenario.

    Attributes:
        started_at: Time of measurement of the first reading.
        events: The alert events of the overheating machine, as stored.
        signature: The events without the clock of the run.
        critical: The stage times of the critical alert, or None if none was raised.
        readings: Number of readings stored for the machines of the plant.
        stages: The distribution of every stage over all stored readings.
        suspended_s: Seconds that the wall clock advanced and the monotonic clock
            did not during the run, which is the time the machine slept.
    """

    started_at: datetime
    events: list[AlertEventRow]
    signature: tuple[EventSignature, ...]
    critical: CriticalAlertTimes | None
    readings: int
    stages: LatencySummary
    suspended_s: float = 0.0

    @property
    def disturbed(self) -> bool:
        """Whether the machine was suspended during the run."""
        return self.suspended_s >= _SUSPENSION_TOLERANCE_S


def suspended_seconds(
    wall_start: datetime, wall_end: datetime, mono_start: float, mono_end: float
) -> float:
    """Returns how much longer the wall clock ran than the monotonic clock.

    Args:
        wall_start: The wall clock at the start.
        wall_end: The wall clock at the end.
        mono_start: The monotonic clock at the start, in seconds.
        mono_end: The monotonic clock at the end, in seconds.
    """
    return (wall_end - wall_start).total_seconds() - (mono_end - mono_start)


def percentile(values: Sequence[float], fraction: float) -> float:
    """Returns a percentile with linear interpolation between the ranks.

    Args:
        values: The sample, which must not be empty.
        fraction: The percentile as a fraction, for instance 0.95.

    Raises:
        ValueError: If the sample is empty or the fraction is outside 0 to 1.
    """
    if not values:
        raise ValueError("percentile of an empty sample")
    if not 0.0 <= fraction <= 1.0:
        raise ValueError(f"fraction must lie between 0 and 1, got {fraction}")
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def signature_of(
    events: Sequence[AlertEventRow], started_at: datetime
) -> tuple[EventSignature, ...]:
    """Reduces alert events to what a repeated run has to reproduce."""
    return tuple(
        EventSignature(
            kind=event.kind,
            severity=event.severity,
            status=event.status,
            temperature_c=event.temperature_c,
            limit=event.limit,
            offset_s=(event.measured_at - started_at).total_seconds(),
        )
        for event in events
    )


def alerting_time(
    history: Sequence[dict[str, object]], machine_id: str, after: datetime
) -> datetime | None:
    """Finds when the alert rule first evaluated a machine as firing after a time.

    Args:
        history: The entries of the state history of Grafana, in any order, each
            with its time in milliseconds, its new state, and its tags.
        machine_id: The machine whose alert is looked for.
        after: The time of measurement of the reading that crossed the threshold.
    """
    moments = []
    for entry in history:
        tags = entry.get("tags") or []
        if f"machine_id:{machine_id}" not in tags:
            continue
        if not str(entry.get("newState", "")).startswith("Alerting"):
            continue
        moment = datetime.fromtimestamp(float(entry["time"]) / 1000.0, UTC)
        if moment >= after:
            moments.append(moment)
    return min(moments) if moments else None


def critical_times(
    events: Sequence[AlertEventRow],
    dashboard_seen_at: datetime | None,
    notified_at: datetime | None,
    alerting_at: datetime | None = None,
) -> CriticalAlertTimes | None:
    """Computes the stage times of the first critical alert among the events."""
    critical = next(
        (e for e in events if e.severity == "critical" and e.status == "raised"), None
    )
    if critical is None or critical.kafka_appended_at is None:
        return None

    def since_measurement(moment: datetime | None) -> float | None:
        return (
            None if moment is None else (moment - critical.measured_at).total_seconds()
        )

    return CriticalAlertTimes(
        measured_to_broker_s=(
            critical.kafka_appended_at - critical.measured_at
        ).total_seconds(),
        broker_to_processor_s=(
            critical.processed_at - critical.kafka_appended_at
        ).total_seconds(),
        processor_to_stored_s=(
            critical.stored_at - critical.processed_at
        ).total_seconds(),
        measured_to_stored_s=(
            critical.stored_at - critical.measured_at
        ).total_seconds(),
        measured_to_dashboard_s=since_measurement(dashboard_seen_at),
        measured_to_notification_s=since_measurement(notified_at),
        measured_to_alerting_s=since_measurement(alerting_at),
        alerting_to_notification_s=(
            None
            if alerting_at is None or notified_at is None
            else (notified_at - alerting_at).total_seconds()
        ),
    )


def dashboard_query(machine_id: str, datasource_uid: str) -> dict[str, object]:
    """Builds the request that asks Grafana for the open critical alert of a machine.

    The query reads the view that the table of open alerts of the dashboard reads
    (ADR-0021). The identifier is restricted to safe characters by the settings.
    """
    return {
        "queries": [
            {
                "refId": "A",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": datasource_uid,
                },
                "editorMode": "code",
                "format": "table",
                "rawQuery": True,
                "rawSql": (
                    "SELECT machine_id FROM open_alerts "
                    f"WHERE severity = 'critical' AND machine_id = '{machine_id}'"
                ),
            }
        ],
        "from": "now-1h",
        "to": "now",
    }


def state_history(grafana: GrafanaSettings, since: datetime) -> list[dict[str, object]]:
    """Reads the state transitions of the alert rules of Grafana since a time.

    The history is read only; no setting of the alerting is changed. An error
    gives an empty history, so that a measurement does not fail because of it.

    Args:
        grafana: The connection to Grafana.
        since: The earliest time of a transition to return.
    """
    credentials = base64.b64encode(
        f"{grafana.user}:{grafana.password}".encode()
    ).decode()
    start_ms = int(since.timestamp() * 1000)
    request = urllib.request.Request(
        f"{grafana.base_url}/api/annotations?type=alert&limit=1000&from={start_ms}",
        headers={"Authorization": f"Basic {credentials}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10.0) as response:
            history = json.load(response)
    except (urllib.error.URLError, OSError, ValueError) as error:
        logger.warning("the state history of Grafana could not be read: %s", error)
        return []
    return history if isinstance(history, list) else []


def has_rows(response: object) -> bool:
    """Tells whether a response of the query interface of Grafana holds a row."""
    if not isinstance(response, dict):
        return False
    result = response.get("results", {}).get("A", {})
    for frame in result.get("frames", []):
        values = frame.get("data", {}).get("values", [])
        if values and values[0]:
            return True
    return False


class _DashboardWatcher:
    """Polls Grafana until the critical alert of a machine is visible to a query."""

    def __init__(self, grafana: GrafanaSettings, machine_id: str) -> None:
        credentials = base64.b64encode(
            f"{grafana.user}:{grafana.password}".encode()
        ).decode()
        body = json.dumps(dashboard_query(machine_id, grafana.datasource_uid))
        self._request = urllib.request.Request(
            f"{grafana.base_url}/api/ds/query",
            data=body.encode("utf-8"),
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json",
            },
        )
        self.seen_at: datetime | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=10.0)

    def _poll(self) -> None:
        while not self._stop.is_set() and self.seen_at is None:
            try:
                with urllib.request.urlopen(self._request, timeout=5.0) as response:
                    answer = json.load(response)
            except (urllib.error.URLError, OSError, ValueError) as error:
                logger.debug("dashboard query failed: %s", error)
            else:
                if has_rows(answer):
                    self.seen_at = datetime.now(UTC)
                    return
            self._stop.wait(_POLL_INTERVAL_S)


class _Receiver:
    """Receives the notification of the contact point and notes when it came."""

    def __init__(self, machine_id: str) -> None:
        self._machine_id = machine_id
        self.notified_at: datetime | None = None
        self._server: NotificationServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def listening(self) -> bool:
        return self._server is not None

    def start(self) -> None:
        try:
            self._server = NotificationServer(NotifierSettings(), self._on_notification)
        except OSError as error:
            logger.warning(
                "no notification receiver on port %d: %s",
                NotifierSettings().port,
                error,
            )
            return
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=10.0)

    def _on_notification(self, notification: Notification) -> None:
        log_notification(notification)
        if (
            self.notified_at is None
            and notification.machine_id == self._machine_id
            and notification.status == "firing"
            and notification.severity == "critical"
        ):
            self.notified_at = datetime.now(UTC)


def _machine_ids(plant: PlantSettings) -> list[str]:
    return [machine.machine_id for machine in plant.machines]


def _admin(plant: PlantSettings) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": plant.kafka.bootstrap_servers,
            "broker.address.family": "v4",
        }
    )


def _topic_messages(
    plant: PlantSettings, topic: str
) -> tuple[int, list[TopicPartition]]:
    """Counts the messages of a topic and returns the end of each partition."""
    probe = Consumer(
        {
            "bootstrap.servers": plant.kafka.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": f"bladewatch-scenario-probe-{uuid.uuid4().hex[:8]}",
        }
    )
    try:
        metadata = probe.list_topics(topic, timeout=10.0)
        partitions = metadata.topics[topic].partitions
        total = 0
        ends = []
        for partition in partitions:
            low, high = probe.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10.0, cached=False
            )
            total += high - low
            ends.append(TopicPartition(topic, partition, high))
    finally:
        probe.close()
    return total, ends


def describe_state(plant: PlantSettings, storage: Storage) -> list[str]:
    """Lists what the stack holds of the scenario, empty if nothing.

    Args:
        plant: The plant whose topics and machines the scenario uses.
        storage: A connected storage.
    """
    lines = []
    for topic in (plant.topics.readings, plant.topics.alerts):
        count, _ = _topic_messages(plant, topic)
        if count:
            lines.append(f"the topic {topic} holds {count} messages")
    counts = storage.count_rows(_machine_ids(plant))
    for table in ("readings", "alerts", "machines"):
        if counts[table]:
            lines.append(
                f"the table {table} holds {counts[table]} rows of the scenario machines"
            )
    return lines


def refusal_message(plant: PlantSettings, found: Sequence[str]) -> str:
    """Words the refusal to start on a stack that holds data, with its remedy."""
    return (
        "The stack already holds data of the scenario:\n  - "
        + "\n  - ".join(found)
        + "\nStarting would mix it with the new run. With --reset the command "
        f"deletes the messages of the topics {plant.topics.readings} and "
        f"{plant.topics.alerts}, the rows of the tables readings, alerts, and "
        "machines for the machines of the scenario, and the hourly aggregate of "
        "these machines, and then starts."
    )


def reset_state(plant: PlantSettings, storage: Storage) -> None:
    """Deletes the data of the scenario from the topics and the database.

    Args:
        plant: The plant whose topics and machines the scenario uses.
        storage: A connected storage.
    """
    admin = _admin(plant)
    ends: list[TopicPartition] = []
    for topic in (plant.topics.readings, plant.topics.alerts):
        _, topic_ends = _topic_messages(plant, topic)
        ends.extend(end for end in topic_ends if end.offset > 0)
    if ends:
        for future in admin.delete_records(ends).values():
            future.result(timeout=30.0)
    storage.clear_machines(_machine_ids(plant))


def _wait_until_group_is_stable(admin: AdminClient, group_id: str) -> None:
    deadline = time.monotonic() + _READY_TIMEOUT_S
    while time.monotonic() < deadline:
        description = admin.describe_consumer_groups([group_id])[group_id].result(
            timeout=10.0
        )
        if description.members and str(description.state).endswith("STABLE"):
            return
        time.sleep(0.25)
    raise ScenarioError("the processor did not join its consumer group in time")


def _processor_thread(
    processor: StreamProcessor, stop: threading.Event, errors: list[BaseException]
) -> threading.Thread:
    def target() -> None:
        try:
            processor.run(stop.is_set)
        except BaseException as error:  # reported to the caller after the run
            errors.append(error)

    return threading.Thread(target=target, daemon=True)


def run_once(
    plant: PlantSettings,
    scenario: ScenarioSettings,
    options: Options | None = None,
    started_at: datetime | None = None,
    grafana: GrafanaSettings | None = None,
) -> RunResult:
    """Runs the scenario once on an empty state.

    Args:
        plant: The plant, whose machine list has to contain the overheating machine.
        scenario: The timing of the incident and the seed.
        options: Pacing, dashboard watching, and the notification receiver.
        started_at: Time of measurement of the first reading, or None for now.
            A fixed time makes two runs identical down to the identifiers.
        grafana: The connection to Grafana for the dashboard watching.

    Raises:
        ScenarioError: If the run does not complete.
    """
    options = options or Options()
    grafana = grafana or GrafanaSettings()
    plant.machine(scenario.machine_id)
    machine_ids = _machine_ids(plant)
    group_id = f"bladewatch-scenario-{uuid.uuid4().hex[:8]}"
    group_plant = replace(
        plant, processor=replace(plant.processor, consumer_group=group_id)
    )
    admin = _admin(plant)
    errors: list[BaseException] = []
    stop = threading.Event()
    receiver = _Receiver(scenario.machine_id)
    watcher = _DashboardWatcher(grafana, scenario.machine_id)
    ticks = int(scenario.duration_s // plant.sensor.interval_s)
    with (
        Storage(plant.database) as storage,
        Storage(plant.database) as processor_storage,
    ):
        processor = StreamProcessor(
            group_plant,
            create_kafka_consumer(group_plant),
            create_kafka_producer(plant.kafka),
            processor_storage,
        )
        thread = _processor_thread(processor, stop, errors)
        thread.start()
        try:
            _wait_until_group_is_stable(admin, group_id)
            if options.show_notifications:
                receiver.start()
            if options.watch_dashboard:
                watcher.start()
            start = started_at or datetime.now(UTC)
            publisher = ReadingPublisher(
                create_kafka_producer(plant.kafka), plant.topics.readings
            )
            overheating = {
                scenario.machine_id: Overheating(
                    scenario.normal_period_s, scenario.rate_c_per_min
                )
            }
            logger.info("scenario started with the seed %d", scenario.seed)
            wall_start, mono_start = datetime.now(UTC), time.monotonic()
            run(
                plant,
                scenario.seed,
                publisher,
                start,
                scenario.duration_s,
                overheating,
                sleep=time.sleep if options.pace else (lambda _seconds: None),
            )
            _wait_until_stored(storage, machine_ids, ticks * len(machine_ids))
            if options.pace:
                _wait_for_visibility(options, watcher, receiver)
            suspended_s = suspended_seconds(
                wall_start, datetime.now(UTC), mono_start, time.monotonic()
            )
        finally:
            stop.set()
            thread.join(timeout=60.0)
            watcher.stop()
            receiver.stop()
            _delete_group(admin, group_id)
        if errors:
            raise ScenarioError(f"the processor failed: {errors[0]}") from errors[0]
        events = storage.alert_events(scenario.machine_id)
        stages = storage.stage_latencies(machine_ids)
    alerting_at = _alerting_at(grafana, events, scenario.machine_id, options)
    return RunResult(
        started_at=start,
        events=events,
        signature=signature_of(events, start),
        critical=critical_times(
            events, watcher.seen_at, receiver.notified_at, alerting_at
        ),
        readings=stages.readings,
        stages=stages,
        suspended_s=suspended_s,
    )


def _alerting_at(
    grafana: GrafanaSettings,
    events: Sequence[AlertEventRow],
    machine_id: str,
    options: Options,
) -> datetime | None:
    if not options.watch_dashboard:
        return None
    critical = next(
        (e for e in events if e.severity == "critical" and e.status == "raised"), None
    )
    if critical is None:
        return None
    history = state_history(grafana, critical.measured_at)
    return alerting_time(history, machine_id, critical.measured_at)


def _wait_until_stored(storage: Storage, machine_ids: list[str], expected: int) -> None:
    deadline = time.monotonic() + _DRAIN_TIMEOUT_S
    while time.monotonic() < deadline:
        if storage.count_rows(machine_ids)["readings"] >= expected:
            return
        time.sleep(0.25)
    raise ScenarioError("the processor did not store every reading in time")


def _wait_for_visibility(
    options: Options, watcher: _DashboardWatcher, receiver: _Receiver
) -> None:
    deadline = time.monotonic() + _GRACE_S
    while time.monotonic() < deadline:
        dashboard_done = not options.watch_dashboard or watcher.seen_at is not None
        notification_done = (
            not options.show_notifications
            or receiver.notified_at is not None
            or not receiver.listening
        )
        if dashboard_done and notification_done:
            return
        time.sleep(0.25)


def _delete_group(admin: AdminClient, group_id: str) -> None:
    try:
        for future in admin.delete_consumer_groups([group_id]).values():
            future.result(timeout=10.0)
    except Exception as error:  # a left over group is harmless and only logged
        logger.debug("consumer group %s was not deleted: %s", group_id, error)


# The M4 report


def _columns() -> list[tuple[str, Callable[[CriticalAlertTimes], float | None]]]:
    return [
        ("measurement to broker", lambda t: t.measured_to_broker_s),
        ("broker to processor", lambda t: t.broker_to_processor_s),
        ("processor to database", lambda t: t.processor_to_stored_s),
        ("measurement to alert in the database", lambda t: t.measured_to_stored_s),
        (
            "measurement to alert on the dashboard query",
            lambda t: t.measured_to_dashboard_s,
        ),
        ("measurement to notification", lambda t: t.measured_to_notification_s),
        (
            "measurement to firing state in Grafana",
            lambda t: t.measured_to_alerting_s,
        ),
        ("firing state to notification", lambda t: t.alerting_to_notification_s),
    ]


def format_report(
    results: Sequence[RunResult], scenario: ScenarioSettings, metadata: dict[str, str]
) -> str:
    """Formats the results of the runs as Markdown tables.

    Args:
        results: One result per repetition.
        scenario: The settings of the runs.
        metadata: Facts that belong to the measurement, written above the tables.
    """
    valid = [result for result in results if not result.disturbed]
    lines = ["# M4, the demonstration scenario", ""]
    for key, value in metadata.items():
        lines.append(f"- {key}: {value}")
    lines += [
        f"- scenario: machine {scenario.machine_id}, seed {scenario.seed}, "
        f"{scenario.normal_period_s:g} seconds of normal operation, then "
        f"{scenario.rate_c_per_min:g} degrees Celsius per minute, "
        f"{scenario.duration_s:g} seconds per run, {scenario.repetitions} repetitions",
        "",
        "## The critical alert in every run, in seconds",
        "",
    ]
    columns = _columns()
    lines.append("| Run | " + " | ".join(name for name, _ in columns) + " | Note |")
    lines.append("| --- | " + " | ".join("---" for _ in columns) + " | --- |")
    for number, result in enumerate(results, start=1):
        note = (
            f"disturbed, the machine was suspended for {result.suspended_s:.0f} "
            "seconds, not part of the statistics"
            if result.disturbed
            else ""
        )
        if result.critical is None:
            lines.append(f"| {number} | no critical alert |")
            continue
        cells = [_cell(getter(result.critical)) for _, getter in columns]
        lines.append(f"| {number} | " + " | ".join(cells) + f" | {note} |")
    lines += ["", "## Over the runs, in seconds", ""]
    lines.append("| Statistic | " + " | ".join(name for name, _ in columns) + " |")
    lines.append("| --- | " + " | ".join("---" for _ in columns) + " |")
    for label, fraction in (
        ("median", 0.5),
        ("95th percentile", 0.95),
        ("maximum", 1.0),
    ):
        cells = []
        for _, getter in columns:
            values = [
                v
                for r in valid
                if r.critical is not None and (v := getter(r.critical)) is not None
            ]
            cells.append(_cell(percentile(values, fraction)) if values else "n/a")
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines += [
        "",
        f"The statistics are taken over {len(valid)} runs, one critical alert each.",
    ]
    if len(valid) != len(results):
        lines.append(
            f"{len(results) - len(valid)} of {len(results)} runs were disturbed and "
            "are not part of the statistics. A run is disturbed when the wall clock "
            "ran at least two seconds longer than the monotonic clock that paces the "
            "producer, which happens when the machine sleeps."
        )
    lines += [
        "",
        "## Every reading of every undisturbed run, in seconds",
        "",
        "| Run | Stage | Median | 95th percentile | Maximum |",
        "| --- | --- | --- | --- | --- |",
    ]
    for number, result in enumerate(results, start=1):
        if result.disturbed:
            continue
        for name, stats in result.stages.stages.items():
            lines.append(
                f"| {number} | {name} | {stats.median_s:.3f} | {stats.p95_s:.3f} | "
                f"{stats.maximum_s:.3f} |"
            )
    identical = len({r.signature for r in results}) == 1
    lines += [
        "",
        "## Repeatability",
        "",
        "The alert events of the runs "
        + (
            "are identical in kind, severity, status, temperature, limit, and time "
            "from the start."
            if identical
            else "DIFFER between the runs."
        ),
        "",
        "| Kind | Severity | Status | Temperature in degrees Celsius | Limit "
        "| Seconds from the start |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for event in results[0].signature:
        lines.append(
            f"| {event.kind} | {event.severity} | {event.status} | "
            f"{event.temperature_c:.2f} | {event.limit:g} | {event.offset_s:g} |"
        )
    return "\n".join(lines) + "\n"


def _cell(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def _git_state() -> str:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return f"{commit}, with uncommitted changes" if dirty else commit


def run_scenario(
    plant: PlantSettings,
    scenario: ScenarioSettings,
    reset: bool,
    options: Options,
    output_dir: Path,
    grafana: GrafanaSettings | None = None,
) -> list[RunResult]:
    """Runs the scenario for its repetitions and writes the report.

    Args:
        plant: The plant of the scenario.
        scenario: The scenario settings, with the number of repetitions.
        reset: Whether the command may delete the data of earlier runs.
        options: Pacing, dashboard watching, and the notification receiver.
        output_dir: Directory that receives the JSON and Markdown results.
        grafana: The connection to Grafana.

    Raises:
        ScenarioError: If the stack holds data and the reset was not asked for, or
            if a run does not complete.
    """
    if scenario.repetitions > 1 and not reset:
        raise ScenarioError("more than one repetition needs --reset between the runs")
    results: list[RunResult] = []
    valid = 0
    attempts = 0
    while valid < scenario.repetitions:
        attempts += 1
        if attempts > scenario.repetitions + _EXTRA_ATTEMPTS:
            raise ScenarioError(
                f"{scenario.repetitions + _EXTRA_ATTEMPTS} runs were not enough to "
                f"get {scenario.repetitions} undisturbed ones"
            )
        with Storage(plant.database) as storage:
            found = describe_state(plant, storage)
            if found and not reset:
                raise ScenarioError(refusal_message(plant, found))
            if found:
                logger.info("resetting: %s", "; ".join(found))
                reset_state(plant, storage)
        logger.info(
            "run %d, %d of %d undisturbed", attempts, valid, scenario.repetitions
        )
        result = run_once(plant, scenario, options, grafana=grafana)
        results.append(result)
        if result.disturbed:
            logger.warning(
                "the machine was suspended for %.0f seconds during the run, which "
                "makes its latencies meaningless; the run is repeated",
                result.suspended_s,
            )
        else:
            valid += 1
    if options.pace:
        _write_results(results, scenario, output_dir)
    return results


def _write_results(
    results: Sequence[RunResult], scenario: ScenarioSettings, output_dir: Path
) -> None:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "date": datetime.now(UTC).strftime("%Y-%m-%d"),
        "commit": _git_state(),
    }
    markdown = output_dir / f"m4-{stamp}.md"
    markdown.write_text(format_report(results, scenario, metadata), encoding="utf-8")
    raw = output_dir / f"m4-{stamp}.json"
    raw.write_text(
        json.dumps(
            {
                "metadata": metadata,
                "scenario": asdict(scenario),
                "runs": [_raw(r) for r in results],
            },
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )
    logger.info("results written to %s and %s", markdown, raw)
    for line in markdown.read_text(encoding="utf-8").splitlines():
        logger.info("%s", line)


def _raw(result: RunResult) -> dict[str, object]:
    return {
        "started_at": result.started_at.isoformat(),
        "events": [asdict(event) for event in result.events],
        "critical": None if result.critical is None else asdict(result.critical),
        "readings": result.readings,
        "suspended_s": result.suspended_s,
        "disturbed": result.disturbed,
        "stages": {n: asdict(s) for n, s in result.stages.stages.items()},
    }
