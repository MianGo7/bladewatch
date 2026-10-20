"""Measurement M1: the latency of every stage under the load of the case.

Runs the incident of the demonstration scenario for ten minutes at 12 readings per
second, without the dashboard poller and the notification receiver that M4 adds, and
reports the median, the 95th percentile, the 99th percentile, and the maximum of
every stage over all readings of all runs (A21, ADR-0025). Implements M1 and NFR1.
"""

import logging
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime

from bladewatch.config import PlantSettings, ScenarioSettings
from bladewatch.measure.stats import Distribution, summarise
from bladewatch.scenario import (
    Options,
    ScenarioError,
    describe_state,
    refusal_message,
    reset_state,
    run_once,
)
from bladewatch.storage import AlertEventRow, Storage

logger = logging.getLogger(__name__)

# A run during which the machine sleeps is repeated, up to this many times.
_EXTRA_ATTEMPTS = 2
M1_DURATION_S = 600.0


@dataclass(frozen=True, slots=True)
class LatencyRun:
    """The latency of one run.

    Attributes:
        started_at: Time of measurement of the first reading.
        suspended_s: Seconds that the machine slept during the run.
        samples: The seconds of every stage for every reading, by stage name.
        alert_latencies_s: Seconds from the measurement to the storage of every
            raised alert event.
    """

    started_at: datetime
    suspended_s: float
    samples: dict[str, list[float]]
    alert_latencies_s: list[float]

    @property
    def disturbed(self) -> bool:
        """Returns whether the machine slept long enough to spoil the run."""
        return self.suspended_s >= 2.0


@dataclass(frozen=True, slots=True)
class LatencyResult:
    """The outcome of M1.

    Attributes:
        runs: The undisturbed runs.
        discarded: The runs that were disturbed and repeated.
        pooled: The distribution of every stage over all readings of the runs.
        alerts: The distribution of the time to the stored alert over all runs, or
            None if no alert was raised.
        per_run: The distribution of every stage in each run.
    """

    runs: list[LatencyRun]
    discarded: list[LatencyRun]
    pooled: dict[str, Distribution]
    alerts: Distribution | None
    per_run: list[dict[str, Distribution]]


def alert_latencies(events: Sequence[AlertEventRow]) -> list[float]:
    """Returns the seconds from measurement to storage of every raised event."""
    return [
        (event.stored_at - event.measured_at).total_seconds()
        for event in events
        if event.status == "raised"
    ]


def evaluate(
    runs: Sequence[LatencyRun], discarded: Sequence[LatencyRun] = ()
) -> LatencyResult:
    """Summarises the runs of M1.

    Args:
        runs: The undisturbed runs.
        discarded: The disturbed runs that were left out.

    Raises:
        ValueError: If there is no run.
    """
    if not runs:
        raise ValueError("evaluate: no run")
    names = list(runs[0].samples)
    pooled = {
        name: summarise([v for run in runs for v in run.samples[name]])
        for name in names
    }
    alerts = [v for run in runs for v in run.alert_latencies_s]
    return LatencyResult(
        runs=list(runs),
        discarded=list(discarded),
        pooled=pooled,
        alerts=summarise(alerts) if alerts else None,
        per_run=[{n: summarise(run.samples[n]) for n in names} for run in runs],
    )


def run_latency(
    plant: PlantSettings,
    scenario: ScenarioSettings,
    reset: bool,
) -> LatencyResult:
    """Runs M1 for the repetitions of the scenario settings.

    Args:
        plant: The plant of the case with its twelve machines.
        scenario: The incident, with the duration and the repetitions of M1.
        reset: Whether the data of earlier runs may be deleted.

    Raises:
        ScenarioError: If the stack holds data and the reset was not asked for, or
            if too many runs were disturbed.
    """
    options = Options(pace=True, watch_dashboard=False, show_notifications=False)
    machine_ids = [machine.machine_id for machine in plant.machines]
    runs: list[LatencyRun] = []
    discarded: list[LatencyRun] = []
    attempts = 0
    while len(runs) < scenario.repetitions:
        attempts += 1
        if attempts > scenario.repetitions + _EXTRA_ATTEMPTS:
            raise ScenarioError("too many runs were disturbed by a suspension")
        with Storage(plant.database) as storage:
            found = describe_state(plant, storage)
            if found and not reset:
                raise ScenarioError(refusal_message(plant, found))
            if found:
                logger.info("resetting: %s", "; ".join(found))
                reset_state(plant, storage)
        logger.info(
            "M1 run %d, %d of %d undisturbed", attempts, len(runs), scenario.repetitions
        )
        result = run_once(plant, scenario, options)
        with Storage(plant.database) as storage:
            samples = storage.stage_samples(machine_ids)
        run = LatencyRun(
            started_at=result.started_at,
            suspended_s=result.suspended_s,
            samples=samples,
            alert_latencies_s=alert_latencies(result.events),
        )
        if run.disturbed:
            logger.warning(
                "the run was suspended for %.0f s and is repeated", run.suspended_s
            )
            discarded.append(run)
            continue
        runs.append(run)
    return evaluate(runs, discarded)


def as_json(result: LatencyResult) -> dict[str, object]:
    """Returns the result as a structure of plain values for the JSON file."""
    return {
        "pooled": {name: asdict(d) for name, d in result.pooled.items()},
        "alerts": None if result.alerts is None else asdict(result.alerts),
        "per_run": [{n: asdict(d) for n, d in run.items()} for run in result.per_run],
        "runs": [
            {
                "started_at": run.started_at.isoformat(),
                "suspended_s": run.suspended_s,
                "alert_latencies_s": run.alert_latencies_s,
                "samples": run.samples,
            }
            for run in result.runs
        ],
        "discarded": [
            {"started_at": run.started_at.isoformat(), "suspended_s": run.suspended_s}
            for run in result.discarded
        ],
    }


def scenario_for_m1(
    seed: int, repetitions: int, duration_s: float = M1_DURATION_S
) -> ScenarioSettings:
    """Returns the scenario settings of M1: the incident, ten minutes, a seed."""
    return replace(
        ScenarioSettings(), seed=seed, repetitions=repetitions, duration_s=duration_s
    )
