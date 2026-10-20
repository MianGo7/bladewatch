"""Runs a measurement and writes its raw result.

The raw result of every measurement is one JSON file in `results/`, which holds the
environment, the settings, and the numbers. The tables and the figures of the case
study are rendered from that file by `bladewatch.measure.render` (ADR-0024).
"""

import json
import logging
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bladewatch.bench import faults, latency, throughput
from bladewatch.bench.cluster import ClusterError, admin_client, message_count
from bladewatch.bench.environment import collect
from bladewatch.bench.throughput import LOAD_PREFIX
from bladewatch.config import FaultSettings, PlantSettings, ThroughputSettings
from bladewatch.measure import MeasureError
from bladewatch.storage import Storage

logger = logging.getLogger(__name__)


def describe_load_state(plant: PlantSettings) -> list[str]:
    """Lists what the stack holds that a load measurement would delete."""
    admin = admin_client(plant.kafka)
    lines = []
    for topic in (plant.topics.readings, plant.topics.alerts):
        count = message_count(admin, topic)
        if count:
            lines.append(f"the topic {topic} holds {count} messages")
    with Storage(plant.database) as storage:
        counts = storage.count_by_prefix(LOAD_PREFIX)
    for table, key in (("readings", "readings"), ("machines", "machines")):
        if counts[key]:
            lines.append(
                f"the table {table} holds {counts[key]} rows of the load machines"
            )
    return lines


def _check_state(plant: PlantSettings, reset: bool) -> None:
    found = describe_load_state(plant)
    if found and not reset:
        raise MeasureError(
            "The stack already holds data that the measurement would delete:\n  - "
            + "\n  - ".join(found)
            + "\nWith --reset the command deletes the messages of the topics "
            f"{plant.topics.readings} and {plant.topics.alerts} and the rows of the "
            f"load machines whose identifier starts with {LOAD_PREFIX}, and then "
            "starts."
        )


def _write(
    output_dir: Path,
    measurement: str,
    profile: str,
    settings: Mapping[str, Any],
    result: object,
) -> Path:
    raw = {
        "measurement": measurement,
        "environment": asdict(collect()),
        "profile": profile,
        "settings": dict(settings),
        "result": result,
    }
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{measurement.lower()}-{stamp}.json"
    path.write_text(json.dumps(raw, indent=1, default=str) + "\n", encoding="utf-8")
    logger.info("raw result written to %s", path)
    return path


def measure_latency(
    plant: PlantSettings,
    seed: int,
    repetitions: int,
    duration_s: float,
    reset: bool,
    output_dir: Path,
) -> Path:
    """Runs M1 and writes its raw result."""
    scenario = latency.scenario_for_m1(seed, repetitions, duration_s)
    result = latency.run_latency(plant, scenario, reset)
    profile = (
        f"{len(plant.machines)} machines with one reading per second, which is "
        f"{len(plant.machines)} readings per second, the incident of the "
        f"demonstration scenario, {repetitions} runs of {duration_s:g} seconds, "
        f"seed {seed}"
    )
    return _write(output_dir, "M1", profile, asdict(scenario), latency.as_json(result))


def measure_throughput(
    plant: PlantSettings,
    settings: ThroughputSettings,
    reset: bool,
    output_dir: Path,
) -> Path:
    """Runs M2 and writes its raw result."""
    _check_state(plant, reset)
    workdir = output_dir / "logs" / datetime.now(UTC).strftime("m2-%Y%m%dT%H%M%SZ")
    try:
        result = throughput.run_throughput(plant, settings, workdir)
    except ClusterError as error:
        raise MeasureError(str(error)) from error
    steps = ", ".join(str(step) for step in settings.steps)
    profile = (
        f"steps of {steps} readings per second, each one stream per reading and per "
        f"second, {settings.step_duration_s:g} seconds per step, "
        f"{settings.repetitions} runs of the ramp, seed {settings.seed}"
    )
    return _write(
        output_dir, "M2", profile, asdict(settings), throughput.as_json(result)
    )


def measure_faults(
    plant: PlantSettings,
    settings: FaultSettings,
    reset: bool,
    output_dir: Path,
) -> Path:
    """Runs M3 and writes its raw result."""
    _check_state(plant, reset)
    workdir = output_dir / "logs" / datetime.now(UTC).strftime("m3-%Y%m%dT%H%M%SZ")
    try:
        runs = faults.run_faults(plant, settings, workdir)
    except ClusterError as error:
        raise MeasureError(str(error)) from error
    profile = (
        f"{settings.streams} readings per second, {settings.warm_s:g} seconds of load, "
        f"{settings.outage_s:g} seconds without the processor, {settings.recovery_s:g} "
        f"seconds of load after it started again, {settings.repetitions} runs of each "
        f"kind of stop, seed {settings.seed}"
    )
    return _write(output_dir, "M3", profile, asdict(settings), faults.as_json(runs))
