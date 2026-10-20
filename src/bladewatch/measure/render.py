"""Renders the table and the figure of a measurement from its raw result.

Reads the JSON file that a measurement wrote and writes the Markdown table and the
figure into the directory of the case study, so that both are rebuilt without a new
run (ADR-0024). Implements the evaluation of M1 to M3.
"""

import json
from pathlib import Path

from bladewatch.measure import MeasureError
from bladewatch.measure.figures import (
    faults_figure,
    latency_figure,
    save,
    step_profile_figure,
    throughput_figure,
)
from bladewatch.measure.reports import faults_report, latency_report, throughput_report

_REPORTS = {"M1": latency_report, "M2": throughput_report, "M3": faults_report}
_FIGURES = {"M1": latency_figure, "M2": throughput_figure, "M3": faults_figure}
_NAMES = {"M1": "latency", "M2": "throughput", "M3": "faults"}


def render(raw_path: Path, figures_dir: Path) -> list[Path]:
    """Renders the table and the figure of a raw result into a directory.

    Args:
        raw_path: The JSON file of a measurement.
        figures_dir: Directory for the table and the figure, usually docs/figures.

    Returns:
        The written files.

    Raises:
        MeasureError: If the file is not the result of a measurement.
    """
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    measurement = raw.get("measurement")
    if measurement not in _REPORTS:
        raise MeasureError(f"{raw_path} is not the result of a measurement")
    figures_dir.mkdir(parents=True, exist_ok=True)
    name = f"{measurement.lower()}-{_NAMES[measurement]}"
    table = figures_dir / f"{name}.md"
    table.write_text(_REPORTS[measurement](raw), encoding="utf-8")
    written = [table]
    written += save(_FIGURES[measurement](raw), figures_dir, name)
    if measurement == "M2":
        profile = step_profile_figure(raw)
        if profile is not None:
            written += save(profile, figures_dir, "m2-step-profile")
    return written
