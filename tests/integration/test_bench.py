from pathlib import Path

import pytest

from bladewatch.bench.faults import run_fault
from bladewatch.bench.run import measure_throughput
from bladewatch.bench.throughput import LOAD_PREFIX, run_step
from bladewatch.config import FaultSettings, ThroughputSettings, default_plant
from bladewatch.measure import MeasureError
from bladewatch.storage import Storage

pytestmark = pytest.mark.integration

# These tests use the topics of the stack and empty them, as every measurement of
# the load does, and they clear the load machines that start with `load-`.
SHORT_STEP = ThroughputSettings(steps=(12,), step_duration_s=6.0, repetitions=1)
SHORT_FAULT = FaultSettings(
    streams=20, warm_s=3.0, outage_s=3.0, recovery_s=6.0, repetitions=1
)


def test_a_load_step_delivers_the_offered_readings_and_stores_every_one(
    tmp_path: Path,
) -> None:
    result = run_step(default_plant(), 12, 1, SHORT_STEP, tmp_path)

    assert result.delivered == 12 * 6
    assert (result.stored, result.distinct) == (72, 72)
    assert result.drain_s is not None
    assert result.outcome.passed


def test_a_load_step_leaves_no_load_machine_behind(tmp_path: Path) -> None:
    run_step(default_plant(), 12, 1, SHORT_STEP, tmp_path)

    with Storage(default_plant().database) as storage:
        assert storage.count_by_prefix(LOAD_PREFIX)["readings"] == 0


@pytest.mark.parametrize("mode", ["finished", "killed"])
def test_a_processor_that_is_stopped_and_started_again_loses_and_duplicates_nothing(
    tmp_path: Path, mode: str
) -> None:
    run = run_fault(default_plant(), mode, 1, SHORT_FAULT, tmp_path)

    assert run.delivered == 20 * 12
    assert (run.missing, run.duplicated) == (0, 0)
    assert run.backlog_at_restart > 0


def test_a_load_measurement_refuses_to_start_on_a_stack_that_holds_data(
    tmp_path: Path,
) -> None:
    run_step(default_plant(), 12, 1, SHORT_STEP, tmp_path)
    # The topics still hold the messages of the step, so that without the reset the
    # measurement does not start.
    with pytest.raises(MeasureError, match="already holds data"):
        measure_throughput(default_plant(), SHORT_STEP, False, tmp_path)
