from datetime import UTC, datetime
from pathlib import Path

import pytest

from bladewatch.config import DatabaseSettings, ScenarioSettings
from bladewatch.scenario import (
    Options,
    ScenarioError,
    describe_state,
    reset_state,
    run_once,
    run_scenario,
)
from bladewatch.storage import AlertRow, ReadingRow, Storage

from .pipeline import Environment
from .stack import sql

pytestmark = pytest.mark.integration

FAST = Options(pace=False, watch_dashboard=False, show_notifications=False)
EXPECTED_KINDS = [
    ("rate_of_rise", "warning", "raised"),
    ("threshold", "critical", "raised"),
]
FIXED_START = datetime(2026, 10, 20, 9, 0, 0, tzinfo=UTC)


def _scenario(environment: Environment, repetitions: int = 1) -> ScenarioSettings:
    return ScenarioSettings(machine_id=environment.mould_id, repetitions=repetitions)


def test_the_scenario_raises_the_rate_warning_and_then_the_critical_alert(
    environment: Environment,
) -> None:
    (result,) = run_scenario(
        environment.plant, _scenario(environment), True, FAST, Path("unused")
    )

    assert [(e.kind, e.severity, e.status) for e in result.events] == EXPECTED_KINDS
    # 460 seconds of one reading per second for the three machines of the test.
    assert result.readings == 460 * 3
    # Only the machine that overheats raises an alert.
    others = sql(
        "SELECT count(*) FROM alerts "
        f"WHERE machine_id LIKE '{environment.prefix}%' "
        f"AND machine_id <> '{environment.mould_id}'"
    )
    assert others == [["0"]]


def test_repeated_runs_raise_identical_alerts_apart_from_their_clock(
    environment: Environment,
) -> None:
    results = run_scenario(
        environment.plant, _scenario(environment, 2), True, FAST, Path("unused")
    )

    first, second = results
    assert first.signature == second.signature
    assert [(e.kind, e.severity, e.status) for e in first.events] == EXPECTED_KINDS


def test_the_same_seed_and_start_time_give_identical_events_down_to_the_identifier(
    environment: Environment,
) -> None:
    scenario = _scenario(environment)
    first = run_once(environment.plant, scenario, FAST, started_at=FIXED_START)
    with Storage(DatabaseSettings()) as storage:
        reset_state(environment.plant, storage)

    second = run_once(environment.plant, scenario, FAST, started_at=FIXED_START)

    assert [(e.alert_id, e.temperature_c, e.measured_at) for e in first.events] == [
        (e.alert_id, e.temperature_c, e.measured_at) for e in second.events
    ]


def test_the_scenario_refuses_to_start_on_data_without_the_reset_flag(
    environment: Environment,
) -> None:
    run_scenario(environment.plant, _scenario(environment), True, FAST, Path("unused"))

    with pytest.raises(ScenarioError) as refusal:
        run_scenario(
            environment.plant, _scenario(environment), False, FAST, Path("unused")
        )

    message = str(refusal.value)
    assert "--reset" in message
    assert "holds" in message
    assert environment.plant.topics.readings in message


def test_a_stack_without_data_needs_no_reset(environment: Environment) -> None:
    (result,) = run_scenario(
        environment.plant, _scenario(environment), False, FAST, Path("unused")
    )

    assert len(result.events) == 2


def test_more_than_one_repetition_needs_the_reset_flag(
    environment: Environment,
) -> None:
    with pytest.raises(ScenarioError, match="--reset"):
        run_scenario(
            environment.plant, _scenario(environment, 2), False, FAST, Path("unused")
        )


def test_the_state_check_lists_the_messages_and_the_rows_that_a_reset_would_delete(
    environment: Environment,
) -> None:
    run_scenario(environment.plant, _scenario(environment), True, FAST, Path("unused"))

    with Storage(DatabaseSettings()) as storage:
        found = describe_state(environment.plant, storage)

    assert any(f"topic {environment.plant.topics.readings}" in line for line in found)
    assert any("table readings" in line for line in found)
    assert any("table alerts" in line for line in found)


def test_the_reset_leaves_other_machines_and_the_topics_empty(
    environment: Environment,
) -> None:
    run_scenario(environment.plant, _scenario(environment), True, FAST, Path("unused"))
    other = f"{environment.prefix}-other"
    moment = datetime.now(UTC)
    with Storage(DatabaseSettings()) as storage:
        storage.store_batch(
            [ReadingRow(other, 0, moment, 50.0, moment, moment)],
            [
                AlertRow(
                    f"{other}:x",
                    other,
                    "threshold",
                    "critical",
                    "raised",
                    96.0,
                    95.0,
                    moment,
                    moment,
                )
            ],
        )

        reset_state(environment.plant, storage)

        assert describe_state(environment.plant, storage) == []
        assert storage.count_rows([other]) == {
            "readings": 1,
            "alerts": 1,
            "machines": 0,
        }
