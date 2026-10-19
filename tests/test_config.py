import dataclasses
import math

import pytest

from bladewatch.config import (
    GRINDER,
    MOULD,
    OVEN,
    KafkaSettings,
    Machine,
    MachineType,
    PlantSettings,
    SensorSettings,
    TopicSettings,
    default_plant,
)


def test_machine_type_requires_ordered_thresholds() -> None:
    with pytest.raises(ValueError, match="normal_c < warning_c < critical_c"):
        MachineType(name="mould", normal_c=70.0, warning_c=95.0, critical_c=80.0)


def test_machine_type_rejects_equal_warning_and_critical_thresholds() -> None:
    with pytest.raises(ValueError, match="normal_c < warning_c < critical_c"):
        MachineType(name="mould", normal_c=70.0, warning_c=80.0, critical_c=80.0)


@pytest.mark.parametrize("value", [math.nan, math.inf])
def test_machine_type_rejects_a_threshold_that_is_not_finite(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        MachineType(name="mould", normal_c=70.0, warning_c=80.0, critical_c=value)


def test_machine_type_requires_a_name() -> None:
    with pytest.raises(ValueError, match="name"):
        MachineType(name="", normal_c=70.0, warning_c=80.0, critical_c=95.0)


def test_machine_requires_an_identifier() -> None:
    with pytest.raises(ValueError, match="machine_id"):
        Machine(machine_id="", machine_type=MOULD)


def test_the_thresholds_of_the_machine_types_are_those_of_assumption_a4() -> None:
    thresholds = {
        t.name: (t.normal_c, t.warning_c, t.critical_c) for t in (MOULD, OVEN, GRINDER)
    }

    assert thresholds == {
        "mould": (70.0, 80.0, 95.0),
        "oven": (100.0, 110.0, 120.0),
        "grinder": (45.0, 60.0, 75.0),
    }


def test_the_default_plant_has_four_machines_of_each_of_the_three_types() -> None:
    plant = default_plant()

    counts = {t: 0 for t in (MOULD, OVEN, GRINDER)}
    for machine in plant.machines:
        counts[machine.machine_type] += 1
    assert len(plant.machines) == 12
    assert list(counts.values()) == [4, 4, 4]


def test_the_default_plant_reads_every_sensor_once_per_second() -> None:
    assert default_plant().sensor.interval_s == 1.0


def test_the_default_plant_uses_the_topics_of_the_infrastructure() -> None:
    topics = default_plant().topics

    assert (topics.readings, topics.alerts) == ("machine-temperature", "machine-alerts")


def test_the_default_plant_gives_every_machine_a_distinct_identifier() -> None:
    identifiers = [machine.machine_id for machine in default_plant().machines]

    assert len(set(identifiers)) == 12
    assert identifiers[0] == "mould-01"


def test_plant_rejects_duplicate_machine_identifiers() -> None:
    machines = (Machine("mould-01", MOULD), Machine("mould-01", OVEN))

    with pytest.raises(ValueError, match="unique"):
        PlantSettings(machines=machines)


def test_plant_requires_at_least_one_machine() -> None:
    with pytest.raises(ValueError, match="at least one machine"):
        PlantSettings(machines=())


def test_plant_looks_up_a_machine_by_its_identifier() -> None:
    plant = default_plant()

    assert plant.machine("oven-02").machine_type is OVEN


def test_plant_reports_an_unknown_machine_identifier() -> None:
    with pytest.raises(KeyError, match="unknown"):
        default_plant().machine("unknown")


@pytest.mark.parametrize("interval_s", [0.0, -1.0, math.nan])
def test_sensor_settings_require_a_positive_interval(interval_s: float) -> None:
    with pytest.raises(ValueError, match="interval_s"):
        SensorSettings(interval_s=interval_s)


@pytest.mark.parametrize("noise_std_c", [-0.1, math.nan, math.inf])
def test_sensor_settings_require_a_finite_and_non_negative_noise(
    noise_std_c: float,
) -> None:
    with pytest.raises(ValueError, match="noise_std_c"):
        SensorSettings(noise_std_c=noise_std_c)


def test_topic_settings_require_distinct_non_empty_names() -> None:
    with pytest.raises(ValueError, match="distinct"):
        TopicSettings(readings="same", alerts="same")
    with pytest.raises(ValueError, match="non-empty"):
        TopicSettings(readings="", alerts="machine-alerts")


def test_kafka_settings_require_bootstrap_servers() -> None:
    with pytest.raises(ValueError, match="bootstrap_servers"):
        KafkaSettings(bootstrap_servers="")


def test_settings_are_immutable() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        default_plant().sensor.interval_s = 2.0
