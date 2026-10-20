import dataclasses
import math

import pytest

from bladewatch.config import (
    GRINDER,
    MOULD,
    OVEN,
    DatabaseSettings,
    GrafanaSettings,
    KafkaSettings,
    Machine,
    MachineType,
    NotifierSettings,
    PlantSettings,
    ProcessorSettings,
    RuleSettings,
    ScenarioSettings,
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


def test_the_default_rule_settings_are_those_of_assumptions_a14_and_a15() -> None:
    rules = default_plant().rules

    assert (
        rules.hysteresis_margin_c,
        rules.rate_window_s,
        rules.rate_limit_c_per_min,
    ) == (2.0, 60.0, 3.0)


@pytest.mark.parametrize(
    "name", ["hysteresis_margin_c", "rate_window_s", "rate_limit_c_per_min"]
)
@pytest.mark.parametrize("value", [0.0, -1.0, math.nan, math.inf])
def test_rule_settings_require_every_parameter_to_be_positive_and_finite(
    name: str, value: float
) -> None:
    with pytest.raises(ValueError, match=name):
        RuleSettings(**{name: value})


def test_the_default_database_settings_are_the_development_credentials() -> None:
    database = default_plant().database

    assert (database.host, database.port, database.dbname, database.user) == (
        "localhost",
        5432,
        "bladewatch",
        "bladewatch",
    )


@pytest.mark.parametrize("port", [0, -1, 65536])
def test_database_settings_reject_a_port_outside_the_valid_range(port: int) -> None:
    with pytest.raises(ValueError, match="port"):
        DatabaseSettings(port=port)


def test_database_settings_require_a_host() -> None:
    with pytest.raises(ValueError, match="host"):
        DatabaseSettings(host="")


def test_the_default_processor_settings_are_those_of_assumption_a17() -> None:
    processor = default_plant().processor

    assert (processor.batch_size, processor.poll_timeout_s) == (500, 0.2)


def test_processor_settings_require_a_consumer_group() -> None:
    with pytest.raises(ValueError, match="consumer_group"):
        ProcessorSettings(consumer_group="")


def test_processor_settings_require_a_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        ProcessorSettings(batch_size=0)


@pytest.mark.parametrize("timeout_s", [0.0, -1.0, math.nan])
def test_processor_settings_require_a_positive_poll_timeout(timeout_s: float) -> None:
    with pytest.raises(ValueError, match="poll_timeout_s"):
        ProcessorSettings(poll_timeout_s=timeout_s)


def test_the_notifier_listens_on_loopback_at_the_port_of_the_contact_point() -> None:
    settings = NotifierSettings()

    assert (settings.host, settings.port) == ("127.0.0.1", 8089)


@pytest.mark.parametrize("port", [-1, 65536])
def test_notifier_settings_reject_a_port_outside_the_valid_range(port: int) -> None:
    with pytest.raises(ValueError, match="port"):
        NotifierSettings(port=port)


def test_notifier_settings_require_a_host() -> None:
    with pytest.raises(ValueError, match="host"):
        NotifierSettings(host="")


def test_the_default_scenario_is_the_one_of_assumptions_a16_and_a20() -> None:
    scenario = ScenarioSettings()

    assert (
        scenario.machine_id,
        scenario.normal_period_s,
        scenario.rate_c_per_min,
        scenario.duration_s,
        scenario.repetitions,
    ) == ("mould-01", 100.0, 5.0, 460.0, 3)


def test_the_scenario_lasts_until_after_the_critical_threshold_is_reached() -> None:
    scenario = ScenarioSettings()

    # From 70 degrees Celsius at 5 degrees Celsius per minute, 25 degrees Celsius
    # take 300 seconds, so that the critical threshold of 95 is reached at 400.
    seconds_to_critical = (
        scenario.normal_period_s + 25.0 / scenario.rate_c_per_min * 60.0
    )
    assert scenario.duration_s > seconds_to_critical


@pytest.mark.parametrize("machine_id", ["", "mould 01", "a'b", "mould-01;", "mühle"])
def test_scenario_settings_reject_a_machine_identifier_that_could_enter_a_query(
    machine_id: str,
) -> None:
    with pytest.raises(ValueError, match="machine_id"):
        ScenarioSettings(machine_id=machine_id)


@pytest.mark.parametrize("name", ["normal_period_s", "rate_c_per_min", "duration_s"])
@pytest.mark.parametrize("value", [0.0, -1.0, math.nan, math.inf])
def test_scenario_settings_require_positive_finite_timing(
    name: str, value: float
) -> None:
    with pytest.raises(ValueError, match=name):
        ScenarioSettings(**{name: value})


def test_the_scenario_has_to_last_longer_than_its_normal_period() -> None:
    with pytest.raises(ValueError, match="duration_s"):
        ScenarioSettings(normal_period_s=100.0, duration_s=100.0)


def test_the_scenario_needs_at_least_one_repetition() -> None:
    with pytest.raises(ValueError, match="repetitions"):
        ScenarioSettings(repetitions=0)


def test_the_default_grafana_settings_are_the_development_credentials() -> None:
    grafana = GrafanaSettings()

    assert (grafana.base_url, grafana.user, grafana.datasource_uid) == (
        "http://localhost:3000",
        "admin",
        "bladewatch-timescaledb",
    )


def test_grafana_settings_require_an_address() -> None:
    with pytest.raises(ValueError, match="base_url"):
        GrafanaSettings(base_url="")
