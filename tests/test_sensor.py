import math
import statistics

import numpy as np
import pytest

from bladewatch.config import MOULD, OVEN, Machine, SensorSettings, default_plant
from bladewatch.sensor import (
    Overheating,
    TemperatureSensor,
    create_sensors,
    spawn_generators,
)

MACHINE = Machine("mould-01", MOULD)
SEED = 20261019


def _sensor(
    noise_std_c: float = 0.5,
    overheating: Overheating | None = None,
    seed: int = SEED,
) -> TemperatureSensor:
    return TemperatureSensor(
        MACHINE,
        SensorSettings(noise_std_c=noise_std_c),
        spawn_generators(seed, 1)[0],
        overheating,
    )


def test_the_mean_of_normal_operation_is_the_normal_temperature_of_the_type() -> None:
    sensor = _sensor(noise_std_c=0.5)

    samples = [sensor.measure_c(float(second)) for second in range(100_000)]

    # The standard error of the mean is 0.5 / sqrt(100000) = 0.0016 degrees
    # Celsius. The tolerance of 0.008 is five standard errors, so that a correct
    # model fails for a different seed with a probability below one in a million.
    assert statistics.fmean(samples) == pytest.approx(MOULD.normal_c, abs=0.008)


def test_the_spread_of_normal_operation_is_the_configured_noise() -> None:
    sensor = _sensor(noise_std_c=0.5)

    samples = [sensor.measure_c(float(second)) for second in range(100_000)]

    # The relative standard error of a standard deviation estimated from n
    # normal samples is 1 / sqrt(2 n) = 0.22 percent. The tolerance of 1.1
    # percent of 0.5 degrees Celsius is five times that.
    assert statistics.stdev(samples) == pytest.approx(0.5, rel=0.011)


def test_a_sensor_without_noise_reports_the_normal_temperature_exactly() -> None:
    sensor = _sensor(noise_std_c=0.0)

    assert sensor.measure_c(12.0) == MOULD.normal_c


def test_the_temperature_stays_normal_before_the_overheating_starts() -> None:
    sensor = _sensor(
        noise_std_c=0.0, overheating=Overheating(start_s=30.0, rate_c_per_min=6.0)
    )

    assert sensor.measure_c(0.0) == MOULD.normal_c
    assert sensor.measure_c(30.0) == MOULD.normal_c


def test_the_overheating_raises_the_temperature_linearly_from_its_start() -> None:
    sensor = _sensor(
        noise_std_c=0.0, overheating=Overheating(start_s=30.0, rate_c_per_min=6.0)
    )

    # Six degrees Celsius per minute are 0.1 degrees Celsius per second.
    assert sensor.measure_c(60.0) == pytest.approx(MOULD.normal_c + 3.0)
    assert sensor.measure_c(90.0) == pytest.approx(MOULD.normal_c + 6.0)
    assert sensor.measure_c(150.0) == pytest.approx(MOULD.normal_c + 12.0)


def test_the_overheating_crosses_the_warning_before_the_critical_threshold() -> None:
    overheating = Overheating(start_s=0.0, rate_c_per_min=6.0)
    sensor = _sensor(noise_std_c=0.0, overheating=overheating)

    times = [float(second) for second in range(0, 600)]
    warning_at = next(t for t in times if sensor.measure_c(t) >= MOULD.warning_c)
    critical_at = next(t for t in times if sensor.measure_c(t) >= MOULD.critical_c)

    # From 70 degrees Celsius at 0.1 degrees Celsius per second: 80 after 100 s,
    # 95 after 250 s.
    assert (warning_at, critical_at) == (100.0, 250.0)


def test_the_noise_is_added_to_the_overheating_course() -> None:
    overheating = Overheating(start_s=0.0, rate_c_per_min=6.0)
    sensor = _sensor(noise_std_c=0.5, overheating=overheating)

    samples = [sensor.measure_c(600.0) for _ in range(20_000)]

    # At 600 s the expected value is 70 + 60 degrees Celsius. The standard error
    # is 0.5 / sqrt(20000) = 0.0035, and the tolerance is five times that.
    assert statistics.fmean(samples) == pytest.approx(130.0, abs=0.018)


def test_two_sensors_with_the_same_seed_report_identical_readings() -> None:
    first = _sensor(seed=7)
    second = _sensor(seed=7)

    assert [first.measure_c(float(t)) for t in range(50)] == [
        second.measure_c(float(t)) for t in range(50)
    ]


def test_two_sensors_with_different_seeds_report_different_readings() -> None:
    first = _sensor(seed=7)
    second = _sensor(seed=8)

    assert [first.measure_c(float(t)) for t in range(50)] != [
        second.measure_c(float(t)) for t in range(50)
    ]


def test_generators_spawned_from_one_seed_are_independent_streams() -> None:
    first, second = spawn_generators(SEED, 2)

    assert first.normal(size=20).tolist() != second.normal(size=20).tolist()


def test_spawned_generators_are_reproducible_for_the_same_seed() -> None:
    first = spawn_generators(SEED, 3)
    second = spawn_generators(SEED, 3)

    assert [g.random() for g in first] == [g.random() for g in second]


def test_the_overheating_rejects_a_rate_that_is_not_positive() -> None:
    with pytest.raises(ValueError, match="rate_c_per_min"):
        Overheating(start_s=0.0, rate_c_per_min=0.0)


@pytest.mark.parametrize("start_s", [-1.0, math.nan])
def test_the_overheating_rejects_a_start_that_is_negative_or_not_a_number(
    start_s: float,
) -> None:
    with pytest.raises(ValueError, match="start_s"):
        Overheating(start_s=start_s, rate_c_per_min=1.0)


def test_create_sensors_returns_one_sensor_per_machine_of_the_plant() -> None:
    plant = default_plant()

    sensors = create_sensors(plant, spawn_generators(SEED, len(plant.machines)))

    assert list(sensors) == [machine.machine_id for machine in plant.machines]


def test_create_sensors_applies_an_overheating_only_to_the_named_machine() -> None:
    plant = default_plant()
    sensors = create_sensors(
        plant,
        spawn_generators(SEED, len(plant.machines)),
        {"oven-02": Overheating(start_s=0.0, rate_c_per_min=60.0)},
    )

    assert sensors["oven-02"].true_temperature_c(60.0) == OVEN.normal_c + 60.0
    assert sensors["oven-01"].true_temperature_c(60.0) == OVEN.normal_c


def test_create_sensors_rejects_an_overheating_for_an_unknown_machine() -> None:
    plant = default_plant()

    with pytest.raises(KeyError, match="unknown"):
        create_sensors(
            plant,
            spawn_generators(SEED, len(plant.machines)),
            {"unknown": Overheating(start_s=0.0, rate_c_per_min=1.0)},
        )


def test_create_sensors_requires_one_generator_per_machine() -> None:
    plant = default_plant()

    with pytest.raises(ValueError, match="one generator per machine"):
        create_sensors(plant, spawn_generators(SEED, 3))


def test_a_sensor_draws_from_the_generator_it_was_given() -> None:
    expected = np.random.Generator(np.random.PCG64(1)).normal(0.0, 0.5)
    sensor = TemperatureSensor(
        MACHINE,
        SensorSettings(noise_std_c=0.5),
        np.random.Generator(np.random.PCG64(1)),
    )

    assert sensor.measure_c(0.0) == MOULD.normal_c + expected
