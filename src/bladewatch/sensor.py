"""Temperature model of the simulated machines.

A sensor reports the normal operating temperature of its machine type plus
Gaussian measurement noise, and an injected overheating adds a linear rise from
a start time on. The module is pure Python and NumPy without input or output,
and every random number comes from a generator that is passed in. Implements
the sensor part of FR1 and the overheating of FR11.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np

from bladewatch.config import Machine, PlantSettings, SensorSettings


@dataclass(frozen=True, slots=True)
class Overheating:
    """An injected overheating of one machine.

    The temperature rises linearly from the start time on and has no upper
    bound, because the scenario ends before a bound would matter.

    Attributes:
        start_s: Time since the start of the simulation at which the rise
            begins, in seconds.
        rate_c_per_min: Rate of the rise in degrees Celsius per minute.
    """

    start_s: float
    rate_c_per_min: float

    def __post_init__(self) -> None:
        """Validates the start and the rate."""
        if not (math.isfinite(self.start_s) and self.start_s >= 0.0):
            raise ValueError(
                f"Overheating: start_s must be finite and not negative, "
                f"got {self.start_s}"
            )
        if not (math.isfinite(self.rate_c_per_min) and self.rate_c_per_min > 0.0):
            raise ValueError(
                f"Overheating: rate_c_per_min must be positive, "
                f"got {self.rate_c_per_min}"
            )

    def excess_c(self, elapsed_s: float) -> float:
        """Returns the rise above normal operation in degrees Celsius."""
        return max(0.0, elapsed_s - self.start_s) * self.rate_c_per_min / 60.0


class TemperatureSensor:
    """The temperature sensor of one machine.

    ADR-0017: normal operation is a constant at the normal temperature of the
    machine type with independent noise per reading, because an autocorrelated
    process would need a parameter that no source supports.
    """

    def __init__(
        self,
        machine: Machine,
        settings: SensorSettings,
        rng: np.random.Generator,
        overheating: Overheating | None = None,
    ) -> None:
        """Creates a sensor.

        Args:
            machine: The machine that the sensor measures.
            settings: The measurement model, in particular the noise.
            rng: The generator that supplies the noise. It is not shared with
                another sensor, so that the readings of a machine do not depend
                on the other machines.
            overheating: An overheating to inject, or None for normal operation.
        """
        self._machine = machine
        self._noise_std_c = settings.noise_std_c
        self._rng = rng
        self._overheating = overheating

    def true_temperature_c(self, elapsed_s: float) -> float:
        """Returns the noise free temperature in degrees Celsius.

        Args:
            elapsed_s: Time since the start of the simulation in seconds.
        """
        temperature_c = self._machine.machine_type.normal_c
        if self._overheating is None:
            return temperature_c
        return temperature_c + self._overheating.excess_c(elapsed_s)

    def measure_c(self, elapsed_s: float) -> float:
        """Returns one reading in degrees Celsius and advances the generator.

        Args:
            elapsed_s: Time since the start of the simulation in seconds.
        """
        noise_c = float(self._rng.normal(0.0, self._noise_std_c))
        return self.true_temperature_c(elapsed_s) + noise_c


def spawn_generators(seed: int, count: int) -> list[np.random.Generator]:
    """Creates independent, reproducible generators from one seed.

    Args:
        seed: The seed of the whole simulation.
        count: The number of generators, normally one per machine.
    """
    children = np.random.SeedSequence(seed).spawn(count)
    return [np.random.Generator(np.random.PCG64(child)) for child in children]


def create_sensors(
    plant: PlantSettings,
    generators: Sequence[np.random.Generator],
    overheating: Mapping[str, Overheating] | None = None,
) -> dict[str, TemperatureSensor]:
    """Creates one sensor per machine of the plant, in the order of the plant.

    Args:
        plant: The monitored plant.
        generators: One generator per machine, in the order of the plant.
        overheating: Overheatings to inject, by machine identifier.

    Raises:
        ValueError: If the number of generators differs from the number of
            machines.
        KeyError: If an overheating names a machine that is not in the plant.
    """
    if len(generators) != len(plant.machines):
        raise ValueError("create_sensors: one generator per machine is required")
    injected = dict(overheating or {})
    for machine_id in injected:
        plant.machine(machine_id)
    return {
        machine.machine_id: TemperatureSensor(
            machine, plant.sensor, generator, injected.get(machine.machine_id)
        )
        for machine, generator in zip(plant.machines, generators, strict=True)
    }
