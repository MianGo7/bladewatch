"""Settings of the prototype as frozen dataclasses.

Holds the machines with their type and thresholds, the sensor model, the topics,
and the connection to Kafka. Every field carries its unit in its name, and every
value is validated on construction, so that an invalid setting fails at start-up
and not inside a running stream. Implements the values of assumptions A1 to A4.
"""

import math
from dataclasses import dataclass, field


def _require_finite(owner: str, **values: float) -> None:
    for name, value in values.items():
        if not math.isfinite(value):
            raise ValueError(f"{owner}: {name} must be finite, got {value}")


@dataclass(frozen=True, slots=True)
class MachineType:
    """The type of a machine with its operating temperature and thresholds.

    Attributes:
        name: Short name of the type, for instance "mould".
        normal_c: Normal operating temperature in degrees Celsius.
        warning_c: Temperature in degrees Celsius at which a warning is raised.
        critical_c: Temperature in degrees Celsius at which a critical alert is
            raised.
    """

    name: str
    normal_c: float
    warning_c: float
    critical_c: float

    def __post_init__(self) -> None:
        """Validates the name and the order of the temperatures."""
        if not self.name:
            raise ValueError("MachineType: name must not be empty")
        _require_finite(
            "MachineType",
            normal_c=self.normal_c,
            warning_c=self.warning_c,
            critical_c=self.critical_c,
        )
        if not self.normal_c < self.warning_c < self.critical_c:
            raise ValueError(
                f"MachineType {self.name}: normal_c < warning_c < critical_c "
                f"must hold, got {self.normal_c}, {self.warning_c}, "
                f"{self.critical_c}"
            )


# A4: the values and their basis are in the assumption table of docs/concept.md.
MOULD = MachineType(name="mould", normal_c=70.0, warning_c=80.0, critical_c=95.0)
OVEN = MachineType(name="oven", normal_c=100.0, warning_c=110.0, critical_c=120.0)
GRINDER = MachineType(name="grinder", normal_c=45.0, warning_c=60.0, critical_c=75.0)


@dataclass(frozen=True, slots=True)
class Machine:
    """A monitored machine with one temperature sensor (A2).

    Attributes:
        machine_id: Identifier of the machine, used as the message key.
        machine_type: Type of the machine, which fixes its thresholds.
    """

    machine_id: str
    machine_type: MachineType

    def __post_init__(self) -> None:
        """Validates the identifier."""
        if not self.machine_id:
            raise ValueError("Machine: machine_id must not be empty")


@dataclass(frozen=True, slots=True)
class SensorSettings:
    """The measurement model of the simulated sensors.

    Attributes:
        interval_s: Time between two readings of one sensor in seconds (A3).
        noise_std_c: Standard deviation of the measurement noise in degrees
            Celsius (A13).
    """

    interval_s: float = 1.0
    noise_std_c: float = 0.5

    def __post_init__(self) -> None:
        """Validates the interval and the noise."""
        if not (math.isfinite(self.interval_s) and self.interval_s > 0.0):
            raise ValueError(
                f"SensorSettings: interval_s must be positive, got {self.interval_s}"
            )
        if not (math.isfinite(self.noise_std_c) and self.noise_std_c >= 0.0):
            raise ValueError(
                "SensorSettings: noise_std_c must be finite and not negative, "
                f"got {self.noise_std_c}"
            )


@dataclass(frozen=True, slots=True)
class RuleSettings:
    """The parameters of the alert rules that the thresholds do not cover.

    Attributes:
        hysteresis_margin_c: Margin in degrees Celsius below the warning
            threshold that the temperature has to reach before an alert closes
            (A14).
        rate_window_s: Length of the sliding window of the rate of rise check in
            seconds (A15).
        rate_limit_c_per_min: Rate of rise in degrees Celsius per minute above
            which a warning is raised (A15).
    """

    hysteresis_margin_c: float = 2.0
    rate_window_s: float = 60.0
    rate_limit_c_per_min: float = 3.0

    def __post_init__(self) -> None:
        """Validates that every parameter is finite and positive."""
        for name in ("hysteresis_margin_c", "rate_window_s", "rate_limit_c_per_min"):
            value = getattr(self, name)
            if not (math.isfinite(value) and value > 0.0):
                raise ValueError(f"RuleSettings: {name} must be positive, got {value}")


@dataclass(frozen=True, slots=True)
class TopicSettings:
    """The names of the Kafka topics, which match infra and compose.yaml.

    Attributes:
        readings: Topic that carries the readings.
        alerts: Topic that carries the alerts.
    """

    readings: str = "machine-temperature"
    alerts: str = "machine-alerts"

    def __post_init__(self) -> None:
        """Validates that both names are set and different."""
        if not self.readings or not self.alerts:
            raise ValueError("TopicSettings: topic names must be non-empty")
        if self.readings == self.alerts:
            raise ValueError("TopicSettings: topic names must be distinct")


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """The connection to the Kafka broker.

    Attributes:
        bootstrap_servers: Comma separated host and port pairs of the brokers.
    """

    bootstrap_servers: str = "localhost:9092"

    def __post_init__(self) -> None:
        """Validates the address."""
        if not self.bootstrap_servers:
            raise ValueError("KafkaSettings: bootstrap_servers must not be empty")


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """The connection to TimescaleDB.

    The defaults are the development credentials of compose.yaml, which protect
    nothing beyond the developer machine.

    Attributes:
        host: Host name or address of the database.
        port: TCP port of the database.
        dbname: Name of the database.
        user: Name of the database user.
        password: Password of the database user.
    """

    host: str = "localhost"
    port: int = 5432
    dbname: str = "bladewatch"
    user: str = "bladewatch"
    password: str = "bladewatch"

    def __post_init__(self) -> None:
        """Validates that the address and the names are set."""
        if not (self.host and self.dbname and self.user):
            raise ValueError("DatabaseSettings: host, dbname, and user are required")
        if not 0 < self.port < 65536:
            raise ValueError(f"DatabaseSettings: invalid port {self.port}")


@dataclass(frozen=True, slots=True)
class ProcessorSettings:
    """The parameters of the stream processor.

    Attributes:
        consumer_group: Kafka consumer group of the processor.
        batch_size: Largest number of readings that one batch holds (A17).
        poll_timeout_s: Longest wait for a full batch in seconds (A17). A
            partial batch is processed when it expires, so the value is the
            largest delay that batching adds to the latency.
    """

    consumer_group: str = "bladewatch-processor"
    batch_size: int = 500
    poll_timeout_s: float = 0.2

    def __post_init__(self) -> None:
        """Validates the group and the batch parameters."""
        if not self.consumer_group:
            raise ValueError("ProcessorSettings: consumer_group must not be empty")
        if self.batch_size < 1:
            raise ValueError(
                "ProcessorSettings: batch_size must be at least 1, "
                f"got {self.batch_size}"
            )
        if not (math.isfinite(self.poll_timeout_s) and self.poll_timeout_s > 0.0):
            raise ValueError(
                "ProcessorSettings: poll_timeout_s must be positive, "
                f"got {self.poll_timeout_s}"
            )


@dataclass(frozen=True, slots=True)
class PlantSettings:
    """The monitored plant: its machines, its sensors, its topics, and Kafka.

    Attributes:
        machines: The monitored machines, with unique identifiers.
        sensor: The measurement model shared by every sensor.
        rules: The parameters of the alert rules.
        topics: The Kafka topics.
        kafka: The connection to the broker.
        database: The connection to TimescaleDB.
        processor: The parameters of the stream processor.
    """

    machines: tuple[Machine, ...]
    sensor: SensorSettings = field(default_factory=SensorSettings)
    rules: RuleSettings = field(default_factory=RuleSettings)
    topics: TopicSettings = field(default_factory=TopicSettings)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    processor: ProcessorSettings = field(default_factory=ProcessorSettings)

    def __post_init__(self) -> None:
        """Validates that the machine identifiers are unique."""
        if not self.machines:
            raise ValueError("PlantSettings: at least one machine is required")
        identifiers = [machine.machine_id for machine in self.machines]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("PlantSettings: machine identifiers must be unique")

    def machine(self, machine_id: str) -> Machine:
        """Returns the machine with the given identifier.

        Args:
            machine_id: Identifier of the machine.

        Raises:
            KeyError: If no machine has that identifier.
        """
        for machine in self.machines:
            if machine.machine_id == machine_id:
                return machine
        raise KeyError(f"unknown machine {machine_id}")


def default_plant() -> PlantSettings:
    """Returns the plant of assumption A1, twelve machines in three types."""
    machines = tuple(
        Machine(f"{machine_type.name}-{index:02d}", machine_type)
        for machine_type in (MOULD, OVEN, GRINDER)
        for index in range(1, 5)
    )
    return PlantSettings(machines=machines)
