"""Command line entry point of BladeWatch, started with `python -m bladewatch`."""

import argparse
import dataclasses
import logging
import signal
import sys
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from bladewatch.bench.latency import M1_DURATION_S
from bladewatch.bench.run import measure_faults, measure_latency, measure_throughput
from bladewatch.config import (
    DatabaseSettings,
    FaultSettings,
    KafkaSettings,
    NotifierSettings,
    PlantSettings,
    ProcessorSettings,
    ScenarioSettings,
    ThroughputSettings,
    default_plant,
    scaled_plant,
)
from bladewatch.measure import MeasureError
from bladewatch.measure.render import render
from bladewatch.notifier import NotificationServer
from bladewatch.processor import (
    ProcessorError,
    StreamProcessor,
    create_kafka_consumer,
)
from bladewatch.producer import (
    PublishError,
    ReadingPublisher,
    create_kafka_producer,
    run,
)
from bladewatch.scenario import Options, ScenarioError, run_scenario
from bladewatch.storage import Storage, StorageError

logger = logging.getLogger(__name__)

_DEFAULT_SEED = 1


def build_parser() -> argparse.ArgumentParser:
    """Builds the argument parser with one sub command per entry point."""
    parser = argparse.ArgumentParser(
        prog="bladewatch",
        description="Prototype of the real-time temperature monitoring of machines.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="verbosity of the log output",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    produce = commands.add_parser(
        "produce", help="publish simulated readings to the Kafka topic"
    )
    produce.add_argument(
        "--seed",
        type=int,
        default=_DEFAULT_SEED,
        help="seed of the simulation; equal seeds give equal readings",
    )
    produce.add_argument(
        "--duration-s",
        type=float,
        default=None,
        help="length of the run in seconds; without it the run continues until "
        "it is interrupted",
    )
    produce.add_argument(
        "--machines",
        type=int,
        default=None,
        help="number of streams of the load measurements, one reading per second "
        "each; without it the twelve machines of the plant publish",
    )
    produce.add_argument(
        "--bootstrap-servers",
        default=KafkaSettings().bootstrap_servers,
        help="address of the Kafka broker, host and port",
    )

    process = commands.add_parser(
        "process",
        help="consume readings, apply the alert rules, and store the results",
    )
    process.add_argument(
        "--bootstrap-servers",
        default=KafkaSettings().bootstrap_servers,
        help="address of the Kafka broker, host and port",
    )
    process.add_argument(
        "--machines",
        type=int,
        default=None,
        help="number of streams of the load measurements; it has to equal the "
        "number of the producer",
    )
    process.add_argument(
        "--group",
        default=ProcessorSettings().consumer_group,
        help="Kafka consumer group of the processor",
    )
    process.add_argument(
        "--db-host",
        default=DatabaseSettings().host,
        help="host of TimescaleDB",
    )
    process.add_argument(
        "--db-port",
        type=int,
        default=DatabaseSettings().port,
        help="port of TimescaleDB",
    )

    notify = commands.add_parser(
        "notify",
        help="receive the notifications of the Grafana contact point and log them",
    )
    notify.add_argument(
        "--host",
        default=NotifierSettings().host,
        help="address to listen on; the contact point reaches it from the container",
    )
    notify.add_argument(
        "--port",
        type=int,
        default=NotifierSettings().port,
        help="port to listen on; it has to match the contact point in infra/grafana",
    )

    scenario = commands.add_parser(
        "scenario",
        help="run the demonstration scenario for the steering committee",
    )
    scenario.add_argument(
        "--reset",
        action="store_true",
        help="delete the data of earlier runs from the topics and the tables "
        "first; without it the scenario refuses to start on a stack that holds data",
    )
    scenario.add_argument(
        "--repetitions",
        type=int,
        default=ScenarioSettings().repetitions,
        help="number of runs; more than one needs --reset",
    )
    scenario.add_argument(
        "--seed",
        type=int,
        default=ScenarioSettings().seed,
        help="seed of the simulation; equal seeds give equal readings",
    )
    scenario.add_argument(
        "--fast",
        action="store_true",
        help="publish without waiting; the alerts are the same but no latency "
        "is meaningful, and nothing is written to the output directory",
    )
    scenario.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results"),
        help="directory for the results of the measurement M4",
    )

    measure = commands.add_parser(
        "measure",
        help="run the measurements M1 to M3 or render their tables and figures",
    )
    kinds = measure.add_subparsers(dest="measurement", required=True)
    defaults = ThroughputSettings(), FaultSettings()

    def common(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--reset",
            action="store_true",
            help="delete the data of earlier runs from the topics and the tables "
            "first; without it the measurement refuses to start on a stack that "
            "holds data",
        )
        command.add_argument(
            "--output-dir",
            type=Path,
            default=Path("results"),
            help="directory for the raw results",
        )
        command.add_argument(
            "--repetitions", type=int, default=None, help="number of runs"
        )
        command.add_argument(
            "--seed", type=int, default=_DEFAULT_SEED, help="seed of the simulation"
        )

    latency = kinds.add_parser("latency", help="M1, the latency of every stage")
    common(latency)
    latency.add_argument(
        "--duration-s", type=float, default=M1_DURATION_S, help="seconds per run"
    )
    throughput = kinds.add_parser("throughput", help="M2, the ramp of load steps")
    common(throughput)
    throughput.add_argument(
        "--steps",
        type=_steps,
        default=defaults[0].steps,
        help="offered readings per second of each step, separated by commas",
    )
    throughput.add_argument(
        "--step-duration-s",
        type=float,
        default=defaults[0].step_duration_s,
        help="seconds of load per step",
    )
    faults = kinds.add_parser(
        "faults", help="M3, the processor is stopped and started again"
    )
    common(faults)
    faults.add_argument(
        "--streams",
        type=int,
        default=defaults[1].streams,
        help="offered readings per second",
    )
    figures = kinds.add_parser(
        "figures", help="render the table and the figure of a raw result"
    )
    figures.add_argument("result", type=Path, help="the JSON file of a measurement")
    figures.add_argument(
        "--figures-dir",
        type=Path,
        default=Path("docs/figures"),
        help="directory for the table and the figure",
    )
    return parser


def _steps(text: str) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in text.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"not a list of integers: {text}") from error


def _plant(machines: int | None) -> PlantSettings:
    return default_plant() if machines is None else scaled_plant(machines)


def _produce(
    seed: int,
    duration_s: float | None,
    bootstrap_servers: str,
    machines: int | None = None,
) -> int:
    plant = dataclasses.replace(
        _plant(machines), kafka=KafkaSettings(bootstrap_servers=bootstrap_servers)
    )
    publisher = ReadingPublisher(
        create_kafka_producer(plant.kafka), plant.topics.readings
    )
    logger.info(
        "publishing %d machines to %s on %s with seed %d",
        len(plant.machines),
        plant.topics.readings,
        plant.kafka.bootstrap_servers,
        seed,
    )
    try:
        run(plant, seed, publisher, datetime.now(UTC), duration_s)
    except KeyboardInterrupt:
        logger.info("interrupted, %d readings delivered", publisher.delivered)
        return 130
    except PublishError as error:
        logger.error("%s", error)
        return 1
    return 0


def _process(
    bootstrap_servers: str,
    db_host: str,
    db_port: int,
    machines: int | None = None,
    group: str = ProcessorSettings().consumer_group,
) -> int:
    plant = dataclasses.replace(
        _plant(machines),
        kafka=KafkaSettings(bootstrap_servers=bootstrap_servers),
        database=DatabaseSettings(host=db_host, port=db_port),
        processor=ProcessorSettings(consumer_group=group),
    )
    stop = threading.Event()

    def request_stop(signal_number: int, frame: FrameType | None) -> None:
        # The batch that is running is finished, so that its offsets are
        # committed after its database transaction.
        logger.info("stop requested, finishing the current batch")
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        with Storage(plant.database) as storage:
            processor = StreamProcessor(
                plant,
                create_kafka_consumer(plant),
                create_kafka_producer(plant.kafka),
                storage,
            )
            logger.info(
                "processing %s on %s in group %s",
                plant.topics.readings,
                plant.kafka.bootstrap_servers,
                plant.processor.consumer_group,
            )
            processor.run(stop.is_set)
    except (StorageError, ProcessorError) as error:
        logger.error("%s", error)
        return 1
    return 0


def _notify(host: str, port: int) -> int:
    server = NotificationServer(NotifierSettings(host=host, port=port))

    def request_stop(signal_number: int, frame: FrameType | None) -> None:
        logger.info("stop requested")
        # shutdown waits for serve_forever, which runs in this thread, so it has
        # to be called from another one.
        threading.Thread(target=server.shutdown).start()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    logger.info("waiting for notifications on %s:%d%s", host, server.port, "/notify")
    server.serve_forever()
    return 0


def _scenario(
    reset: bool, repetitions: int, seed: int, fast: bool, output_dir: Path
) -> int:
    scenario = ScenarioSettings(seed=seed, repetitions=repetitions)
    options = Options(
        pace=not fast, watch_dashboard=not fast, show_notifications=not fast
    )
    if fast:
        logger.warning("fast mode: the latencies of this run mean nothing")
    try:
        run_scenario(default_plant(), scenario, reset, options, output_dir)
    except (ScenarioError, StorageError, ProcessorError) as error:
        logger.error("%s", error)
        return 2
    return 0


def _measure(arguments: argparse.Namespace) -> int:
    plant = default_plant()
    try:
        if arguments.measurement == "figures":
            for path in render(arguments.result, arguments.figures_dir):
                logger.info("written %s", path)
            return 0
        if arguments.measurement == "latency":
            measure_latency(
                plant,
                arguments.seed,
                arguments.repetitions or ScenarioSettings().repetitions,
                arguments.duration_s,
                arguments.reset,
                arguments.output_dir,
            )
        elif arguments.measurement == "throughput":
            measure_throughput(
                plant,
                dataclasses.replace(
                    ThroughputSettings(),
                    steps=arguments.steps,
                    step_duration_s=arguments.step_duration_s,
                    seed=arguments.seed,
                    repetitions=arguments.repetitions
                    or ThroughputSettings().repetitions,
                ),
                arguments.reset,
                arguments.output_dir,
            )
        else:
            measure_faults(
                plant,
                dataclasses.replace(
                    FaultSettings(),
                    streams=arguments.streams,
                    seed=arguments.seed,
                    repetitions=arguments.repetitions or FaultSettings().repetitions,
                ),
                arguments.reset,
                arguments.output_dir,
            )
    except (MeasureError, ScenarioError, StorageError, ProcessorError) as error:
        logger.error("%s", error)
        return 2
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the command line and returns the exit status.

    Args:
        argv: The arguments without the program name, or None for sys.argv.
    """
    arguments = build_parser().parse_args(argv)
    logging.basicConfig(
        level=arguments.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if arguments.command == "produce":
        return _produce(
            arguments.seed,
            arguments.duration_s,
            arguments.bootstrap_servers,
            arguments.machines,
        )
    if arguments.command == "process":
        return _process(
            arguments.bootstrap_servers,
            arguments.db_host,
            arguments.db_port,
            arguments.machines,
            arguments.group,
        )
    if arguments.command == "notify":
        return _notify(arguments.host, arguments.port)
    if arguments.command == "scenario":
        return _scenario(
            arguments.reset,
            arguments.repetitions,
            arguments.seed,
            arguments.fast,
            arguments.output_dir,
        )
    if arguments.command == "measure":
        return _measure(arguments)
    raise AssertionError(f"unhandled command {arguments.command}")
