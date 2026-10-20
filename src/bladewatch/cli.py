"""Command line entry point of BladeWatch, started with `python -m bladewatch`."""

import argparse
import dataclasses
import logging
import signal
import sys
import threading
from collections.abc import Sequence
from datetime import UTC, datetime
from types import FrameType

from bladewatch.config import (
    DatabaseSettings,
    KafkaSettings,
    NotifierSettings,
    default_plant,
)
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
    return parser


def _produce(seed: int, duration_s: float | None, bootstrap_servers: str) -> int:
    plant = dataclasses.replace(
        default_plant(), kafka=KafkaSettings(bootstrap_servers=bootstrap_servers)
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


def _process(bootstrap_servers: str, db_host: str, db_port: int) -> int:
    plant = dataclasses.replace(
        default_plant(),
        kafka=KafkaSettings(bootstrap_servers=bootstrap_servers),
        database=DatabaseSettings(host=db_host, port=db_port),
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
            arguments.seed, arguments.duration_s, arguments.bootstrap_servers
        )
    if arguments.command == "process":
        return _process(
            arguments.bootstrap_servers, arguments.db_host, arguments.db_port
        )
    if arguments.command == "notify":
        return _notify(arguments.host, arguments.port)
    raise AssertionError(f"unhandled command {arguments.command}")
