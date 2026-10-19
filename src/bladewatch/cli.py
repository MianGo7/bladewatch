"""Command line entry point of BladeWatch, started with `python -m bladewatch`."""

import argparse
import dataclasses
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

from bladewatch.config import KafkaSettings, default_plant
from bladewatch.producer import (
    PublishError,
    ReadingPublisher,
    create_kafka_producer,
    run,
)

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
    raise AssertionError(f"unhandled command {arguments.command}")
