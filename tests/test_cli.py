import pytest

from bladewatch.cli import build_parser


def test_the_produce_command_has_defaults_for_every_option() -> None:
    arguments = build_parser().parse_args(["produce"])

    assert arguments.seed == 1
    assert arguments.duration_s is None
    assert arguments.bootstrap_servers == "localhost:9092"


def test_the_produce_command_takes_a_seed_a_duration_and_a_broker() -> None:
    arguments = build_parser().parse_args(
        ["produce", "--seed", "7", "--duration-s", "2.5", "--bootstrap-servers", "k:1"]
    )

    assert (arguments.seed, arguments.duration_s, arguments.bootstrap_servers) == (
        7,
        2.5,
        "k:1",
    )


def test_a_command_is_required() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])
