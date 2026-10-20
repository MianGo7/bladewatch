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


def test_the_process_command_has_defaults_for_every_option() -> None:
    arguments = build_parser().parse_args(["process"])

    assert arguments.bootstrap_servers == "localhost:9092"
    assert (arguments.db_host, arguments.db_port) == ("localhost", 5432)


def test_the_process_command_takes_a_broker_and_a_database() -> None:
    arguments = build_parser().parse_args(
        ["process", "--bootstrap-servers", "k:1", "--db-host", "db", "--db-port", "6"]
    )

    assert (arguments.bootstrap_servers, arguments.db_host, arguments.db_port) == (
        "k:1",
        "db",
        6,
    )


def test_the_notify_command_listens_on_loopback_at_the_port_of_the_contact_point() -> (
    None
):
    arguments = build_parser().parse_args(["notify"])

    assert (arguments.host, arguments.port) == ("127.0.0.1", 8089)


def test_the_scenario_command_does_not_reset_unless_it_is_asked_to() -> None:
    arguments = build_parser().parse_args(["scenario"])

    assert arguments.reset is False
    assert (arguments.repetitions, arguments.seed, arguments.fast) == (3, 1, False)


def test_the_scenario_command_takes_the_reset_flag_and_its_options() -> None:
    arguments = build_parser().parse_args(
        ["scenario", "--reset", "--repetitions", "1", "--seed", "9", "--fast"]
    )

    assert (arguments.reset, arguments.repetitions, arguments.seed, arguments.fast) == (
        True,
        1,
        9,
        True,
    )


def test_the_producer_and_the_processor_publish_the_twelve_machines_by_default() -> (
    None
):
    parser = build_parser()

    assert parser.parse_args(["produce"]).machines is None
    assert parser.parse_args(["process"]).machines is None


def test_the_load_measurements_start_the_producer_and_the_processor_with_streams() -> (
    None
):
    parser = build_parser()

    produce = parser.parse_args(["produce", "--machines", "2000"])
    process = parser.parse_args(["process", "--machines", "2000", "--group", "g1"])

    assert produce.machines == 2000
    assert (process.machines, process.group) == (2000, "g1")


def test_the_processor_uses_the_consumer_group_of_the_settings_by_default() -> None:
    assert build_parser().parse_args(["process"]).group == "bladewatch-processor"


def test_the_latency_measurement_defaults_to_ten_minutes_and_does_not_reset() -> None:
    arguments = build_parser().parse_args(["measure", "latency"])

    assert arguments.duration_s == 600.0
    assert arguments.reset is False
    assert arguments.repetitions is None


def test_the_throughput_measurement_takes_the_steps_as_a_list() -> None:
    arguments = build_parser().parse_args(
        ["measure", "throughput", "--steps", "12,100,500", "--step-duration-s", "30"]
    )

    assert arguments.steps == (12, 100, 500)
    assert arguments.step_duration_s == 30.0


def test_the_throughput_measurement_defaults_to_the_steps_of_assumption_a9() -> None:
    arguments = build_parser().parse_args(["measure", "throughput"])

    assert arguments.steps[:4] == (12, 100, 500, 2000)
    assert arguments.step_duration_s == 120.0


def test_a_list_of_steps_that_is_not_numeric_is_rejected() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["measure", "throughput", "--steps", "12,x"])


def test_the_fault_measurement_defaults_to_the_reference_load() -> None:
    assert build_parser().parse_args(["measure", "faults"]).streams == 2000


def test_the_figures_command_takes_a_result_file() -> None:
    arguments = build_parser().parse_args(["measure", "figures", "results/m1.json"])

    assert str(arguments.result) == "results/m1.json"
    assert str(arguments.figures_dir) == "docs/figures"


def test_a_measurement_has_to_be_named() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["measure"])
