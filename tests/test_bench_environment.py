import subprocess
from collections.abc import Callable

import pytest

from bladewatch.bench.environment import (
    commit_state,
    describe_docker,
    describe_hardware,
)


def _runner(answers: dict[str, str]) -> Callable[[list[str]], str]:
    def run(command: list[str]) -> str:
        key = " ".join(command)
        for prefix, answer in answers.items():
            if key.startswith(prefix):
                return answer
        raise subprocess.SubprocessError(key)

    return run


def test_a_clean_working_tree_names_only_the_commit() -> None:
    run = _runner({"git rev-parse": "60af417", "git status": ""})

    assert commit_state(run) == "60af417"


def test_a_working_tree_with_changes_is_named_as_such() -> None:
    run = _runner({"git rev-parse": "60af417", "git status": " M file.py"})

    assert commit_state(run) == "60af417, with uncommitted changes"


def test_an_unknown_commit_is_reported_as_unknown() -> None:
    assert commit_state(_runner({})) == "unknown"


def test_the_hardware_line_names_model_chip_memory_and_system() -> None:
    run = _runner(
        {
            "sysctl -n hw.model": "Mac17,8",
            "sysctl -n machdep": "Apple M5 Pro",
            "sysctl -n hw.memsize": str(48 * 1024**3),
            "sw_vers": "27.0.1",
        }
    )

    assert describe_hardware(run) == (
        "Mac17,8 with Apple M5 Pro, 48 GiB of memory, macOS 27.0.1"
    )


def test_the_hardware_line_falls_back_to_the_platform_without_the_tools() -> None:
    assert describe_hardware(_runner({})) != ""


def test_the_docker_line_names_the_version_and_the_limits() -> None:
    run = _runner({"docker info": f"18 {int(7.746 * 1024**3)} 29.8.2"})

    assert describe_docker(run) == (
        "Docker Engine 29.8.2 with the limits of 18 CPUs and 7.746 GiB of memory"
    )


def test_an_unreadable_docker_answer_is_reported_as_unknown() -> None:
    assert describe_docker(_runner({"docker info": "garbage"})) == "unknown"


@pytest.mark.parametrize("answer", ["", "1 2"])
def test_an_incomplete_docker_answer_is_reported_as_unknown(answer: str) -> None:
    assert describe_docker(_runner({"docker info": answer})) == "unknown"
