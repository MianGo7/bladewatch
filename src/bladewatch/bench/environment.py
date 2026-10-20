"""The record of where and on what a measurement ran.

Collects the commit, the date, the hardware, and the Docker resource limits, which
every written result has to name, because a latency or a throughput is meaningless
without them (ADR-0024). The commands are run through a replaceable function, so
that the parsing is tested without the tools.
"""

import platform
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

_GIB = 1024**3


@dataclass(frozen=True, slots=True)
class Environment:
    """Where a measurement ran.

    Attributes:
        date: Date of the run, as YYYY-MM-DD in UTC.
        commit: Short commit hash, with a remark if the working tree has changes.
        hardware: Model, chip, memory, and operating system of the machine.
        docker: Resource limits of the Docker engine.
    """

    date: str
    commit: str
    hardware: str
    docker: str


def _run(command: list[str]) -> str:
    return subprocess.run(
        command, capture_output=True, text=True, check=True, timeout=20
    ).stdout.strip()


def commit_state(run: Callable[[list[str]], str] = _run) -> str:
    """Returns the short commit, with a remark when the working tree has changes."""
    try:
        commit = run(["git", "rev-parse", "--short", "HEAD"])
        dirty = run(["git", "status", "--porcelain"])
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return f"{commit}, with uncommitted changes" if dirty else commit


def describe_hardware(run: Callable[[list[str]], str] = _run) -> str:
    """Describes the machine in one line, as far as the tools tell.

    Args:
        run: Runs a command and returns its output. Replaceable in tests.
    """
    try:
        model = run(["sysctl", "-n", "hw.model"])
        chip = run(["sysctl", "-n", "machdep.cpu.brand_string"])
        memory = int(run(["sysctl", "-n", "hw.memsize"])) / _GIB
        system = run(["sw_vers", "-productVersion"])
    except (OSError, subprocess.SubprocessError, ValueError):
        return f"{platform.machine()}, {platform.system()} {platform.release()}"
    return f"{model} with {chip}, {memory:.0f} GiB of memory, macOS {system}"


def describe_docker(run: Callable[[list[str]], str] = _run) -> str:
    """Describes the CPU and memory limits of the Docker engine in one line.

    Args:
        run: Runs a command and returns its output. Replaceable in tests.
    """
    try:
        output = run(
            ["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}} {{.ServerVersion}}"]
        )
        cpus, memory, version = output.split()
        memory_gib = int(memory) / _GIB
    except (OSError, subprocess.SubprocessError, ValueError):
        return "unknown"
    return (
        f"Docker Engine {version} with the limits of {cpus} CPUs and "
        f"{memory_gib:.3f} GiB of memory"
    )


def collect(now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> Environment:
    """Collects the environment of a measurement."""
    return Environment(
        date=now().strftime("%Y-%m-%d"),
        commit=commit_state(),
        hardware=describe_hardware(),
        docker=describe_docker(),
    )
