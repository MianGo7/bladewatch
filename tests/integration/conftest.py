import pytest

from .stack import SERVICE_PORTS, port_is_open


@pytest.fixture(scope="session", autouse=True)
def stack_is_running() -> None:
    closed = [name for name, port in SERVICE_PORTS.items() if not port_is_open(port)]
    if closed:
        pytest.skip(
            f"The stack is not running ({', '.join(closed)} unreachable). "
            "Start it with `docker compose up -d --wait`."
        )
