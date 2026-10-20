import base64
import json
import socket
import subprocess
import urllib.request
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).parents[2]
SERVICE_PORTS = {"kafka": 9092, "timescaledb": 5432, "grafana": 3000}
DATABASE_COMMAND = ["psql", "-U", "bladewatch", "-d", "bladewatch", "-At", "-F", "|"]


def port_is_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1):
            return True
    except OSError:
        return False


def compose(
    *arguments: str, check: bool = True, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Runs a Docker Compose command in the repository root and captures its output."""
    return subprocess.run(
        ["docker", "compose", *arguments],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=check,
        input=stdin,
        timeout=120,
    )


def sql(*statements: str) -> list[list[str]]:
    """Runs statements through psql in the database container and returns the rows."""
    command = [argument for statement in statements for argument in ("-c", statement)]
    result = compose("exec", "-T", "timescaledb", *DATABASE_COMMAND, *command)
    return [line.split("|") for line in result.stdout.splitlines() if line]


def container_states() -> dict[str, dict[str, object]]:
    """Returns the state of every container of the stack by service name."""
    output = compose("ps", "-a", "--format", "json").stdout
    rows = [json.loads(line) for line in output.splitlines() if line]
    return {row["Service"]: row for row in rows}


def delete_consumer_group(group_id: str) -> None:
    """Removes a consumer group that a test created, so that it leaves no trace."""
    compose(
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "localhost:29092",
        "--delete",
        "--group",
        group_id,
        check=False,
    )


def grafana(
    path: str, body: dict[str, object] | None = None
) -> dict[str, object] | list[object]:
    """Calls the Grafana HTTP interface with the development credentials.

    A request with a body is a POST, and one without is a GET.
    """
    credentials = base64.b64encode(b"admin:bladewatch").decode()
    headers = {"Authorization": f"Basic {credentials}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"http://localhost:3000{path}", data=data, headers=headers
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)
