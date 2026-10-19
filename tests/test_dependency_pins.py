import re
import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).parents[1] / "pyproject.toml"
EXACT_PIN = re.compile(r"^[A-Za-z0-9_.\[\]-]+==\d+(\.\d+)*$")


def _declared_dependencies() -> list[str]:
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    runtime = project["project"]["dependencies"]
    groups = [
        requirement
        for requirements in project.get("dependency-groups", {}).values()
        for requirement in requirements
    ]
    return [*runtime, *groups]


def test_every_python_dependency_is_pinned_to_an_exact_version() -> None:
    unpinned = [
        requirement
        for requirement in _declared_dependencies()
        if not EXACT_PIN.match(requirement)
    ]

    assert unpinned == []
