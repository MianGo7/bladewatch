import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
PACKAGE = ROOT / "src" / "bladewatch"
DIAGRAMS = ROOT / "docs" / "diagrams"
ARROW = re.compile(r"^\s*(\w+)\s+-(?:up|down|left|right)?->\s+(\w+)", re.MULTILINE)
COMPONENT = re.compile(r'^\s*component\s+"[^"]+"\s+as\s+(\w+)', re.MULTILINE)
# The alias of a component in the diagram, where it differs from the name that the
# import statement uses.
ALIASES = {"__main__": "main"}


def _unit(path: Path) -> str:
    """Returns the module or the package that a source file belongs to."""
    return path.relative_to(PACKAGE).parts[0].removesuffix(".py")


def _imports() -> set[tuple[str, str]]:
    """Reads the imports between the units of the package and its libraries."""
    edges: set[tuple[str, str]] = set()
    for path in PACKAGE.rglob("*.py"):
        unit = _unit(path)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            elif isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            else:
                continue
            for name in names:
                parts = name.split(".")
                if parts[0] == "bladewatch":
                    target = parts[1] if len(parts) > 1 else None
                elif parts[0] in sys.stdlib_module_names:
                    target = None
                else:
                    target = parts[0]
                if target and target != unit:
                    edges.add((ALIASES.get(unit, unit), ALIASES.get(target, target)))
    return edges


def _text() -> str:
    return (DIAGRAMS / "package-components.puml").read_text(encoding="utf-8")


def _units() -> set[str]:
    units = {ALIASES.get(_unit(p), _unit(p)) for p in PACKAGE.rglob("*.py")}
    return units - {"__init__"}


def test_the_component_diagram_shows_exactly_the_imports_of_the_source_files() -> None:
    drawn = set(ARROW.findall(_text()))

    # The imports of config are not drawn and are checked separately.
    assert drawn == {edge for edge in _imports() if edge[1] != "config"}


def test_every_module_except_main_and_messages_imports_the_settings() -> None:
    importers = {source for source, target in _imports() if target == "config"}

    assert importers == _units() - {"main", "messages", "config"}
    assert "except main and messages" in _text()


def test_the_component_diagram_draws_no_arrow_twice() -> None:
    arrows = ARROW.findall(_text())

    assert len(arrows) == len(set(arrows))


def test_the_component_diagram_has_a_component_for_every_unit_and_library() -> None:
    units = _units()
    libraries = {target for _, target in _imports()} - units
    declared = set(COMPONENT.findall(_text()))

    assert declared == units | libraries


def test_every_arrow_of_the_component_diagram_joins_declared_components() -> None:
    declared = set(COMPONENT.findall(_text()))

    for source, target in ARROW.findall(_text()):
        assert {source, target} <= declared


# The deployment diagram against compose.yaml

COMPOSE = (ROOT / "compose.yaml").read_text(encoding="utf-8")


def _deployment() -> str:
    return (DIAGRAMS / "deployment.puml").read_text(encoding="utf-8")


def _compose_services() -> set[str]:
    section = COMPOSE.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    return set(re.findall(r"^  ([\w-]+):\s*$", section, flags=re.MULTILINE))


def _compose_volumes() -> set[str]:
    section = COMPOSE.split("\nvolumes:\n", 1)[1]
    return set(re.findall(r"^  ([\w-]+):\s*$", section, flags=re.MULTILINE))


def test_the_deployment_diagram_shows_every_service_of_the_compose_file() -> None:
    drawn = set(
        re.findall(r'node\s+"([\w-]+)"\s+as\s+\w+\s+<<container>>', _deployment())
    )

    assert drawn == _compose_services()


def test_the_deployment_diagram_shows_every_named_volume_of_the_compose_file() -> None:
    drawn = set(
        re.findall(r'database\s+"([\w-]+)"\s+as\s+\w+\s+<<volume>>', _deployment())
    )

    assert drawn == _compose_volumes()


def test_the_deployment_diagram_names_every_published_address_and_image() -> None:
    addresses = re.findall(r'- "(127\.0\.0\.1:\d+):\d+"', COMPOSE)
    images = re.findall(r"^\s+image:\s*(\S+)\s*$", COMPOSE, flags=re.MULTILINE)

    assert addresses
    for name in [*addresses, *images]:
        assert name in _deployment(), name


def test_the_deployment_diagram_shows_the_topics_with_their_partitions() -> None:
    created = re.findall(r"--topic ([\w-]+) --partitions (\d+)", COMPOSE)
    drawn = re.findall(r'queue\s+"([\w-]+)\\n(\d+) partitions', _deployment())

    assert created
    assert sorted(drawn) == sorted(created)


def test_the_deployment_diagram_shows_the_bind_mounts_of_the_compose_file() -> None:
    mounts = re.findall(r"^\s+- \./([\w/.-]+):", COMPOSE, flags=re.MULTILINE)

    assert mounts
    for mount in mounts:
        assert mount in _deployment(), mount
