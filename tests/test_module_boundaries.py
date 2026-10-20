import ast
import sys
from pathlib import Path

import pytest

PACKAGE = Path(__file__).parents[1] / "src" / "bladewatch"
PURE_MODULES = ["config", "sensor", "rules", "messages"]
# Libraries that the pure modules must not import, so that they run in a plain
# unit test without Docker (NFR7).
FORBIDDEN = {"confluent_kafka", "psycopg", "psycopg2", "matplotlib"}


def _imported_top_level_names(module: str) -> set[str]:
    tree = ast.parse((PACKAGE / f"{module}.py").read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module.split(".")[0])
    return names


@pytest.mark.parametrize("module", PURE_MODULES)
def test_pure_module_imports_neither_the_kafka_client_nor_a_database_driver(
    module: str,
) -> None:
    assert _imported_top_level_names(module) & FORBIDDEN == set()


def test_the_sensor_module_does_not_import_the_message_format() -> None:
    sensor_imports = ast.parse((PACKAGE / "sensor.py").read_text(encoding="utf-8"))
    modules = {
        node.module
        for node in ast.walk(sensor_imports)
        if isinstance(node, ast.ImportFrom) and node.module
    }

    assert "bladewatch.messages" not in modules


def test_only_the_storage_module_imports_the_database_driver() -> None:
    importers = {
        path.stem
        for path in PACKAGE.glob("*.py")
        if "psycopg" in _imported_top_level_names(path.stem)
    }

    assert importers == {"storage"}


def test_the_notifier_imports_only_the_standard_library_and_the_settings() -> None:
    tree = ast.parse((PACKAGE / "notifier.py").read_text(encoding="utf-8"))
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)

    foreign = {
        module
        for module in modules
        if module.split(".")[0] not in sys.stdlib_module_names
        and module != "bladewatch.config"
    }
    assert foreign == set()
