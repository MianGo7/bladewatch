import tomllib
from pathlib import Path

import bladewatch


def test_package_version_matches_the_project_metadata() -> None:
    pyproject = Path(__file__).parents[1] / "pyproject.toml"
    declared = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"][
        "version"
    ]

    assert bladewatch.__version__ == declared
