import re
from pathlib import Path

COMPOSE_FILE = Path(__file__).parents[1] / "compose.yaml"
EXACT_VERSION_TAG = re.compile(r"^[\w./-]+:\d+\.\d+\.\d+(-[\w.]+)?$")


def _images() -> list[str]:
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    return re.findall(r"^\s+image:\s*(\S+)\s*$", text, flags=re.MULTILINE)


def test_compose_file_declares_at_least_one_image() -> None:
    assert _images()


def test_every_container_image_is_pinned_to_an_exact_version_tag() -> None:
    unpinned = [image for image in _images() if not EXACT_VERSION_TAG.match(image)]

    assert unpinned == []
