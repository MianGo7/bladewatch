import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from bladewatch.config import GRINDER, MOULD, OVEN

from .pipeline import (
    START,
    TICKS,
    Environment,
    isolated_environment,
    make_ticks,
    publish,
    run_until_stored,
)
from .stack import grafana, sql

pytestmark = pytest.mark.integration

DASHBOARD_FILE = (
    Path(__file__).parents[2] / "infra" / "grafana" / "dashboards" / "bladewatch.json"
)
DASHBOARD_UID = "bladewatch-overview"


@pytest.fixture(scope="module")
def seeded() -> Iterator[Environment]:
    """A run of the pipeline that ends with an open critical alert of one mould."""
    with isolated_environment() as environment:
        publish(environment, make_ticks(environment, 0, TICKS))
        run_until_stored(environment, TICKS * 3)
        # The hourly aggregate of data older than its refresh window is
        # materialised by a refresh, which the policy does every 15 minutes.
        sql("CALL refresh_continuous_aggregate('readings_hourly', NULL, NULL)")
        yield environment


def _panels() -> dict[int, dict[str, object]]:
    document = json.loads(DASHBOARD_FILE.read_text(encoding="utf-8"))
    return {panel["id"]: panel for panel in document["panels"]}


def _run_queries(
    panel_id: int, machine: str = "", window: tuple[datetime, datetime] | None = None
) -> list[dict[str, list[object]]]:
    """Runs the queries of a panel through Grafana and returns its frames."""
    start, end = window or (START - timedelta(minutes=1), datetime.now(UTC))
    queries = []
    for target in _panels()[panel_id]["targets"]:
        query = dict(target)
        query["rawSql"] = query["rawSql"].replace("$machine", machine)
        query["intervalMs"] = 1000
        query["maxDataPoints"] = 2000
        queries.append(query)
    response = grafana(
        "/api/ds/query",
        {
            "queries": queries,
            "from": str(int(start.timestamp() * 1000)),
            "to": str(int(end.timestamp() * 1000)),
        },
    )
    frames = []
    for result in response["results"].values():
        assert "error" not in result, result.get("error")
        for frame in result["frames"]:
            names = [field["name"] for field in frame["schema"]["fields"]]
            frames.append(
                {
                    "frame": frame["schema"].get("name", ""),
                    **dict(zip(names, frame["data"]["values"], strict=True)),
                }
            )
    return frames


def _rows(panel_id: int, machine: str = "") -> list[dict[str, object]]:
    (frame,) = _run_queries(panel_id, machine)
    columns = [name for name in frame if name != "frame"]
    return [
        dict(zip(columns, values, strict=True))
        for values in zip(*(frame[c] for c in columns), strict=True)
    ]


def test_the_dashboard_is_provisioned_from_the_file() -> None:
    response = grafana(f"/api/dashboards/uid/{DASHBOARD_UID}")

    assert response["meta"]["provisioned"] is True
    assert response["dashboard"]["title"] == "BladeWatch machine monitoring"
    assert len(response["dashboard"]["panels"]) == len(_panels())


def test_the_provisioned_dashboard_refreshes_every_second() -> None:
    response = grafana(f"/api/dashboards/uid/{DASHBOARD_UID}")

    assert response["dashboard"]["refresh"] == "1s"


def test_the_machine_variable_lists_the_registered_machines(
    seeded: Environment,
) -> None:
    sql_text = json.loads(DASHBOARD_FILE.read_text(encoding="utf-8"))["templating"][
        "list"
    ][0]["query"]
    response = grafana(
        "/api/ds/query",
        {
            "queries": [
                {
                    "refId": "A",
                    "datasource": {
                        "type": "grafana-postgresql-datasource",
                        "uid": "bladewatch-timescaledb",
                    },
                    "rawSql": sql_text,
                    "format": "table",
                    "rawQuery": True,
                    "editorMode": "code",
                }
            ],
            "from": "now-1h",
            "to": "now",
        },
    )

    identifiers = response["results"]["A"]["frames"][0]["data"]["values"][0]
    assert seeded.mould_id in identifiers


def test_the_status_table_shows_every_machine_with_its_state_and_thresholds(
    seeded: Environment,
) -> None:
    rows = {r["Machine"]: r for r in _rows(1) if r["Machine"].startswith(seeded.prefix)}

    assert set(rows) == {
        f"{seeded.prefix}-mould-01",
        f"{seeded.prefix}-oven-01",
        f"{seeded.prefix}-grinder-01",
    }
    mould = rows[seeded.mould_id]
    assert mould["State"] == "critical"
    assert (mould["Warning (°C)"], mould["Critical (°C)"]) == (
        MOULD.warning_c,
        MOULD.critical_c,
    )
    assert mould["Temp. (°C)"] >= MOULD.critical_c
    oven = rows[f"{seeded.prefix}-oven-01"]
    assert (oven["State"], oven["Warning (°C)"], oven["Critical (°C)"]) == (
        "normal",
        OVEN.warning_c,
        OVEN.critical_c,
    )
    grinder = rows[f"{seeded.prefix}-grinder-01"]
    assert (grinder["Warning (°C)"], grinder["Critical (°C)"]) == (
        GRINDER.warning_c,
        GRINDER.critical_c,
    )


@pytest.mark.parametrize(
    ("panel_id", "machine_type"), [(2, "mould"), (3, "oven"), (4, "grinder")]
)
def test_the_time_series_panel_of_a_type_shows_only_machines_of_that_type(
    seeded: Environment, panel_id: int, machine_type: str
) -> None:
    (frame,) = _run_queries(panel_id)

    # Grafana joins the series into one wide frame with a field per machine.
    series = {
        name: values for name, values in frame.items() if name.startswith(seeded.prefix)
    }
    assert set(series) == {f"{seeded.prefix}-{machine_type}-01"}
    # The frame also holds the times of any other machine, with gaps for this one,
    # so only the points of this machine are counted.
    (values,) = series.values()
    assert len([value for value in values if value is not None]) == TICKS


def test_the_open_alerts_panel_shows_the_critical_alert_of_the_processor(
    seeded: Environment,
) -> None:
    rows = [r for r in _rows(5) if r["Machine"] == seeded.mould_id]

    assert len(rows) == 1
    assert (rows[0]["Severity"], rows[0]["Kind"]) == ("critical", "threshold")
    assert rows[0]["Limit"] == MOULD.critical_c


def test_the_recent_events_panel_shows_the_raise_and_the_escalation(
    seeded: Environment,
) -> None:
    rows = [r for r in _rows(6) if r["Machine"] == seeded.mould_id]

    assert [(r["Severity"], r["Kind"], r["Status"]) for r in rows] == [
        ("critical", "threshold", "raised"),
        ("warning", "rate_of_rise", "raised"),
    ]


def test_the_history_panel_shows_minimum_mean_and_maximum_of_the_hours(
    seeded: Environment,
) -> None:
    # The panel itself looks back 24 hours. A bucket is labelled with the start
    # of its hour, so the window has to begin before the hour of the first reading.
    window = (START - timedelta(hours=2), datetime.now(UTC))
    frames = _run_queries(7, machine=seeded.mould_id, window=window)

    (frame,) = frames
    assert {"minimum", "mean", "maximum"} <= set(frame)
    assert len(frame["minimum"]) >= 1
    assert all(
        low <= mean <= high
        for low, mean, high in zip(
            frame["minimum"], frame["mean"], frame["maximum"], strict=True
        )
    )
