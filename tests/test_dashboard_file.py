import json
import re
from pathlib import Path

import pytest

from bladewatch.config import GRINDER, MOULD, OVEN, MachineType, NotifierSettings
from bladewatch.notifier import NOTIFY_PATH

ROOT = Path(__file__).parents[1]
GRAFANA = ROOT / "infra" / "grafana"
DASHBOARD = json.loads((GRAFANA / "dashboards" / "bladewatch.json").read_text("utf-8"))
PANELS = {panel["title"]: panel for panel in DASHBOARD["panels"]}
KNOWN_RELATIONS = {
    "readings",
    "machines",
    "machine_status",
    "open_alerts",
    "alerts",
    "readings_hourly",
}


def _text(*parts: str) -> str:
    return (GRAFANA.joinpath(*parts)).read_text(encoding="utf-8")


def test_the_dashboard_refreshes_every_second_as_assumption_a18_states() -> None:
    assert DASHBOARD["refresh"] == "1s"


def test_the_compose_file_allows_the_refresh_interval_of_the_dashboard() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

    assert "GF_DASHBOARDS_MIN_REFRESH_INTERVAL: 1s" in compose


@pytest.mark.parametrize(
    ("title", "machine_type"),
    [("Moulds", MOULD), ("Ovens", OVEN), ("Grinding machines", GRINDER)],
)
def test_the_threshold_lines_of_a_panel_are_the_thresholds_of_its_machine_type(
    title: str, machine_type: MachineType
) -> None:
    steps = PANELS[title]["fieldConfig"]["defaults"]["thresholds"]["steps"]

    values = [step["value"] for step in steps if step["value"] is not None]
    assert values == [machine_type.warning_c, machine_type.critical_c]


@pytest.mark.parametrize(
    ("title", "machine_type"),
    [("Moulds", MOULD), ("Ovens", OVEN), ("Grinding machines", GRINDER)],
)
def test_the_panel_of_a_type_queries_the_machines_of_that_type(
    title: str, machine_type: MachineType
) -> None:
    (target,) = PANELS[title]["targets"]

    assert f"m.machine_type = '{machine_type.name}'" in target["rawSql"]


def test_a_machine_has_the_same_colour_in_every_time_series_panel() -> None:
    def colours(title: str) -> dict[str, str]:
        overrides = PANELS[title]["fieldConfig"]["overrides"]
        return {
            o["matcher"]["options"]: o["properties"][0]["value"]["fixedColor"]
            for o in overrides
        }

    assert colours("Moulds") == colours("Ovens") == colours("Grinding machines")
    assert len(set(colours("Moulds").values())) == 4


def test_every_panel_and_the_variable_use_the_provisioned_data_source() -> None:
    provisioned = _text("provisioning", "datasources", "timescaledb.yaml")
    uid = re.search(r"uid:\s*(\S+)", provisioned).group(1)

    uids = {panel["datasource"]["uid"] for panel in DASHBOARD["panels"]}
    uids.add(DASHBOARD["templating"]["list"][0]["datasource"]["uid"])
    assert uids == {uid}


def test_every_query_reads_only_the_tables_and_views_of_the_schema() -> None:
    queries = [t["rawSql"] for p in DASHBOARD["panels"] for t in p["targets"]]
    queries.append(DASHBOARD["templating"]["list"][0]["query"])

    relations = {
        match.lower()
        for query in queries
        # A call such as EXTRACT(EPOCH FROM now()) is not a relation.
        for match in re.findall(
            r"\b(?:FROM|JOIN)\s+([a-z_]+)\b(?!\()", query, flags=re.I
        )
    }
    assert relations <= KNOWN_RELATIONS
    assert {"machine_status", "open_alerts", "alerts", "readings_hourly"} <= relations


def test_the_status_table_writes_the_state_as_text_with_a_colour_per_state() -> None:
    overrides = PANELS["State of the machines"]["fieldConfig"]["overrides"]
    state = next(o for o in overrides if o["matcher"]["options"] == "State")

    mapping = next(p for p in state["properties"] if p["id"] == "mappings")["value"][0]
    options = mapping["options"]
    assert {name: v["text"] for name, v in options.items()} == {
        "normal": "normal",
        "warning": "warning",
        "critical": "critical",
    }
    assert len({v["color"] for v in options.values()}) == 3


def test_the_history_panel_reads_the_continuous_aggregate_of_the_machine() -> None:
    (target,) = PANELS["Hourly history of $machine"]["targets"]

    assert "FROM readings_hourly" in target["rawSql"]
    assert "machine_id = '$machine'" in target["rawSql"]


def test_the_contact_point_posts_to_the_port_and_path_of_the_notifier() -> None:
    contact_point = _text("provisioning", "alerting", "contact-points.yaml")

    url = re.search(r"url:\s*(\S+)", contact_point).group(1)
    assert url == f"http://host.docker.internal:{NotifierSettings().port}{NOTIFY_PATH}"


def test_the_alert_rule_selects_the_open_critical_alerts_with_a_machine_label() -> None:
    rules = _text("provisioning", "alerting", "alert-rules.yaml")

    assert "FROM open_alerts" in rules
    assert "WHERE severity = 'critical'" in rules
    assert "machine_id" in rules
    assert "interval: 10s" in rules
    assert "for: 0s" in rules


def test_the_notification_policy_routes_to_the_contact_point() -> None:
    policies = _text("provisioning", "alerting", "policies.yaml")
    contact_point = _text("provisioning", "alerting", "contact-points.yaml")

    receiver = re.search(r"receiver:\s*(\S+)", policies).group(1)
    assert f"name: {receiver}" in contact_point


@pytest.mark.parametrize(
    ("title", "machine_type"),
    [("Moulds", MOULD), ("Ovens", OVEN), ("Grinding machines", GRINDER)],
)
def test_the_axis_of_a_panel_reaches_both_threshold_lines(
    title: str, machine_type: MachineType
) -> None:
    custom = PANELS[title]["fieldConfig"]["defaults"]["custom"]

    assert custom["axisSoftMin"] < machine_type.normal_c
    assert custom["axisSoftMax"] > machine_type.critical_c
