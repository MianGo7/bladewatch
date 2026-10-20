# Figures

The result figures, tables, and dashboard screenshots that the case study refers
to. A screenshot is named after what it shows and the scenario that produced it,
and it is saved as PNG.

## Dashboard in normal operation

Scenario: the stack is running, the processor is started with
`uv run python -m bladewatch process`, and the producer with
`uv run python -m bladewatch produce --seed 1 --duration-s 1200`. The screenshots are
taken after the producer has run for more than ten minutes, so that every panel holds
the full time range of ten minutes without a gap. All twelve machines are in the state
normal and no alert is open.

The base address is
`http://localhost:3000/d/bladewatch-overview/bladewatch-machine-monitoring`. The
parameters `theme=light` and `kiosk` hide the navigation and fix the theme, and
`_dash.hideTimePicker` and `_dash.hideVariables` hide the control bar, so that the
first screen of the dashboard fits into one window.

Display and capture: the display has a pixel ratio of 2. The page is loaded, its
timers are cancelled so that every part of the picture shows the same instant, the
footer bar of Grafana is hidden, and the window is captured in four parts at the full
device resolution, which are joined into one PNG without scaling. The overview is
captured at a window of 1100 by 941 pixels and the single panels at a width of 900
pixels, so that the text stays legible when the picture is printed at the width of
the text of the case study.

| File | View | Address after the base address | Size in pixels | Status |
| --- | --- | --- | --- | --- |
| `dashboard-overview-normal-operation.png` | The first screen of the dashboard: state of the twelve machines, open alerts, and the temperature of the three machine types | `?orgId=1&from=now-10m&to=now&var-machine=mould-01&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 2200 by 1846 | done |
| `dashboard-state-table-normal-operation.png` | The table with the state of the twelve machines | `?orgId=1&from=now-10m&to=now&viewPanel=panel-1&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 1800 by 1090 | done |
| `dashboard-moulds-temperature-normal-operation.png` | The temperature of the four moulds with the threshold lines | `?orgId=1&from=now-10m&to=now&viewPanel=panel-2&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 1800 by 1060 | done |
| `dashboard-hourly-history-mould-01-normal-operation.png` | The hourly history of one mould, which the panel reads over its own range of 24 hours | `?orgId=1&var-machine=mould-01&viewPanel=panel-7&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 1800 by 940 | done |

The screenshots of the dashboard at the moment of the critical alert and of the
history afterwards, and the terminal output of the received notification, belong to
the demonstration scenario of B7 and are listed below.

The history panel holds one point per machine and hour, so after twenty minutes of data
it shows two points per series. The picture documents that the panel works and is of
little use as a result figure until hours of data exist. The pictures were taken on
2026-10-20 with about 20 minutes of data from the producer with the seed 1, and the
Grafana version 13.2.4.

## Demonstration scenario

Scenario: `uv run python -m bladewatch scenario --reset`, the third run of the measurement M4 on the
commit 60af417 with the seed 1. The machine `mould-01` runs normally for 100 seconds and then
rises at 5 degrees Celsius per minute, the rate warning is raised 134 seconds after the start at
72.3 degrees Celsius, and the critical alert 396 seconds after the start at 95.3 degrees Celsius.
The pictures were taken on 2026-10-20 with Grafana 13.2.4. The overview was captured 36 seconds
after the critical reading, with the critical alert 0.151 seconds after its measurement in the
database, at a page width of 1100 pixels. The chart of the moulds was captured at the same width
and the table of alert events at 900 pixels, both after the end of the run with the range of the
last 9 minutes.

| File | View | Address after the base address | Size in pixels |
| --- | --- | --- | --- |
| `dashboard-overview-critical-alert-scenario.png` | The first screen at the moment of the critical alert: `mould-01` is critical in the state table, the alert is in the table of open alerts, and the line of the mould has crossed the critical threshold | `?orgId=1&from=now-10m&to=now&var-machine=mould-01&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 2200 by 1848 |
| `dashboard-moulds-temperature-incident-scenario.png` | The course of the incident: four moulds, one of them rising from 70 degrees Celsius through the warning threshold of 80 and the critical threshold of 95 | `?orgId=1&from=now-9m&to=now&viewPanel=panel-2&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 2200 by 1868 |
| `dashboard-alert-events-incident-scenario.png` | The two alert events of the incident, the rate of rise warning and the critical alert, with their times in UTC | `?orgId=1&from=now-9m&to=now&viewPanel=panel-6&theme=light&kiosk&_dash.hideTimePicker&_dash.hideVariables` | 1800 by 560 |
| `scenario-notification-output.txt` | The terminal output of the scenario with the notification of the contact point as the receiver logged it, cut to the lines of the start, the notification, and the end of the production | not a screenshot | text |
| `m4-demonstration-scenario.md` | The results of the measurement M4 over three undisturbed runs, with the commit, the date, and the scenario | not a screenshot | table |

The hourly history of a machine for the scenario is not part of this set. It is retaken
after the runs of B8, when the database holds hours of data, and before the next
`scenario --reset`, which deletes the data of the scenario machines.
