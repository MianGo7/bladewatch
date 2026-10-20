# M4, the demonstration scenario

- date: 2026-10-20
- commit: 60af417
- scenario: machine mould-01, seed 1, 100 seconds of normal operation, then 5 degrees Celsius per minute, 460 seconds per run, 3 repetitions

## The critical alert in every run, in seconds

| Run | measurement to broker | broker to processor | processor to database | measurement to alert in the database | measurement to alert on the dashboard query | measurement to notification | measurement to firing state in Grafana | firing state to notification | Note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.014 | 0.116 | 0.004 | 0.134 | 0.202 | 27.161 | 7.135 | 20.026 |  |
| 2 | 0.019 | 0.082 | 0.005 | 0.106 | 0.153 | 26.768 | 6.751 | 20.017 |  |
| 3 | 0.016 | 0.131 | 0.003 | 0.151 | 0.185 | 26.305 | 6.284 | 20.020 |  |

## Over the runs, in seconds

| Statistic | measurement to broker | broker to processor | processor to database | measurement to alert in the database | measurement to alert on the dashboard query | measurement to notification | measurement to firing state in Grafana | firing state to notification |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| median | 0.016 | 0.116 | 0.004 | 0.134 | 0.185 | 26.768 | 6.751 | 20.020 |
| 95th percentile | 0.019 | 0.130 | 0.005 | 0.149 | 0.200 | 27.121 | 7.096 | 20.025 |
| maximum | 0.019 | 0.131 | 0.005 | 0.151 | 0.202 | 27.161 | 7.135 | 20.026 |

The statistics are taken over 3 runs, one critical alert each.

## Every reading of every undisturbed run, in seconds

| Run | Stage | Median | 95th percentile | Maximum |
| --- | --- | --- | --- | --- |
| 1 | measured to broker | 0.013 | 0.019 | 0.507 |
| 1 | broker to processor | 0.110 | 0.201 | 0.217 |
| 1 | processor to stored | 0.002 | 0.003 | 0.009 |
| 1 | measured to stored | 0.126 | 0.218 | 0.547 |
| 2 | measured to broker | 0.013 | 0.018 | 0.515 |
| 2 | broker to processor | 0.109 | 0.203 | 0.218 |
| 2 | processor to stored | 0.002 | 0.004 | 0.006 |
| 2 | measured to stored | 0.128 | 0.221 | 0.540 |
| 3 | measured to broker | 0.014 | 0.018 | 0.506 |
| 3 | broker to processor | 0.110 | 0.202 | 0.217 |
| 3 | processor to stored | 0.002 | 0.004 | 0.007 |
| 3 | measured to stored | 0.126 | 0.218 | 0.547 |

## Repeatability

The alert events of the runs are identical in kind, severity, status, temperature, limit, and time from the start.

| Kind | Severity | Status | Temperature in degrees Celsius | Limit | Seconds from the start |
| --- | --- | --- | --- | --- | --- |
| rate_of_rise | warning | raised | 72.33 | 3 | 134 |
| threshold | critical | raised | 95.25 | 95 | 396 |
