# M4, the demonstration scenario

- date: 2026-10-20
- commit: 58b71f1, with uncommitted changes
- scenario: machine mould-01, seed 1, 100 seconds of normal operation, then 5 degrees Celsius per minute, 460 seconds per run, 3 repetitions

## The critical alert in every run, in seconds

| Run | measurement to broker | broker to processor | processor to database | measurement to alert in the database | measurement to alert on the dashboard query | measurement to notification | Note |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.016 | 0.168 | 0.002 | 0.186 | 0.284 | 28.677 |  |
| 2 | 0.016 | 0.192 | 0.005 | 0.213 | 0.247 | 28.238 |  |
| 3 | 0.012 | 0.190 | 0.012 | 0.214 | 0.251 | 27.734 |  |

## Over the runs, in seconds

| Statistic | measurement to broker | broker to processor | processor to database | measurement to alert in the database | measurement to alert on the dashboard query | measurement to notification |
| --- | --- | --- | --- | --- | --- | --- |
| median | 0.016 | 0.190 | 0.005 | 0.213 | 0.251 | 28.238 |
| 95th percentile | 0.016 | 0.192 | 0.011 | 0.214 | 0.281 | 28.633 |
| maximum | 0.016 | 0.192 | 0.012 | 0.214 | 0.284 | 28.677 |

The statistics are taken over 3 runs, one critical alert each.

## Every reading of every undisturbed run, in seconds

| Run | Stage | Median | 95th percentile | Maximum |
| --- | --- | --- | --- | --- |
| 1 | measured to broker | 0.015 | 0.019 | 0.516 |
| 1 | broker to processor | 0.108 | 0.205 | 0.219 |
| 1 | processor to stored | 0.002 | 0.004 | 0.009 |
| 1 | measured to stored | 0.127 | 0.222 | 0.537 |
| 2 | measured to broker | 0.012 | 0.018 | 0.510 |
| 2 | broker to processor | 0.110 | 0.204 | 0.217 |
| 2 | processor to stored | 0.002 | 0.003 | 0.005 |
| 2 | measured to stored | 0.124 | 0.219 | 0.552 |
| 3 | measured to broker | 0.012 | 0.017 | 0.514 |
| 3 | broker to processor | 0.106 | 0.199 | 0.218 |
| 3 | processor to stored | 0.002 | 0.003 | 0.011 |
| 3 | measured to stored | 0.121 | 0.213 | 0.567 |

## Repeatability

The alert events of the runs are identical in kind, severity, status, temperature, limit, and time from the start.

| Kind | Severity | Status | Temperature in degrees Celsius | Limit | Seconds from the start |
| --- | --- | --- | --- | --- | --- |
| rate_of_rise | warning | raised | 72.33 | 3 | 134 |
| threshold | critical | raised | 95.25 | 95 | 396 |
