# M1, latency under the load of the case

- date: 2026-10-20
- commit: 5a63d01
- hardware: Mac17,8 with Apple M5 Pro, 48 GiB of memory, macOS 27.0.1
- Docker: Docker Engine 29.8.2 with the limits of 18 CPUs and 7.746 GiB of memory
- load profile: 12 machines with one reading per second, which is 12 readings per second, the incident of the demonstration scenario, 3 runs of 600 seconds, seed 1

## Every reading of every run, in seconds

| Stage | Readings | Median | 95th percentile | 99th percentile | Maximum |
| --- | --- | --- | --- | --- | --- |
| measured to broker | 21 600 | 0.013 | 0.019 | 0.020 | 0.514 |
| broker to processor | 21 600 | 0.110 | 0.202 | 0.211 | 0.220 |
| processor to stored | 21 600 | 0.003 | 0.004 | 0.005 | 0.031 |
| measured to stored | 21 600 | 0.126 | 0.218 | 0.228 | 0.557 |

## Every raised alert event, in seconds

| Stage | Events | Median | 95th percentile | Maximum |
| --- | --- | --- | --- | --- |
| measurement to alert in the database | 6 | 0.111 | 0.219 | 0.229 |

## Measurement to storage in each run, in seconds

| Run | Readings | Median | 95th percentile | 99th percentile | Maximum |
| --- | --- | --- | --- | --- | --- |
| 1 | 7 200 | 0.127 | 0.219 | 0.228 | 0.538 |
| 2 | 7 200 | 0.124 | 0.218 | 0.229 | 0.557 |
| 3 | 7 200 | 0.126 | 0.218 | 0.227 | 0.550 |
