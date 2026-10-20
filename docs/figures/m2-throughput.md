# M2, throughput

- date: 2026-10-20
- commit: 5a63d01
- hardware: Mac17,8 with Apple M5 Pro, 48 GiB of memory, macOS 27.0.1
- Docker: Docker Engine 29.8.2 with the limits of 18 CPUs and 7.746 GiB of memory
- load profile: steps of 12, 100, 500, 2000, 5000, 10000, 25000, 50000, 100000 readings per second, each one stream per reading and per second, 120 seconds per step, 3 runs of the ramp, seed 1

## Every step, over the runs of the ramp

| Offered per second | Runs | Delivered per second | Stored per second | 95th percentile of measurement to storage in seconds | Largest lag in messages | Seconds to drain | Share of the largest partition | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 12 | 3 | 12 | 12 | 0.213 (largest 0.218) | 12 | 0.004 | 33.3 percent | passed |
| 100 | 3 | 100 | 101 | 0.222 (largest 0.223) | 100 | 0.004 | 23.0 percent | passed |
| 500 | 3 | 500 | 498 | 0.052 (largest 0.052) | 500 | 0.003 | 17.6 percent | passed |
| 2 000 | 3 | 2 000 | 2 003 | 0.116 (largest 0.117) | 2 000 | 0.506 | 17.9 percent | passed |
| 5 000 | 3 | 5 000 | 5 029 | 0.216 (largest 0.220) | 5 000 | 0.515 | 17.4 percent | lag growing; passed |
| 10 000 | 2 | 10 000 | 10 068 | 0.389 (largest 0.390) | 9 500 | 0.508 | 17.1 percent | passed |
| 25 000 | 2 | 25 000 | 24 880 | 0.924 (largest 0.944) | 32 000 | 1.510 | 17.0 percent | passed |
| 50 000 | 2 | 50 000 | 25 136 | 65.605 (largest 66.474) | 1 739 500 | 73.072 | 16.9 percent | latency target missed, lag growing |

## Stage times over the step of 50 000 readings per second, run 3, in seconds

The readings are grouped by the time of their measurement, in stretches of 10 seconds from the first one.

| Stretch from | Readings | Broker to processor, median | Broker to processor, 95th percentile | Processor to storage, median | Processor to storage, 95th percentile |
| --- | --- | --- | --- | --- | --- |
| 0 | 500 000 | 0.916 | 1.442 | 0.006 | 0.007 |
| 10 | 500 000 | 1.565 | 2.174 | 0.006 | 0.008 |
| 20 | 500 000 | 2.380 | 3.257 | 0.007 | 0.008 |
| 30 | 500 000 | 3.449 | 4.398 | 0.007 | 0.008 |
| 40 | 500 000 | 4.539 | 5.533 | 0.007 | 0.008 |
| 50 | 500 000 | 5.892 | 7.184 | 0.007 | 0.008 |
| 60 | 500 000 | 11.816 | 17.044 | 0.011 | 0.013 |
| 70 | 500 000 | 21.946 | 27.636 | 0.011 | 0.013 |
| 80 | 500 000 | 32.596 | 38.831 | 0.011 | 0.014 |
| 90 | 500 000 | 43.704 | 49.759 | 0.011 | 0.013 |
| 100 | 500 000 | 54.501 | 61.071 | 0.011 | 0.014 |
| 110 | 500 000 | 65.732 | 71.428 | 0.012 | 0.014 |

## Where each ramp stopped

- run 1: the first failing step offered 5 000 readings per second
- run 2: the first failing step offered 50 000 readings per second
- run 3: the first failing step offered 50 000 readings per second
