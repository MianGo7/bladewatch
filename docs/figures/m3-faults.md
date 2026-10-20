# M3, fault tolerance

- date: 2026-10-20
- commit: 5a63d01
- hardware: Mac17,8 with Apple M5 Pro, 48 GiB of memory, macOS 27.0.1
- Docker: Docker Engine 29.8.2 with the limits of 18 CPUs and 7.746 GiB of memory
- load profile: 2000 readings per second, 30 seconds of load, 30 seconds without the processor, 60 seconds of load after it started again, 3 runs of each kind of stop, seed 1

## Every run, readings and seconds

| Stop | Run | Sent | Stored | Missing | Stored twice | Received again | Backlog at the restart | Seconds to the first commit | Seconds to catch up | Seconds to drain |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| killed | 1 | 240 000 | 240 000 | 0 | 0 | 0 | 60 000 | 14.973 | 17.987 | 0.514 |
| finished | 1 | 240 000 | 240 000 | 0 | 0 | 0 | 60 000 | 2.796 | 3.797 | 0.002 |
| killed | 2 | 240 000 | 240 000 | 0 | 0 | 0 | 60 000 | 14.964 | 17.973 | 0.515 |
| finished | 2 | 240 000 | 240 000 | 0 | 0 | 0 | 60 000 | 2.691 | 4.694 | 0.002 |
| killed | 3 | 240 000 | 240 000 | 0 | 0 | 0 | 60 000 | 14.952 | 17.966 | 0.513 |
| finished | 3 | 240 000 | 240 000 | 0 | 0 | 0 | 60 000 | 2.738 | 3.741 | 0.513 |
