"""Runs of the measurements M1 to M3 against the running stack.

The package starts the producer and the processor, samples the consumer lag, reads
the stored rows, and writes the raw result of a measurement as a JSON file
(ADR-0024). The statistics, the tables, and the figures are computed by
`bladewatch.measure`. The demonstration scenario and its measurement M4 live in
`bladewatch.scenario`, which M1 reuses.
"""
