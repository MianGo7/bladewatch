"""Statistics, tables, and figures of the measurements M1 to M3.

The package holds what is computed from the raw result of a measurement: the
distributions and the criteria, the Markdown tables, and the figures (ADR-0024). It
imports only `config` and the numerical libraries, and it is the only package that
imports Matplotlib. The runs themselves are started by `bladewatch.bench`, and the
demonstration scenario with its measurement M4 lives in `bladewatch.scenario`.
"""


class MeasureError(RuntimeError):
    """A measurement cannot start or its result cannot be rendered."""
