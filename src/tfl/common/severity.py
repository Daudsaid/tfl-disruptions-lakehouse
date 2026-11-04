"""TfL statusSeverity codes: one place for what counts as good, planned or unplanned.

TfL's numeric codes are not ordered by how bad they are (16 "Not Running" > 10 "Good Service"),
so anything that needs "worst" uses severity_rank() instead of the raw code.
"""

from itertools import chain

from pyspark.sql import Column
from pyspark.sql import functions as F

GOOD = [10, 18, 19]  # Good Service, No Issues, Information
PLANNED = [4, 5, 11, 14, 16, 20]  # Planned Closure, Part Closure, Part Closed, Change of frequency, Not Running, Service Closed

# Worst first. Unplanned before planned before good, so the worst status always matches the line's state.
WORST_FIRST = [
    1, 2, 3, 6, 0, 7, 8, 15, 9, 17, 12, 13,  # unplanned: Closed, Suspended, Part Suspended, Severe Delays, Special Service, ...
    20, 16, 4, 5, 11, 14,  # planned: Service Closed, Not Running, Planned Closure, Part Closure, Part Closed, Change of frequency
    10, 18, 19,  # good
]
UNKNOWN_RANK = WORST_FIRST.index(20) - 0.5  # unknown codes count as unplanned, just ahead of the planned ones


def classify(severity: Column) -> Column:
    return F.when(severity.isin(GOOD), "good").when(severity.isin(PLANNED), "planned").otherwise("unplanned")


def severity_rank(severity: Column) -> Column:
    """0 = worst. Use for sorting and for picking a line's worst status."""
    ranks = F.create_map(*chain.from_iterable((F.lit(code), F.lit(i)) for i, code in enumerate(WORST_FIRST)))
    return F.coalesce(ranks[severity], F.lit(UNKNOWN_RANK))
