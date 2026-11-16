"""How the values along an axis are written.

Behavior: `io.deephaven.plot.axisformatters.AxisFormat`
"""

from typing import Optional


class AxisFormat:
    """A format of the tick labels of an axis, with an optional pattern."""

    def __init__(self, j_axis_format=None):
        self.pattern: Optional[str] = None

    def set_pattern(self, pattern: str) -> None:
        """Sets the pattern the values are written with."""
        self.pattern = pattern


class DecimalAxisFormat(AxisFormat):
    """A format for numbers, with a pattern of `java.text.DecimalFormat`."""

    def __init__(self):
        super().__init__()


class NanosAxisFormat(AxisFormat):
    """A format for times held as nanoseconds since the epoch, with a pattern of
    `java.time.format.DateTimeFormatter`."""

    def __init__(self, tz=None):
        super().__init__()
        self.tz = tz
