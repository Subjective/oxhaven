"""How the series of one set of axes are drawn.

Behavior: `io.deephaven.plot.PlotStyle`
"""

from enum import Enum


class PlotStyle(Enum):
    """A way to draw series. The value is the name the protocol knows the style by."""

    BAR = "BAR"
    STACKED_BAR = "STACKED_BAR"
    LINE = "LINE"
    AREA = "AREA"
    STACKED_AREA = "STACKED_AREA"
    PIE = "PIE"
    HISTOGRAM = "HISTOGRAM"
    OHLC = "OHLC"
    SCATTER = "SCATTER"
    STEP = "STEP"
    ERROR_BAR = "ERROR_BAR"
