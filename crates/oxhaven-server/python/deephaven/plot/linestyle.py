"""The style of a line: its width, its ends and joins, and its dashes.

A client is told nothing of the style of a line, by this server or by Java
(`FigureWidgetTranslator` leaves it out), so a `LineStyle` is kept and changes no plot.

Behavior: `io.deephaven.plot.LineStyle`
"""

from enum import Enum
from numbers import Number
from typing import List, Optional


class LineEndStyle(Enum):
    """How a line ends."""

    BUTT = "BUTT"
    ROUND = "ROUND"
    SQUARE = "SQUARE"


class LineJoinStyle(Enum):
    """How two segments of a line meet."""

    BEVEL = "BEVEL"
    MITER = "MITER"
    ROUND = "ROUND"


class LineStyle:
    """The width of a line, its end and join styles, and the lengths of its dashes."""

    def __init__(
        self,
        width: float = 1.0,
        end_style: LineEndStyle = LineEndStyle.ROUND,
        join_style: LineJoinStyle = LineJoinStyle.ROUND,
        dash_pattern: Optional[List[Number]] = None,
    ):
        self.width = width
        self.end_style = end_style
        self.join_style = join_style
        self.dash_pattern = dash_pattern
