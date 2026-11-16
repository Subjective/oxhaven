"""The shapes a point of a series can be drawn as.

Behavior: `io.deephaven.gui.shape.NamedShape`
"""

from enum import Enum


class Shape(Enum):
    """A shape. The value is the name the protocol knows the shape by."""

    SQUARE = "SQUARE"
    CIRCLE = "CIRCLE"
    UP_TRIANGLE = "UP_TRIANGLE"
    DIAMOND = "DIAMOND"
    HORIZONTAL_RECTANGLE = "HORIZONTAL_RECTANGLE"
    ELLIPSE = "ELLIPSE"
    RIGHT_TRIANGLE = "RIGHT_TRIANGLE"
    DOWN_TRIANGLE = "DOWN_TRIANGLE"
    VERTICAL_RECTANGLE = "VERTICAL_RECTANGLE"
    LEFT_TRIANGLE = "LEFT_TRIANGLE"
