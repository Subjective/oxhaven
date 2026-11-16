"""Plotting: figures that a client draws from tables of the server.

A `Figure` is built by calling its methods, each of which returns a new figure, and becomes
something a client can open when `show()` is called on it.

The names and signatures are those of the reference's `deephaven.plot`. What is behind
them is `docs/FIGURES.md`.
"""

from .axisformat import AxisFormat, DecimalAxisFormat, NanosAxisFormat
from .axistransform import AxisTransform, axis_transform, axis_transform_names
from .color import Color, Colors
from .font import Font, FontStyle, font_family_names
from .linestyle import LineEndStyle, LineJoinStyle, LineStyle
from .plotstyle import PlotStyle
from .selectable_dataset import SelectableDataSet
from .shape import Shape

from .figure import Figure

__all__ = [
    "AxisFormat",
    "DecimalAxisFormat",
    "NanosAxisFormat",
    "AxisTransform",
    "axis_transform_names",
    "axis_transform",
    "Color",
    "Colors",
    "Font",
    "FontStyle",
    "font_family_names",
    "LineStyle",
    "LineEndStyle",
    "LineJoinStyle",
    "PlotStyle",
    "SelectableDataSet",
    "Shape",
    "Figure",
]
