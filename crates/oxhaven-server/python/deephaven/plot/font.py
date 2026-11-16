"""Fonts for the titles and labels of a figure.

A client is told the family of a font and nothing else, as by Java
(`FigureWidgetTranslator#toCssFont`).

Behavior: `io.deephaven.plot.Font`
"""

from enum import Enum


def font_family_names() -> list:
    """The font families of the server. Java answers with the fonts of its graphics
    environment; this server draws nothing and has none."""
    raise NotImplementedError("not ported: io.deephaven.plot.Font#availableFontFamilies")


class FontStyle(Enum):
    """The style of a font."""

    PLAIN = "PLAIN"
    BOLD = "BOLD"
    ITALIC = "ITALIC"
    BOLD_ITALIC = "BOLD_ITALIC"


class Font:
    """A font: a family, a style and a size."""

    def __init__(
        self,
        family: str = "Arial",
        style: FontStyle = FontStyle.PLAIN,
        size: int = 8,
    ):
        self.family = family
        self.style = style
        self.size = size
