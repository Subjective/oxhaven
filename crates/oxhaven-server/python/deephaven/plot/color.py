"""Colors for the lines, points, titles and axes of a figure.

Behavior: `io.deephaven.gui.color.Color`
"""

import colorsys

from deephaven.dherror import DHError

from ._named_colors import NAMED


class Color:
    """A color: red, green, blue and alpha, each from 0 to 255."""

    def __init__(self, r: int, g: int, b: int, alpha: int = 255):
        for part in (r, g, b, alpha):
            if not 0 <= part <= 255:
                raise ValueError(f"a color has parts from 0 to 255, not {part}")
        self._rgba = (int(r), int(g), int(b), int(alpha))

    def to_hex(self) -> str:
        """The color as `#RRGGBB`, in upper case; the alpha is left out."""
        return "#%02X%02X%02X" % self._rgba[:3]

    def _css(self) -> str:
        """The color as a client is told it: `#rrggbb`, in lower case.

        Behavior: `FigureWidgetTranslator#toCssColorString`
        """
        return "#%02x%02x%02x" % self._rgba[:3]

    def __eq__(self, other) -> bool:
        return isinstance(other, Color) and self._rgba == other._rgba

    def __hash__(self) -> int:
        return hash(self._rgba)

    def __repr__(self) -> str:
        return f"Color{self._rgba}"

    @staticmethod
    def of_name(name: str) -> "Color":
        """The color of a name, in any case, or of `#RRGGBB`."""
        try:
            return _parse(name)
        except Exception as e:
            raise DHError(e, "failed to get a color by its name.") from e

    @staticmethod
    def of_rgb(r: int, g: int, b: int, alpha: int = 255) -> "Color":
        """The color of red, green, blue and alpha values from 0 to 255."""
        try:
            return Color(r, g, b, alpha)
        except Exception as e:
            raise DHError(e, "failed to create a color from rgb values.") from e

    @staticmethod
    def of_rgb_f(r: float, g: float, b: float, alpha: float = 1.0) -> "Color":
        """The color of red, green, blue and alpha values from 0.0 to 1.0."""
        try:
            # Behavior: `java.awt.Color(float, float, float, float)`, which rounds half up
            return Color(*(int(part * 255 + 0.5) for part in (r, g, b, alpha)))
        except Exception as e:
            raise DHError(e, "failed to create a color from rgb values.") from e

    @staticmethod
    def of_hsl(h: float, s: float, l: float, alpha: float = 1.0) -> "Color":
        """The color of a hue in degrees, and a saturation, a lightness (both from 0 to
        100) and an alpha from 0.0 to 1.0."""
        try:
            if not 0 <= s <= 100:
                raise ValueError(f"a saturation is from 0 to 100, not {s}")
            if not 0 <= l <= 100:
                raise ValueError(f"a lightness is from 0 to 100, not {l}")
            if not 0 <= alpha <= 1:
                raise ValueError(f"an alpha is from 0.0 to 1.0, not {alpha}")
            r, g, b = colorsys.hls_to_rgb((h % 360) / 360, l / 100, s / 100)
            return Color(*(int(part * 255 + 0.5) for part in (r, g, b, alpha)))
        except Exception as e:
            raise DHError(e, "failed to create a color from hsl values.") from e


def _parse(text: str) -> Color:
    """Behavior: `io.deephaven.gui.color.Color(String)`"""
    if text is None:
        raise ValueError("java.lang.IllegalArgumentException: Color can not be null")
    if "#" in text:
        # Behavior: `java.awt.Color#decode`, for the form colors are written in
        try:
            value = int(text.strip().lstrip("#"), 16)
        except ValueError:
            digits = text.strip().lstrip("#")
            raise ValueError(f'java.lang.NumberFormatException: For input string: "{digits}" under radix 16') from None
        return Color((value >> 16) & 0xFF, (value >> 8) & 0xFF, value & 0xFF)
    digits = NAMED.get(text.upper())
    if digits is None:
        raise ValueError(f"java.lang.IllegalArgumentException: Invalid color name: {text}")
    return Color(int(digits[0:2], 16), int(digits[2:4], 16), int(digits[4:6], 16))


# What `io.deephaven.plot.util.PlotUtils#intToColor` counts through: the colors of
# Matplotlib, in the order of Java's palette.
_PALETTE = ("d62728", "1f77b4", "2ca02c", "ff7f0e", "9467bd", "8c564b", "17becf", "e377c2", "7f7f7f", "bcbd22")


def _css_of(color) -> str:
    """What a client is told for a color given as a `Color`, a name, or a place in the
    palette."""
    if isinstance(color, Color):
        return color._css()
    if isinstance(color, bool):
        raise TypeError(f"not a color: {color!r}")
    if isinstance(color, int):
        if color < 0:
            raise ValueError(f"no color for the negative number {color}")
        return "#" + _PALETTE[color % len(_PALETTE)]
    if isinstance(color, str):
        return _parse(color)._css()
    raise TypeError(f"not a color: {color!r}")


class Colors:
    """The colors that have names, each as an attribute."""


for _name, _digits in NAMED.items():
    setattr(Colors, _name, Color(int(_digits[0:2], 16), int(_digits[2:4], 16), int(_digits[4:6], 16)))
del _name, _digits
