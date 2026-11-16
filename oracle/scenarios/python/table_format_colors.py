# Colours that are styles: the fields of `ColorUtil` (`long`s, `PINK`, `ORANGE` and
# `GREEN` those of the `DB_` colours), and `colorRGB(int, boolean)`, whose alpha no
# style reads, as values and in formats; and a colour `ColorUtil` does not have. A colour
# as the value of a column, a column of `Color` objects in Java, is refused here and has
# no step (`tasks/7.8.4-tree-formats.md`).
# oracle: results styles, formatted
from deephaven import empty_table

util = "io.deephaven.engine.util.ColorUtil"
source = empty_table(4).update(["X = i", "V = i * 1.5"])
styles = source.update([
    f"Red = {util}.RED",
    f"Pink = {util}.PINK",
    f"Green = {util}.GREEN",
    f"Pale = {util}.PALE_BLUEGREEN",
    f"None_ = {util}.NO_FORMATTING",
    f"Bit = {util}.FOREGROUND_SET_BIT + {util}Impl.COLOR_SET_BIT",
    "Alpha = bg(colorRGB(0x7F102030, true))",
    "Opaque = bgfg(colorRGB(0x102030, false), colorRGB(X * 1000, true))",
])
formatted = source.format_columns([
    f"X = X > 1 ? {util}.ORANGE : {util}.NO_FORMATTING",
    "V = bg(colorRGB(0x7FFF0000, true))",
])
# oracle: step
# oracle: fails
colours = source.update(f"C = {util}.DB_PINK")
