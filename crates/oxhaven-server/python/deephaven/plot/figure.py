"""Figures: charts of the columns of tables, which a client draws.

A `Figure` keeps what a script plots: charts in a grid, the axes of each chart and the
series of each pair of axes. Every method returns a new figure and leaves its own
unchanged. `show()` turns the figure into the description a client fetches, a
`FigureDescriptor` of the console protocol, and the tables the description names.

The reference package generates its `figure.py` from the Java `Figure` interface, and each
of its methods chooses Java calls by the parameters that are set. This module is written by
hand: the parameters, their accepted types and the Java call a combination of parameters
stands for are the reference's, each method lists them in a table (`_rules`), and what a
Java call does is written here from what Java 42.9 was seen to do (`docs/FIGURES.md`).

Behavior: `io.deephaven.plot.FigureImpl`, `io.deephaven.figure.FigureWidgetTranslator`
"""

from __future__ import annotations

import copy
import json
import math
from collections.abc import Callable as _Callable
from typing import Any, Callable, Dict, List, Union

from deephaven import _engine, agg
from deephaven._java_time import JavaError, NotPorted
from deephaven.dherror import DHError
from deephaven.dtypes import BusinessCalendar, Instant
from deephaven.table import PartitionedTable, Table

from .axisformat import AxisFormat
from .axistransform import AxisTransform
from .color import Color, _css_of
from .font import Font
from .linestyle import LineStyle
from .plotstyle import PlotStyle
from .selectable_dataset import SelectableDataSet
from .shape import Shape

_PLOT = "io.deephaven.plot.errors."

# The names of the accepted types, as the reference's errors give them.
_TYPE_NAMES = {list: "List", dict: "Dict", _Callable: "Callable"}

_COLOR = (str, int, Color)
_TABLE = (Table, SelectableDataSet)
_DATA = (str, list)
_AXIS_TYPES = {
    "t": _TABLE,
    "label": (str,),
    "color": _COLOR,
    "font": (Font,),
    "format": (AxisFormat,),
    "format_pattern": (str,),
    "min": (str, float),
    "max": (str, float),
    "invert": (bool,),
    "log": (bool,),
    "business_time": (bool,),
    "calendar": (str, type(BusinessCalendar)),
    "transform": (AxisTransform,),
}
_TICK_TYPES = {"font": (Font,), "gap": (float,), "loc": (list,), "angle": (int,), "visible": (int,)}
_MINOR_TYPES = {"nminor": (int,), "visible": (int,)}

# The column types Java draws along a numeric axis.
# Behavior: `io.deephaven.plot.datasets.data.IndexableNumericDataTable`, seen to refuse
# `byte` and `char`
_XY_TYPES = {"short", "int", "long", "float", "double", "java.time.Instant"}
_HISTOGRAM_TYPES = {"byte", "short", "int", "long", "float", "double"}

_SHAPES = [
    "SQUARE",
    "CIRCLE",
    "UP_TRIANGLE",
    "DIAMOND",
    "HORIZONTAL_RECTANGLE",
    "ELLIPSE",
    "RIGHT_TRIANGLE",
    "DOWN_TRIANGLE",
    "VERTICAL_RECTANGLE",
    "LEFT_TRIANGLE",
]

# The style of the series of axes that were given none.
# Behavior: `io.deephaven.plot.AxesImpl#getPlotStyle`
_DEFAULT_STYLE = {"XY": "LINE", "CATEGORY": "BAR", "PIE": "PIE", "TREEMAP": "TREEMAP"}

_CAT_ORDER = "__CAT_ORDER"


def _typed(name: str, value: Any, types: tuple) -> None:
    """Refuses a value that is not of one of `types`, as the reference does."""
    if value is None or isinstance(value, types):
        return
    supported = [_TYPE_NAMES.get(t, t.__name__) for t in types]
    raise DHError(message=f"Improper input type: name={name} type={type(value)} supported={supported}")


def _set_of(args: dict, types: dict) -> set:
    """The names of the arguments that are set, after their types are checked."""
    for name, value in args.items():
        _typed(name, value, types[name])
    return {name for name, value in args.items() if value is not None}


def _info(figure_title, chart_title=None, series=None) -> str:
    """Where in a figure an error happened, as Java's plot exceptions start.

    Behavior: `io.deephaven.plot.errors.PlotInfo#encodeInfo`
    """
    parts = []
    if figure_title is not None:
        parts.append(f"Figure: {figure_title}.")
    if chart_title is not None:
        parts.append(f"Chart: {chart_title}.")
    if series is not None:
        parts.append(f"Series: {series}.")
    return f"Plot Information: {' '.join(parts)} " if parts else ""


def _java_hash(text: str) -> int:
    """Behavior: `java.lang.String#hashCode`, spread as `java.util.HashMap#hash` does"""
    value = 0
    for char in text:
        value = (value * 31 + ord(char)) & 0xFFFFFFFF
    return value ^ (value >> 16)


def _hash_map_order(keys: list) -> list:
    """`keys`, which were put in a `java.util.HashMap` in this order, in the order the map
    gives them back.

    Behavior: `java.util.HashMap`, whose buckets keep the order of insertion while a bucket
    holds fewer than eight keys
    """
    capacity = 16
    while len(keys) > capacity * 3 // 4:
        capacity *= 2
    return sorted(keys, key=lambda key: _java_hash(key) & (capacity - 1))


class _Axis:
    """One axis of a chart.

    Behavior: `io.deephaven.plot.AxisImpl`
    """

    def __init__(self, dim: int, index: int):
        self.dim = dim
        self.index = index
        self.type = None
        self.log = False
        self.label = None
        self.label_font = None
        self.format_pattern = None
        self.color = None
        self.min = math.nan
        self.max = math.nan
        self.minor_ticks_visible = False
        self.major_ticks_visible = True
        self.minor_tick_count = 0
        self.gap = -1.0
        self.tick_locations = None
        self.tick_label_angle = 0.0
        self.invert = False

    @property
    def id(self) -> str:
        return "XY"[self.dim] + str(self.index)


class _Series:
    """One series, or the series of every constituent of a partitioned table.

    `sources` are `(source type, dimension or None, column)`; `table` is the place of what
    the columns are read from in the figure's list.
    """

    def __init__(self, name: str, kind: str, multi: bool, table: int, sources: list):
        self.name = name
        self.kind = kind
        self.multi = multi
        self.table = table
        self.sources = sources
        self.lines_visible = None
        self.points_visible = None
        self.gradient_visible = None
        self.line_color = None
        self.point_color = None
        self.point_color_text = None
        self.point_label_format = None
        self.x_tool_tip_pattern = None
        self.y_tool_tip_pattern = None
        self.point_label = None
        self.point_size = None
        self.point_shape = None


class _Axes:
    """Axes: one axis for each dimension, and the series drawn against them.

    Behavior: `io.deephaven.plot.AxesImpl`
    """

    def __init__(self, name: str, axis: list):
        self.name = name
        self.axis = axis
        self.dimension = -1
        self.plot_style = None
        self.series: List[_Series] = []


class _Chart:
    """Behavior: `io.deephaven.plot.ChartImpl`"""

    def __init__(self, row: int, column: int):
        self.row = row
        self.column = column
        self.rowspan = 1
        self.colspan = 1
        self.type = None
        self.title = None
        self.title_font = None
        self.title_color = None
        self.show_legend = True
        self.legend_font = None
        self.legend_color = None
        self.initialized = False
        self.axis: List[List[_Axis]] = [[], []]
        self.axes: List[_Axes] = []


class _Model:
    """Behavior: `io.deephaven.plot.BaseFigureImpl`, `io.deephaven.plot.ChartArray`"""

    def __init__(self, rows: int, cols: int):
        if rows < 1 or cols < 1:
            raise JavaError(f"{_PLOT}PlotIllegalArgumentException: Grid must be at least 1x1")
        self.rows = rows
        self.cols = cols
        self.title = None
        self.title_font = None
        self.title_color = None
        self.update_interval = 1000
        self.charts: List[_Chart] = []
        # the place in `charts` of the chart of each cell, or -1
        self.taken = [[-1] * cols for _ in range(rows)]

    def check_bounds(self, row: int, column: int) -> None:
        if row < 0 or column < 0:
            raise JavaError(
                f"{_PLOT}PlotIllegalArgumentException: {_info(self.title)}Chart indices must be >0. "
                f"row:{row} col:{column}"
            )
        if row >= self.rows or column >= self.cols:
            raise JavaError(
                f"{_PLOT}PlotIllegalArgumentException: {_info(self.title)}Chart is not in grid. Trying to access chart "
                f"at position [{row + 1}x{column + 1}], chart grid is [{self.rows}x{self.cols}]"
            )


class _Edit:
    """The changes one method makes: a copy of the figure, and the chart, axes, axis and
    series the next call acts on.

    Each `j_` method does what the Java method named in its comment does. Java copies the
    figure for every such call; the calls of one method here share one copy, which a
    failure discards as a whole.
    """

    def __init__(self, figure: "Figure"):
        self.model: _Model = copy.deepcopy(figure._model)
        self.tables = list(figure._tables)
        # (chart,), (chart, axes), (chart, dim, index), (chart, axes, series name)
        self.chart_at, self.axes_at, self.axis_at, self.series_at = figure._cursor

    # -- where the cursor goes after a call. Behavior: `FigureImpl#make`

    def _last_axis(self, axes_at):
        if axes_at is None:
            return None
        chart, axes = axes_at
        found = self.model.charts[chart].axes[axes]
        if found.dimension <= 0:
            return None
        dim = found.dimension - 1
        return (chart, dim, found.axis[dim])

    def _last_series(self, axes_at):
        if axes_at is None:
            return None
        chart, axes = axes_at
        series = self.model.charts[chart].axes[axes].series
        return (chart, axes, series[-1].name) if series else None

    def _made_chart(self, chart: int) -> None:
        self.chart_at = chart
        count = len(self.model.charts[chart].axes)
        self.axes_at = (chart, count - 1) if count else None
        self.axis_at = self._last_axis(self.axes_at)
        self.series_at = self._last_series(self.axes_at)

    def _made_axes(self, axes_at) -> None:
        self.chart_at = axes_at[0]
        self.axes_at = axes_at
        self.axis_at = self._last_axis(axes_at)
        self.series_at = self._last_series(axes_at)

    def _made_axis(self, axis_at) -> None:
        self.chart_at = axis_at[0]
        self.axis_at = axis_at
        self.series_at = self._last_series(self.axes_at)

    def _made_series(self, series_at) -> None:
        self.chart_at = series_at[0]
        self.axes_at = series_at[:2]
        self.axis_at = self._last_axis(self.axes_at)
        self.series_at = series_at

    # -- what a call acts on. Behavior: `FigureImpl#chart`, `#axes`, `#axis`, `#series`

    def _chart(self) -> int:
        if self.chart_at is None:
            return self._new_chart()
        return self.chart_at

    def _axes(self):
        if self.axes_at is None:
            chart = self._chart()
            return self._new_axes(chart, None)
        return self.axes_at

    def _axis(self) -> _Axis:
        if self.axis_at is None:
            raise self._error("PlotRuntimeException", "No axes have been selected.", chart=None)
        chart, dim, index = self.axis_at
        return self.model.charts[chart].axis[dim][index]

    def _series(self) -> _Series:
        if self.series_at is not None:
            chart, axes, name = self.series_at
            for series in self.model.charts[chart].axes[axes].series:
                if series.name == name:
                    return series
        raise self._error("PlotRuntimeException", "No series has been selected.", chart=None)

    def _error(self, kind: str, text: str, chart=-1, series=None) -> Exception:
        """A Java plot exception. `chart` is the place of the chart it names, None for
        none, -1 for the chart the cursor is on."""
        if chart == -1:
            chart = self.chart_at
        title = None
        if chart is not None:
            title = self.model.charts[chart].title or ""
        return JavaError(f"{_PLOT}{kind}: {_info(self.model.title, title, series)}{text}")

    # -- charts

    def _new_chart(self, row=None, column=None) -> int:
        model = self.model
        if row is None:
            open_cells = [(r, c) for r in range(model.rows) for c in range(model.cols) if model.taken[r][c] < 0]
            if not open_cells:
                raise JavaError(f"{_PLOT}PlotRuntimeException: {_info(model.title)}No open space for chart in figure")
            row, column = open_cells[0]
        model.check_bounds(row, column)
        if model.taken[row][column] >= 0:
            raise NotPorted(
                "io.deephaven.plot.ChartArray#addChart",
                "a chart where the figure has one. Java keeps both charts in the figure it "
                "sends, or fails with an IndexOutOfBoundsException",
            )
        model.charts.append(_Chart(row, column))
        model.taken[row][column] = len(model.charts) - 1
        return len(model.charts) - 1

    def j_new_chart(self) -> None:  # newChart()
        self._made_chart(self._new_chart())

    def j_new_chart_index(self, index: int) -> None:  # newChart(int)
        self._made_chart(self._new_chart(index // self.model.cols, index % self.model.cols))

    def j_new_chart_at(self, row: int, col: int) -> None:  # newChart(int, int)
        self._made_chart(self._new_chart(row, col))

    def j_chart_at(self, row: int, col: int) -> None:  # chart(int, int)
        self.model.check_bounds(row, col)
        chart = self.model.taken[row][col]
        if chart < 0:
            raise JavaError(
                'java.lang.NullPointerException: Cannot invoke "io.deephaven.plot.ChartImpl.figure()" '
                'because "chart" is null'
            )
        self._made_chart(chart)

    def j_chart_index(self, index: int) -> None:  # chart(int)
        self.j_chart_at(index // self.model.cols, index % self.model.cols)

    def _on_chart(self) -> _Chart:
        chart = self._chart()
        self._made_chart(chart)
        return self.model.charts[chart]

    def j_span(self, row_span, col_span) -> None:  # span(int, int), rowSpan(int), colSpan(int)
        chart_at = self._chart()
        chart = self.model.charts[chart_at]
        rows = chart.rowspan if row_span is None else row_span
        cols = chart.colspan if col_span is None else col_span
        if rows < 1 or cols < 1:
            raise self._error(
                "PlotIllegalArgumentException",
                f"Row and column span must be at least one! rowspan={rows}, colspan={cols}",
                chart=chart_at,
            )
        model = self.model
        model.check_bounds(chart.row + rows - 1, chart.column + cols - 1)
        cells = [(r, c) for r in range(chart.row, chart.row + rows) for c in range(chart.column, chart.column + cols)]
        if any(model.taken[r][c] not in (-1, chart_at) for r, c in cells):
            raise NotPorted(
                "io.deephaven.plot.ChartArray#resizeChart", "a chart that grows over another chart"
            )
        for r in range(model.rows):
            for c in range(model.cols):
                if model.taken[r][c] == chart_at:
                    model.taken[r][c] = -1
        for r, c in cells:
            model.taken[r][c] = chart_at
        chart.rowspan, chart.colspan = rows, cols
        self._made_chart(chart_at)

    def j_remove_chart(self, row: int, col: int) -> None:  # removeChart(int, int)
        # Java frees the cells and keeps the chart in the figure a client is sent
        model = self.model
        model.check_bounds(row, col)
        chart_at = model.taken[row][col]
        if chart_at < 0:
            raise JavaError(
                f"{_PLOT}PlotIllegalArgumentException: {_info(model.title)}Can not remove chart at "
                f"({row}, {col})- chart does not exist!"
            )
        for r in range(model.rows):
            for c in range(model.cols):
                if model.taken[r][c] == chart_at:
                    model.taken[r][c] = -1

    def j_remove_chart_index(self, index: int) -> None:  # removeChart(int)
        self.j_remove_chart(index // self.model.cols, index % self.model.cols)

    # -- axes

    def _new_axes(self, chart_at: int, name, shared=None):
        """Makes axes in a chart: with new axis where `shared` has None.

        Behavior: `io.deephaven.plot.ChartImpl#newAxes`
        """
        chart = self.model.charts[chart_at]
        text = str(len(chart.axes)) if name is None else name
        if any(axes.name == text for axes in chart.axes):
            raise self._error(
                "PlotRuntimeException", f"Axis with this name already exists. name={text}", chart=chart_at
            )
        axis = []
        for dim in range(2):
            if shared is not None and shared[dim] is not None:
                axis.append(shared[dim])
            else:
                chart.axis[dim].append(_Axis(dim, len(chart.axis[dim])))
                axis.append(len(chart.axis[dim]) - 1)
        chart.axes.append(_Axes(text, axis))
        return (chart_at, len(chart.axes) - 1)

    def j_new_axes(self, name=None, dim=None) -> None:  # newAxes(), (String), (int), (String, int)
        if dim is not None and dim != 2:
            raise NotPorted("io.deephaven.plot.Figure#newAxes(int)", "axes that do not have two dimensions")
        self._made_axes(self._new_axes(self._chart(), name))

    def j_twin(self, name=None, dim=None, share=(0, 1)) -> None:  # twin, twinX, twinY and their variants
        chart_at, axes_at = self._axes()
        old = self.model.charts[chart_at].axes[axes_at]
        if dim is not None:
            if not 0 <= dim <= 1:
                raise self._error(
                    "PlotIllegalArgumentException",
                    f"Axis not found: index={dim}, required in range = [0,1]",
                    chart=chart_at,
                )
            share = (dim,)
        shared = [old.axis[d] if d in share else None for d in range(2)]
        self._made_axes(self._new_axes(chart_at, name, shared))

    def j_axes_name(self, name: str) -> None:  # axes(String)
        chart_at = self._chart()
        for place, axes in enumerate(self.model.charts[chart_at].axes):
            if axes.name == name:
                self._made_axes((chart_at, place))
                return
        raise JavaError(
            'java.lang.NullPointerException: Cannot invoke "io.deephaven.plot.AxesImpl.chart()" '
            'because "axes" is null'
        )

    def j_axes_id(self, place: int) -> None:  # axes(int)
        chart_at = self._chart()
        count = len(self.model.charts[chart_at].axes)
        if not 0 <= place < count:
            raise self._error(
                "PlotIllegalArgumentException",
                f"Axes not in chart: index={place}, required in range = [0,{count - 1}]",
                chart=chart_at,
            )
        self._made_axes((chart_at, place))

    def _on_axes(self) -> _Axes:
        axes_at = self._axes()
        self._made_axes(axes_at)
        return self.model.charts[axes_at[0]].axes[axes_at[1]]

    def j_plot_style(self, style) -> None:  # plotStyle(PlotStyle), plotStyle(String)
        if isinstance(style, PlotStyle):
            name = style.value
        else:
            name = style.upper()
            if name not in PlotStyle.__members__ and name != "TREEMAP":
                raise JavaError(f"java.lang.IllegalArgumentException: PlotStyle {style} is not defined")
        self._on_axes().plot_style = name

    def _remove_series(self, axes_list, names) -> None:
        for axes in axes_list:
            axes.series = [series for series in axes.series if series.name not in names]

    def j_axes_remove_series(self, names) -> None:  # axesRemoveSeries(String...)
        self._remove_series([self._on_axes()], names)

    def j_chart_remove_series(self, names) -> None:  # chartRemoveSeries(String...)
        self._remove_series(self._on_chart().axes, names)

    def j_figure_remove_series(self, names) -> None:  # figureRemoveSeries(String...)
        for chart in self.model.charts:
            self._remove_series(chart.axes, names)

    # -- one axis

    def j_axis(self, dim: int) -> None:  # axis(int)
        chart_at, axes_at = self._axes()
        axes = self.model.charts[chart_at].axes[axes_at]
        if not 0 <= dim <= 1:
            raise self._error(
                "PlotIllegalArgumentException",
                f"Axis not found: index={dim}, required in range = [0,1]",
                chart=chart_at,
            )
        # Java leaves the axes the next call acts on as they were
        self._made_axis((chart_at, dim, axes.axis[dim]))

    def _selected_axis(self) -> _Axis:
        axis = self._axis()
        self._made_axis(self.axis_at)
        return axis

    def _axis_of_axes(self, dim: int) -> _Axis:
        """The axis of a dimension of the axes the cursor is on, for `xLabel` and the like."""
        chart_at, axes_at = self._axes()
        self._made_axes((chart_at, axes_at))
        chart = self.model.charts[chart_at]
        return chart.axis[dim][chart.axes[axes_at].axis[dim]]

    # -- plots

    def _column(self, table: Table, column: str, series: str, types=None, message=None):
        """Checks a column a series reads.

        Behavior: `io.deephaven.plot.util.ArgumentValidations#assertIsNumericOrTime` and
        `io.deephaven.plot.datasets.data.IndexableNumericDataTable`
        """
        definition = table.definition
        if column not in definition:
            raise self._plot_error("PlotRuntimeException", f"Column not present in table: column={column}", series)
        if types is None:
            return
        data_type = definition[column].data_type
        if data_type.j_name in types:
            return
        if message is not None:
            raise self._plot_error("PlotRuntimeException", message, series)
        shown = data_type.j_name if data_type.is_primitive else f"class {data_type.j_name}"
        raise JavaError(
            f"java.lang.UnsupportedOperationException: Unsupported numeric data type: columnName={column} type={shown}"
        )

    def _start(self) -> None:
        """Puts the cursor on the axes a plot draws on, made if there are none."""
        self._made_axes(self._axes())

    def _plot_error(self, kind: str, text: str, series: str) -> Exception:
        return self._error(kind, text, chart=self.axes_at[0], series=series)

    def _table(self, kind: str, table: Table, *key, shared: bool = True) -> int:
        """The place of a table of the figure, added if the figure does not have it."""
        entry = (kind, table, key)
        if shared:
            for place, (other_kind, other, other_key) in enumerate(self.tables):
                if other_kind == kind and other.j_table is table.j_table and other_key == key:
                    return place
        self.tables.append(entry)
        return len(self.tables) - 1

    def _add(self, chart_type: str, series: _Series, x_type: str = "NUMBER", style=None) -> None:
        """Adds a series to the axes the cursor is on.

        Behavior: `io.deephaven.plot.AxesImpl#configureXYPlot` and the like, then
        `io.deephaven.plot.SeriesCollection#add`
        """
        chart_at, axes_at = self.axes_at
        chart = self.model.charts[chart_at]
        axes = chart.axes[axes_at]
        if chart.type is not None and chart.type != chart_type:
            raise self._error(
                "PlotUnsupportedOperationException",
                f"Attempting to create inconsistent plot types: {chart.type}, {chart_type}",
                chart=chart_at,
            )
        if any(other.name == series.name for other in axes.series):
            raise self._error(
                "PlotUnsupportedOperationException",
                f"Series with the same name already exists in the collection.  name={series.name}",
                chart=chart_at,
            )
        chart.type = chart_type
        chart.axis[0][axes.axis[0]].type = x_type
        chart.axis[1][axes.axis[1]].type = "NUMBER"
        if style is not None:
            axes.plot_style = style
        axes.dimension = 2
        chart.initialized = True
        axes.series.append(series)
        self._made_series((chart_at, axes_at, series.name))

    def _by(self, table: Table, by, name: str) -> None:
        if not by:
            raise self._plot_error("PlotIllegalArgumentException", "Null or empty input: byColumns", name)
        for column in by:
            if column not in table.definition:
                # the last line of the error of the formula Java partitions by
                raise JavaError(f"Cannot find variable or class {column}")

    def j_plot_xy(self, name, table, sources, by=None) -> None:
        # plot, plotBy, errorBarX, errorBarXBy, errorBarY, errorBarYBy, errorBarXY, errorBarXYBy
        self._start()
        for _, _, column in sources:
            self._column(table, column, name, _XY_TYPES)
        kind = "xy" if len(sources) == 2 else "xyerr"
        if by is None:
            series = _Series(name, kind, False, self._table("plain", table), sources)
        else:
            self._by(table, by, name)
            series = _Series(name, kind, True, self._table("by", table, tuple(by)), sources)
        self._add("XY", series)

    def j_ohlc(self, name, table, x, open, high, low, close, by=None) -> None:  # ohlcPlot, ohlcPlotBy
        self._start()
        sources = [("TIME", 0, x), ("OPEN", 1, open), ("CLOSE", 1, close), ("HIGH", 1, high), ("LOW", 1, low)]
        for _, _, column in sources:
            self._column(table, column, name, _XY_TYPES)
        if by is None:
            series = _Series(name, "ohlc", False, self._table("plain", table), sources)
        else:
            self._by(table, by, name)
            series = _Series(name, "ohlc", True, self._table("by", table, tuple(by)), sources)
        self._add("XY", series, style="OHLC")

    def _category(self, table: Table, category: str) -> None:
        if category not in table.definition:
            raise JavaError(
                "io.deephaven.engine.table.impl.NoSuchColumnException: aggregation: not all group-by "
                f"columns [{category}] are present in input table with columns "
                f"[{', '.join(table.column_names)}]. Missing columns: [{category}]"
            )

    def j_cat(self, name, table, category, y, y_low=None, y_high=None, by=None, chart_type="CATEGORY") -> None:
        # catPlot, catPlotBy, catErrorBar, piePlot
        self._start()
        self._category(table, category)
        sources = [("X", 0, category), ("Y", 1, y)]
        if y_low is not None:
            sources += [("Y_LOW", 1, y_low), ("Y_HIGH", 1, y_high)]
        for _, _, column in sources[1:]:
            self._column(table, column, name, _XY_TYPES, f"Invalid data type in data column: column={column}")
        kind = "cat" if y_low is None else "caterr"
        if by is None:
            series = _Series(name, kind, False, self._table("cat", table, category), sources)
        else:
            self._by(table, by, name)
            series = _Series(name, kind, True, self._table("catby", table, category, tuple(by)), sources)
        self._add(chart_type, series, x_type="CATEGORY")

    def j_cat_hist(self, name, table, category) -> None:  # catHistPlot
        self._start()
        self._category(table, category)
        place = self._table("cathist", table, category, shared=False)
        series = _Series(name, "cat", False, place, [("X", 0, category), ("Y", 1, "Count")])
        self._add("CATEGORY", series, x_type="CATEGORY", style="HISTOGRAM")

    def j_treemap(self, name, table, id, parent, value, label, hover_text, color) -> None:  # treemapPlot
        self._start()
        self._category(table, id)
        sources = [("X", 0, id), ("PARENT", None, parent)]
        if value is not None:
            sources.append(("Y", 1, value))
        for kind, column in (("LABEL", label), ("COLOR", color), ("HOVER_TEXT", hover_text)):
            if column is not None:
                sources.append((kind, None, column))
        for _, _, column in sources[1:]:
            self._column(table, column, name)
        series = _Series(name, "treemap", False, self._table("cat", table, id), sources)
        self._add("TREEMAP", series, x_type="CATEGORY")

    def j_hist(self, name, table, x, nbins, xmin=None, xmax=None) -> None:  # histPlot
        self._start()
        self._column(
            table, x, name, _HISTOGRAM_TYPES, f"Histogram can not be computed on non-numeric column: {x}"
        )
        if nbins < 1:
            raise NotPorted(
                "io.deephaven.plot.Figure#histPlot",
                "a histogram of no bins, for which Java computes bins without bounds",
            )
        counts = _histogram(table, x, nbins, xmin, xmax)
        sources = [
            ("X", 0, "BinMid"),
            ("X_LOW", 0, "BinMin"),
            ("X_HIGH", 0, "BinMax"),
            ("Y", 1, "Count"),
            ("Y_LOW", 1, "Count"),
            ("Y_HIGH", 1, "Count"),
        ]
        series = _Series(name, "hist", False, self._table("plain", counts, shared=False), sources)
        self._add("XY", series, style="HISTOGRAM")

    # -- series

    def j_series_name(self, name: str) -> None:  # series(String)
        chart_at, axes_at = self._axes()
        if not any(series.name == name for series in self.model.charts[chart_at].axes[axes_at].series):
            raise self._error("PlotIllegalArgumentException", f"Series {name} not found", chart=chart_at)
        self._made_series((chart_at, axes_at, name))

    def _selected_series(self) -> _Series:
        series = self._series()
        self._made_series(self.series_at)
        return series


def _histogram(table: Table, column: str, nbins: int, xmin, xmax) -> Table:
    """The counts of the values of a column in `nbins` bins of one width, between the
    least and the greatest value or between `xmin` and `xmax`: a row for each bin that has
    a value, in the order the bins are first met.

    Behavior: `io.deephaven.plot.datasets.histogram.HistogramCalculator#calc` with
    `io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual`
    """
    data = table.view([f"X = isNull({column}) ? Double.NaN : (double) {column}"]).where("isFinite(X)")
    if xmin is None:
        ranged = data.join(data.agg_by([agg.min_("RangeMin = X"), agg.max_("RangeMax = X")]))
    else:
        ranged = data.update_view([f"RangeMin = (double) {float(xmin)!r}", f"RangeMax = (double) {float(xmax)!r}"])
    index = "(long) ((X - RangeMin) / BinWidth)"
    return (
        ranged.update_view(
            [
                f"BinWidth = (RangeMax - RangeMin) / {int(nbins)}",
                f"RangeIndex = X < RangeMin || X > RangeMax ? NULL_LONG : X == RangeMax ? {index} - 1 : {index}",
            ]
        )
        .where("!isNull(RangeIndex)")
        .agg_by([agg.count_("Count"), agg.last(["RangeMin", "BinWidth"])], by=["RangeIndex"])
        .update_view(
            [
                "BinMin = RangeMin + BinWidth * RangeIndex",
                "BinMax = RangeMin + BinWidth * (RangeIndex + 1)",
                "BinMid = 0.5 * (BinMin + BinMax)",
            ]
        )
        .view(["BinMin", "BinMax", "BinMid", "Count"])
    )


def _category_table(table: Table, keys: list, values: list) -> Table:
    """The last row of each category, with the row key the category was first seen at.

    Behavior: `io.deephaven.plot.util.PlotUtils#createCategoryAggs`, an `AggFirstRowKey`
    with the `AggLast` of the other columns
    """
    aggs = [agg.first(_CAT_ORDER)]
    if values:
        aggs.append(agg.last(values))
    return table.update_view(f"{_CAT_ORDER} = k").agg_by(aggs, by=keys)


def _number(value: float):
    """A double as protobuf JSON writes it."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    return float(value)


def _with_default(value) -> dict:
    return {} if value is None else value


class Figure:
    """A figure: charts in a grid, each with axes and series. A figure does not change;
    every method returns a new figure. Only a figure `show()` returned can be opened by a
    client."""

    def __init__(self, rows: int = 1, cols: int = 1, j_figure=None):
        self._handle = None
        self._model = None
        self._tables: list = []
        self._cursor = (None, None, None, None)
        if j_figure is not None:
            if not isinstance(j_figure, _engine.native.FigureHandle):
                raise DHError(message=f"not a figure handle: {type(j_figure).__name__}")
            self._handle = j_figure
        else:
            self._model = _Model(rows, cols)

    @classmethod
    def _shown(cls, handle) -> "Figure":
        """The figure of a handle the server holds: one that was shown elsewhere."""
        return cls(j_figure=handle)

    @property
    def j_object(self):
        """The server's handle of the figure, if it was shown; None if it was not, as an
        unshown figure of Java is no object a client can fetch."""
        return self._handle

    @property
    def j_figure(self):
        return self._handle

    def _edit(self) -> _Edit:
        if self._model is None:
            raise NotPorted(
                "io.deephaven.plot.Figure",
                "changes to a figure that reached Python as an object of the server; what it plots is not kept",
            )
        return _Edit(self)

    def _done(self, edit: _Edit) -> "Figure":
        figure = Figure.__new__(Figure)
        figure._handle = None
        figure._model = edit.model
        figure._tables = edit.tables
        figure._cursor = (edit.chart_at, edit.axes_at, edit.axis_at, edit.series_at)
        return figure

    def _apply(self, left: set, rules: list, args: dict) -> "Figure":
        """Runs the Java calls of a method: each rule whose parameters are all still
        unused, in order, uses them up. Nothing called or a parameter left over is an
        unsupported combination, as in the reference."""
        edit = self._edit()
        called = False
        for names, call in rules:
            if set(names) <= left:
                call(edit, *[args[name] for name in names])
                left = left - set(names)
                called = True
        if not called or left:
            raise DHError(f"unsupported parameter combination: {left}")
        return self._done(edit)

    def _exactly(self, left: set, rules: list, args: dict) -> "Figure":
        """Runs the one Java call whose parameters are exactly the ones that are set."""
        for names, call in rules:
            if set(names) == left:
                edit = self._edit()
                call(edit, *[args[name] for name in names])
                return self._done(edit)
        raise DHError(f"unsupported parameter combination: {left}")

    # -- the figure

    def show(self) -> "Figure":
        """The figure as a client can open it."""
        if self._model is None:
            return self
        model = self._model
        if not all(chart.initialized for chart in model.charts):
            raise JavaError(f"{_PLOT}PlotRuntimeException: {_info(model.title)}No plots created yet.")
        description, tables, partitioned = _describe(model, self._tables)
        handle = _engine.native.figure(
            json.dumps(description),
            [table.j_table for table in tables],
            [table.j_partitioned_table for table in partitioned],
        )
        figure = Figure.__new__(Figure)
        figure._handle = handle
        figure._model = model
        figure._tables = self._tables
        figure._cursor = self._cursor
        return figure

    def save(
        self, path: str, height: int = None, width: int = None, wait: bool = None, timeout_seconds: int = None
    ) -> "Figure":
        """Refused: this server draws nothing. Java refuses it too."""
        if not path:
            raise DHError("required parameter is not set: path")
        raise JavaError(
            "java.lang.UnsupportedOperationException: class io.deephaven.plot.BaseFigureImpl does not implement save"
        )

    def figure(
        self,
        remove_series: List[str] = None,
        remove_chart_index: int = None,
        remove_chart_row: int = None,
        remove_chart_col: int = None,
        update_millis: int = None,
    ) -> "Figure":
        """Changes the figure: removes series or a chart, or sets how often a client
        redraws."""
        args = dict(
            remove_series=remove_series,
            remove_chart_index=remove_chart_index,
            remove_chart_row=remove_chart_row,
            remove_chart_col=remove_chart_col,
            update_millis=update_millis,
        )
        types = dict(
            remove_series=(list,),
            remove_chart_index=(int,),
            remove_chart_row=(int,),
            remove_chart_col=(int,),
            update_millis=(int,),
        )

        def update(edit, millis):  # updateInterval(long)
            edit.model.update_interval = millis

        rules = [
            (("remove_chart_row", "remove_chart_col"), _Edit.j_remove_chart),
            (("remove_series",), _Edit.j_figure_remove_series),
            (("remove_chart_index",), _Edit.j_remove_chart_index),
            (("update_millis",), update),
        ]
        return self._apply(_set_of(args, types), rules, args)

    def figure_title(self, title: str = None, color: Union[str, int, Color] = None, font: Font = None) -> "Figure":
        """Sets the title of the figure, its color or its font."""
        args = dict(title=title, color=color, font=font)

        def set_title(edit, text):  # figureTitle(String)
            edit.model.title = text

        def set_color(edit, value):  # figureTitleColor(Paint), (String)
            edit.model.title_color = _color(value)

        def set_font(edit, value):  # figureTitleFont(Font)
            edit.model.title_font = value.family

        rules = [(("title",), set_title), (("color",), set_color), (("font",), set_font)]
        return self._apply(_set_of(args, dict(title=(str,), color=_COLOR, font=(Font,))), rules, args)

    # -- charts

    def new_chart(self, index: int = None, row: int = None, col: int = None) -> "Figure":
        """Adds a chart: in the next open cell, or at a place in the grid."""
        args = dict(index=index, row=row, col=col)
        rules = [
            ((), _Edit.j_new_chart),
            (("index",), _Edit.j_new_chart_index),
            (("row", "col"), _Edit.j_new_chart_at),
        ]
        return self._exactly(_set_of(args, dict(index=(int,), row=(int,), col=(int,))), rules, args)

    def chart(
        self,
        multi_series_key: List[Any] = None,
        remove_series: List[str] = None,
        index: int = None,
        row: int = None,
        col: int = None,
        row_span: int = None,
        col_span: int = None,
        orientation: str = None,
        grid_visible: bool = None,
        x_grid_visible: bool = None,
        y_grid_visible: bool = None,
        pie_label_format: str = None,
    ) -> "Figure":
        """Chooses a chart, or changes the chart the figure is on."""
        args = dict(
            multi_series_key=multi_series_key,
            remove_series=remove_series,
            index=index,
            row=row,
            col=col,
            row_span=row_span,
            col_span=col_span,
            orientation=orientation,
            grid_visible=grid_visible,
            x_grid_visible=x_grid_visible,
            y_grid_visible=y_grid_visible,
            pie_label_format=pie_label_format,
        )
        types = dict(
            multi_series_key=(list,),
            remove_series=(list,),
            index=(int,),
            row=(int,),
            col=(int,),
            row_span=(int,),
            col_span=(int,),
            orientation=(str,),
            grid_visible=(bool,),
            x_grid_visible=(bool,),
            y_grid_visible=(bool,),
            pie_label_format=(str,),
        )
        left = _set_of(args, types)
        _no_key(left)

        def orient(edit, value):  # plotOrientation(String)
            # Behavior: `io.deephaven.plot.ChartImpl.PlotOrientation#fromString`
            name = value.upper()
            if name and "HORIZONTAL".startswith(name):
                # Java swaps the axes of a category chart for a client; of an XY chart it
                # tells a client nothing
                raise NotPorted("io.deephaven.plot.Figure#plotOrientation", "a horizontal chart")
            if not (name and "VERTICAL".startswith(name)):
                edit._made_chart(edit._chart())
                raise edit._error(
                    "PlotIllegalArgumentException",
                    f"No enum constant io.deephaven.plot.ChartImpl.PlotOrientation.{name}",
                )
            edit._on_chart()

        def span(edit, rows, cols):  # span(int, int): the rows, then the columns
            edit.j_span(rows, None)
            edit.j_span(None, cols)

        def untold(edit, value):
            # gridLinesVisible, xGridLinesVisible, yGridLinesVisible, piePercentLabelFormat:
            # kept by Java and told to no client
            edit._on_chart()

        rules = [
            (("row", "col"), _Edit.j_chart_at),
            (("row_span", "col_span"), span),
            (("index",), _Edit.j_chart_index),
            (("remove_series",), _Edit.j_chart_remove_series),
            (("row_span",), lambda edit, rows: edit.j_span(rows, None)),
            (("col_span",), lambda edit, cols: edit.j_span(None, cols)),
            (("orientation",), orient),
            (("grid_visible",), untold),
            (("x_grid_visible",), untold),
            (("y_grid_visible",), untold),
            (("pie_label_format",), untold),
        ]
        return self._apply(left, rules, args)

    def chart_title(
        self,
        t: Union[Table, SelectableDataSet] = None,
        title: str = None,
        columns: List[str] = None,
        format: str = None,
        max_rows: int = None,
        column_names_in_title: bool = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
    ) -> "Figure":
        """Sets the title of the chart, its color or its font."""
        args = dict(
            t=t,
            title=title,
            columns=columns,
            format=format,
            max_rows=max_rows,
            column_names_in_title=column_names_in_title,
            color=color,
            font=font,
        )
        types = dict(
            t=_TABLE,
            title=(str,),
            columns=(list,),
            format=(str,),
            max_rows=(int,),
            column_names_in_title=(bool,),
            color=_COLOR,
            font=(Font,),
        )

        def from_table(edit, *values):
            raise NotPorted("io.deephaven.plot.Figure#chartTitle(Table, String...)", "a title read from a table")

        def set_title(edit, text):  # chartTitle(String)
            edit._on_chart().title = text

        def set_color(edit, value):  # chartTitleColor(Paint), (String)
            edit._on_chart().title_color = _color(value)

        def set_font(edit, value):  # chartTitleFont(Font)
            edit._on_chart().title_font = value.family

        rules = [
            (("column_names_in_title", "t", "columns"), from_table),
            (("format", "t", "columns"), from_table),
            (("t", "columns"), from_table),
            (("title",), set_title),
            (("color",), set_color),
            (("font",), set_font),
            (("max_rows",), lambda edit, rows: edit._on_chart()),  # maxRowsInTitle(int)
        ]
        return self._apply(_set_of(args, types), rules, args)

    def chart_legend(self, color: Union[str, int, Color] = None, font: Font = None, visible: int = None) -> "Figure":
        """Sets the color or the font of the legend of the chart, or whether it shows."""
        args = dict(color=color, font=font, visible=visible)

        def set_color(edit, value):  # legendColor(Paint), (String)
            edit._on_chart().legend_color = _color(value)

        def set_font(edit, value):  # legendFont(Font)
            edit._on_chart().legend_font = value.family

        def set_visible(edit, value):  # legendVisible(boolean)
            edit._on_chart().show_legend = bool(value)

        rules = [(("color",), set_color), (("font",), set_font), (("visible",), set_visible)]
        return self._apply(_set_of(args, dict(color=_COLOR, font=(Font,), visible=(int,))), rules, args)

    # -- axes

    def new_axes(self, name: str = None, dim: int = None) -> "Figure":
        """Adds axes to the chart, each with a new axis."""
        args = dict(name=name, dim=dim)
        rules = [
            ((), _Edit.j_new_axes),
            (("dim",), lambda edit, dim: edit.j_new_axes(None, dim)),
            (("name",), _Edit.j_new_axes),
            (("name", "dim"), _Edit.j_new_axes),
        ]
        return self._exactly(_set_of(args, dict(name=(str,), dim=(int,))), rules, args)

    def twin(self, name: str = None, dim: int = None) -> "Figure":
        """Adds axes that share every axis with the axes the figure is on, or only the
        axis of the dimension `dim`."""
        args = dict(name=name, dim=dim)
        rules = [
            ((), _Edit.j_twin),
            (("dim",), lambda edit, dim: edit.j_twin(None, dim)),
            (("name",), _Edit.j_twin),
            (("name", "dim"), _Edit.j_twin),
        ]
        return self._exactly(_set_of(args, dict(name=(str,), dim=(int,))), rules, args)

    def x_twin(self, name: str = None) -> "Figure":
        """Adds axes that share the x axis with the axes the figure is on."""
        args = dict(name=name)
        rules = [
            ((), lambda edit: edit.j_twin(None, None, (0,))),
            (("name",), lambda edit, name: edit.j_twin(name, None, (0,))),
        ]
        return self._exactly(_set_of(args, dict(name=(str,))), rules, args)

    def y_twin(self, name: str = None) -> "Figure":
        """Adds axes that share the y axis with the axes the figure is on."""
        args = dict(name=name)
        rules = [
            ((), lambda edit: edit.j_twin(None, None, (1,))),
            (("name",), lambda edit, name: edit.j_twin(name, None, (1,))),
        ]
        return self._exactly(_set_of(args, dict(name=(str,))), rules, args)

    def axes(
        self,
        name: str = None,
        axes: int = None,
        remove_series: List[str] = None,
        plot_style: Union[str, PlotStyle] = None,
    ) -> "Figure":
        """Chooses axes of the chart, removes series from them or sets how their series
        are drawn."""
        args = dict(name=name, axes=axes, remove_series=remove_series, plot_style=plot_style)
        types = dict(name=(str,), axes=(int,), remove_series=(list,), plot_style=(str, PlotStyle))
        rules = [
            (("axes",), _Edit.j_axes_id),
            (("name",), _Edit.j_axes_name),
            (("remove_series",), _Edit.j_axes_remove_series),
            (("plot_style",), _Edit.j_plot_style),
        ]
        return self._apply(_set_of(args, types), rules, args)

    def _axis_rules(self, axis_of, java: str, first: list) -> list:
        """The rules of `axis`, `x_axis` and `y_axis`: the same changes, to the axis
        `axis_of(edit)` gives."""

        def refuse(what):
            def call(edit, *values):
                raise NotPorted(f"io.deephaven.plot.Figure#{java(what)}")

            return call

        def no_overload(edit, *values):
            raise JavaError("no matching Java method overloads found")

        def set_min(edit, value):
            if isinstance(value, str):
                no_overload(edit)
            axis_of(edit).min = float(value)

        def set_max(edit, value):
            if isinstance(value, str):
                no_overload(edit)
            axis_of(edit).max = float(value)

        def set_range(edit, low, high):
            set_min(edit, low)
            set_max(edit, high)

        def set_color(edit, value):
            axis_of(edit).color = _color(value)

        def set_pattern(edit, value):
            axis_of(edit).format_pattern = value

        def set_label(edit, value):
            axis_of(edit).label = value

        def set_font(edit, value):
            axis_of(edit).label_font = value.family

        def set_invert(edit, value):
            axis_of(edit).invert = value

        def set_log(edit, value):
            axis_of(edit).log = value

        return (
            [
                (("t", "min"), no_overload),
                (("t", "max"), no_overload),
                (("min", "max"), set_range),
                (("t", "calendar"), refuse("businessTime")),
            ]
            + first
            + [
                (("color",), set_color),
                # a format is kept by Java and told to no client
                (("format",), lambda edit, value: axis_of(edit)),
                (("format_pattern",), set_pattern),
                (("label",), set_label),
                (("font",), set_font),
                (("invert",), set_invert),
                (("log",), set_log),
                (("min",), set_min),
                (("max",), set_max),
                (("business_time",), refuse("businessTime")),
                (("calendar",), refuse("businessTime")),
                (("transform",), refuse("transform")),
            ]
        )

    def axis(
        self,
        dim: int = None,
        t: Union[Table, SelectableDataSet] = None,
        label: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
        format: AxisFormat = None,
        format_pattern: str = None,
        min: Union[str, float] = None,
        max: Union[str, float] = None,
        invert: bool = None,
        log: bool = None,
        business_time: bool = None,
        calendar: Union[str, BusinessCalendar] = None,
        transform: AxisTransform = None,
    ) -> "Figure":
        """Chooses the axis of a dimension of the axes, or changes the chosen axis."""
        args = dict(
            dim=dim,
            t=t,
            label=label,
            color=color,
            font=font,
            format=format,
            format_pattern=format_pattern,
            min=min,
            max=max,
            invert=invert,
            log=log,
            business_time=business_time,
            calendar=calendar,
            transform=transform,
        )
        rules = self._axis_rules(_Edit._selected_axis, lambda name: name, [(("dim",), _Edit.j_axis)])
        return self._apply(_set_of(args, {"dim": (int,), **_AXIS_TYPES}), rules, args)

    def _dim_axis(self, dim: int, args: dict) -> "Figure":
        letter = "xy"[dim]
        rules = self._axis_rules(
            lambda edit: edit._axis_of_axes(dim), lambda name: letter + name[0].upper() + name[1:], []
        )
        left = _set_of(args, _AXIS_TYPES)
        if not left:
            # xAxis(), yAxis(): chooses the axis
            edit = self._edit()
            chart_at, axes_at = edit._axes()
            axes = edit.model.charts[chart_at].axes[axes_at]
            edit._made_axis((chart_at, dim, axes.axis[dim]))
            return self._done(edit)
        return self._apply(left, rules, args)

    def x_axis(
        self,
        t: Union[Table, SelectableDataSet] = None,
        label: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
        format: AxisFormat = None,
        format_pattern: str = None,
        min: Union[str, float] = None,
        max: Union[str, float] = None,
        invert: bool = None,
        log: bool = None,
        business_time: bool = None,
        calendar: Union[str, BusinessCalendar] = None,
        transform: AxisTransform = None,
    ) -> "Figure":
        """Changes the x axis of the axes, or chooses it."""
        return self._dim_axis(
            0,
            dict(
                t=t,
                label=label,
                color=color,
                font=font,
                format=format,
                format_pattern=format_pattern,
                min=min,
                max=max,
                invert=invert,
                log=log,
                business_time=business_time,
                calendar=calendar,
                transform=transform,
            ),
        )

    def y_axis(
        self,
        t: Union[Table, SelectableDataSet] = None,
        label: str = None,
        color: Union[str, int, Color] = None,
        font: Font = None,
        format: AxisFormat = None,
        format_pattern: str = None,
        min: Union[str, float] = None,
        max: Union[str, float] = None,
        invert: bool = None,
        log: bool = None,
        business_time: bool = None,
        calendar: Union[str, BusinessCalendar] = None,
        transform: AxisTransform = None,
    ) -> "Figure":
        """Changes the y axis of the axes, or chooses it."""
        return self._dim_axis(
            1,
            dict(
                t=t,
                label=label,
                color=color,
                font=font,
                format=format,
                format_pattern=format_pattern,
                min=min,
                max=max,
                invert=invert,
                log=log,
                business_time=business_time,
                calendar=calendar,
                transform=transform,
            ),
        )

    def _ticks(self, axis_of, args: dict) -> "Figure":
        def set_gap(edit, value):  # ticks(double)
            axis = axis_of(edit)
            axis.gap = float(value)
            axis.major_ticks_visible = True

        def set_locations(edit, value):  # ticks(double[])
            axis_of(edit).tick_locations = [float(place) for place in value]

        def set_visible(edit, value):  # ticksVisible(boolean)
            axis_of(edit).major_ticks_visible = bool(value)

        def set_angle(edit, value):  # tickLabelAngle(double)
            axis_of(edit).tick_label_angle = float(value)

        rules = [
            (("gap",), set_gap),
            (("loc",), set_locations),
            # the font of the ticks is kept by Java and told to no client
            (("font",), lambda edit, value: axis_of(edit)),
            (("visible",), set_visible),
            (("angle",), set_angle),
        ]
        return self._apply(_set_of(args, _TICK_TYPES), rules, args)

    def _ticks_minor(self, axis_of, args: dict) -> "Figure":
        def set_count(edit, value):  # minorTicks(int)
            axis = axis_of(edit)
            axis.minor_tick_count = value
            axis.minor_ticks_visible = True

        def set_visible(edit, value):  # minorTicksVisible(boolean)
            axis_of(edit).minor_ticks_visible = bool(value)

        rules = [(("nminor",), set_count), (("visible",), set_visible)]
        return self._apply(_set_of(args, _MINOR_TYPES), rules, args)

    def ticks(
        self, font: Font = None, gap: float = None, loc: List[float] = None, angle: int = None, visible: int = None
    ) -> "Figure":
        """Changes the major ticks of the chosen axis."""
        return self._ticks(_Edit._selected_axis, dict(font=font, gap=gap, loc=loc, angle=angle, visible=visible))

    def x_ticks(
        self, font: Font = None, gap: float = None, loc: List[float] = None, angle: int = None, visible: int = None
    ) -> "Figure":
        """Changes the major ticks of the x axis."""
        args = dict(font=font, gap=gap, loc=loc, angle=angle, visible=visible)
        return self._ticks(lambda edit: edit._axis_of_axes(0), args)

    def y_ticks(
        self, font: Font = None, gap: float = None, loc: List[float] = None, angle: int = None, visible: int = None
    ) -> "Figure":
        """Changes the major ticks of the y axis."""
        args = dict(font=font, gap=gap, loc=loc, angle=angle, visible=visible)
        return self._ticks(lambda edit: edit._axis_of_axes(1), args)

    def ticks_minor(self, nminor: int = None, visible: int = None) -> "Figure":
        """Changes the minor ticks of the chosen axis."""
        return self._ticks_minor(_Edit._selected_axis, dict(nminor=nminor, visible=visible))

    def x_ticks_minor(self, nminor: int = None, visible: int = None) -> "Figure":
        """Changes the minor ticks of the x axis."""
        return self._ticks_minor(lambda edit: edit._axis_of_axes(0), dict(nminor=nminor, visible=visible))

    def y_ticks_minor(self, nminor: int = None, visible: int = None) -> "Figure":
        """Changes the minor ticks of the y axis."""
        return self._ticks_minor(lambda edit: edit._axis_of_axes(1), dict(nminor=nminor, visible=visible))

    # -- plots

    def plot_xy(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        x: Union[str, List[int], List[float], List[Instant]] = None,
        x_low: Union[str, List[int], List[float], List[Instant]] = None,
        x_high: Union[str, List[int], List[float], List[Instant]] = None,
        y: Union[str, List[int], List[float], List[Instant]] = None,
        y_low: Union[str, List[int], List[float], List[Instant]] = None,
        y_high: Union[str, List[int], List[float], List[Instant]] = None,
        function: Callable = None,
        by: List[str] = None,
        x_time_axis: bool = None,
        y_time_axis: bool = None,
    ) -> "Figure":
        """Plots a column against another: a line by default, or what the style of the
        axes says. With `by`, one series for each distinct value of those columns. With
        low and high columns, error bars."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        args = dict(
            series_name=series_name,
            t=t,
            x=x,
            x_low=x_low,
            x_high=x_high,
            y=y,
            y_low=y_low,
            y_high=y_high,
            function=function,
            by=by,
            x_time_axis=x_time_axis,
            y_time_axis=y_time_axis,
        )
        types = dict(
            series_name=(str,),
            t=_TABLE,
            x=_DATA,
            x_low=_DATA,
            x_high=_DATA,
            y=_DATA,
            y_low=_DATA,
            y_high=_DATA,
            function=(_Callable,),
            by=(list,),
            x_time_axis=(bool,),
            y_time_axis=(bool,),
        )
        left = _set_of(args, types)

        def function_plot(edit, *values):
            raise NotPorted("io.deephaven.plot.Figure#plot(Comparable, DoubleUnaryOperator)")

        def xy(edit, name, table, x, y, by=None):
            edit.j_plot_xy(name, _table(table, "plot"), [("X", 0, x), ("Y", 1, y)], by)

        def error_x(edit, name, table, x, x_low, x_high, y, by=None):
            sources = [("X", 0, x), ("X_LOW", 0, x_low), ("X_HIGH", 0, x_high)]
            sources += [("Y", 1, y), ("Y_LOW", 1, y), ("Y_HIGH", 1, y)]
            edit.j_plot_xy(name, _table(table, "errorBarX"), _errors(sources, by), by)

        def error_y(edit, name, table, x, y, y_low, y_high, by=None):
            sources = [("X", 0, x), ("X_LOW", 0, x), ("X_HIGH", 0, x)]
            sources += [("Y", 1, y), ("Y_LOW", 1, y_low), ("Y_HIGH", 1, y_high)]
            edit.j_plot_xy(name, _table(table, "errorBarY"), _errors(sources, by), by)

        def error_xy(edit, name, table, x, x_low, x_high, y, y_low, y_high, by=None):
            sources = [("X", 0, x), ("X_LOW", 0, x_low), ("X_HIGH", 0, x_high)]
            sources += [("Y", 1, y), ("Y_LOW", 1, y_low), ("Y_HIGH", 1, y_high)]
            edit.j_plot_xy(name, _table(table, "errorBarXY"), _errors(sources, by), by)

        rules = [
            (("series_name", "function"), function_plot),
            (("series_name", "x", "y"), _arrays("plot")),
            (("series_name", "t", "x", "y"), xy),
            (("series_name", "x", "y", "x_time_axis", "y_time_axis"), _arrays("plot")),
            (("series_name", "t", "x", "y", "by"), xy),
            (("series_name", "x", "x_low", "x_high", "y"), _arrays("errorBarX")),
            (("series_name", "t", "x", "x_low", "x_high", "y"), error_x),
            (("series_name", "t", "x", "x_low", "x_high", "y", "by"), error_x),
            (("series_name", "x", "y", "y_low", "y_high"), _arrays("errorBarY")),
            (("series_name", "t", "x", "y", "y_low", "y_high"), error_y),
            (("series_name", "t", "x", "y", "y_low", "y_high", "by"), error_y),
            (("series_name", "x", "x_low", "x_high", "y", "y_low", "y_high"), _arrays("errorBarXY")),
            (("series_name", "t", "x", "x_low", "x_high", "y", "y_low", "y_high"), error_xy),
            (("series_name", "t", "x", "x_low", "x_high", "y", "y_low", "y_high", "by"), error_xy),
        ]
        return self._exactly(left, rules, args)

    def plot_xy_hist(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        x: Union[str, List[int], List[float], List[Instant]] = None,
        xmin: float = None,
        xmax: float = None,
        nbins: int = None,
    ) -> "Figure":
        """Plots a histogram of a column: the counts of its values in `nbins` bins of one
        width."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        args = dict(series_name=series_name, t=t, x=x, xmin=xmin, xmax=xmax, nbins=nbins)
        types = dict(series_name=(str,), t=_TABLE, x=_DATA, xmin=(float,), xmax=(float,), nbins=(int,))
        left = _set_of(args, types)

        def counted(edit, *values):
            raise NotPorted("io.deephaven.plot.Figure#histPlot(Comparable, Table)", "a table of counted bins")

        def hist(edit, name, table, x, nbins):
            edit.j_hist(name, _table(table, "histPlot"), _name(x, "histPlot"), nbins)

        def hist_range(edit, name, table, x, xmin, xmax, nbins):
            edit.j_hist(name, _table(table, "histPlot"), _name(x, "histPlot"), nbins, xmin, xmax)

        rules = [
            (("series_name", "t"), counted),
            (("series_name", "x", "nbins"), _arrays("histPlot")),
            (("series_name", "t", "x", "nbins"), hist),
            (("series_name", "x", "xmin", "xmax", "nbins"), _arrays("histPlot")),
            (("series_name", "t", "x", "xmin", "xmax", "nbins"), hist_range),
        ]
        return self._exactly(left, rules, args)

    def plot_cat(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
        y: Union[str, List[int], List[float], List[Instant]] = None,
        y_low: Union[str, List[int], List[float], List[Instant]] = None,
        y_high: Union[str, List[int], List[float], List[Instant]] = None,
        by: List[str] = None,
    ) -> "Figure":
        """Plots the last value of a column for each category: bars by default."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        args = dict(series_name=series_name, t=t, category=category, y=y, y_low=y_low, y_high=y_high, by=by)
        types = dict(series_name=(str,), t=_TABLE, category=_DATA, y=_DATA, y_low=_DATA, y_high=_DATA, by=(list,))
        left = _set_of(args, types)

        def cat(edit, name, table, category, y, by=None):
            edit.j_cat(name, _table(table, "catPlot"), _name(category, "catPlot"), _name(y, "catPlot"), by=by)

        def cat_error(edit, name, table, category, y, y_low, y_high):
            java = "catErrorBar"
            columns = [_name(column, java) for column in (category, y, y_low, y_high)]
            edit.j_cat(name, _table(table, java), *columns)

        def cat_error_by(edit, *values):
            raise NotPorted(
                "io.deephaven.plot.Figure#catErrorBarBy", "Java makes the series and tells a client an error for it"
            )

        rules = [
            (("series_name", "category", "y"), _arrays("catPlot")),
            (("series_name", "t", "category", "y"), cat),
            (("series_name", "t", "category", "y", "by"), cat),
            (("series_name", "category", "y", "y_low", "y_high"), _arrays("catErrorBar")),
            (("series_name", "t", "category", "y", "y_low", "y_high"), cat_error),
            (("series_name", "t", "category", "y", "y_low", "y_high", "by"), cat_error_by),
        ]
        return self._exactly(left, rules, args)

    def plot_cat_hist(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
    ) -> "Figure":
        """Plots the number of rows of each category."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        args = dict(series_name=series_name, t=t, category=category)
        left = _set_of(args, dict(series_name=(str,), t=_TABLE, category=_DATA))

        def cat_hist(edit, name, table, category):
            edit.j_cat_hist(name, _table(table, "catHistPlot"), _name(category, "catHistPlot"))

        rules = [
            (("series_name", "category"), _arrays("catHistPlot")),
            (("series_name", "t", "category"), cat_hist),
        ]
        return self._exactly(left, rules, args)

    def plot_ohlc(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        x: Union[str, List[Instant]] = None,
        open: Union[str, List[int], List[float], List[Instant]] = None,
        high: Union[str, List[int], List[float], List[Instant]] = None,
        low: Union[str, List[int], List[float], List[Instant]] = None,
        close: Union[str, List[int], List[float], List[Instant]] = None,
        by: List[str] = None,
    ) -> "Figure":
        """Plots open, high, low and close columns against a time column."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        args = dict(series_name=series_name, t=t, x=x, open=open, high=high, low=low, close=close, by=by)
        types = dict(
            series_name=(str,), t=_TABLE, x=_DATA, open=_DATA, high=_DATA, low=_DATA, close=_DATA, by=(list,)
        )
        left = _set_of(args, types)

        def ohlc(edit, name, table, x, open, high, low, close, by=None):
            columns = [_name(column, "ohlcPlot") for column in (x, open, high, low, close)]
            edit.j_ohlc(name, _table(table, "ohlcPlot"), *columns, by=by)

        rules = [
            (("series_name", "x", "open", "high", "low", "close"), _arrays("ohlcPlot")),
            (("series_name", "t", "x", "open", "high", "low", "close"), ohlc),
            (("series_name", "t", "x", "open", "high", "low", "close", "by"), ohlc),
        ]
        return self._exactly(left, rules, args)

    def plot_pie(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
        y: Union[str, List[int], List[float], List[Instant]] = None,
    ) -> "Figure":
        """Plots the last value of a column for each category as the slices of a pie."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        args = dict(series_name=series_name, t=t, category=category, y=y)
        left = _set_of(args, dict(series_name=(str,), t=_TABLE, category=_DATA, y=_DATA))

        def pie(edit, name, table, category, y):
            columns = [_name(column, "piePlot") for column in (category, y)]
            edit.j_cat(name, _table(table, "piePlot"), *columns, chart_type="PIE")

        rules = [
            (("series_name", "category", "y"), _arrays("piePlot")),
            (("series_name", "t", "category", "y"), pie),
        ]
        return self._exactly(left, rules, args)

    def plot_treemap(
        self,
        series_name: str,
        t: Union[Table, SelectableDataSet],
        id: str,
        parent: str,
        value: str = None,
        label: str = None,
        hover_text: str = None,
        color: str = None,
    ) -> "Figure":
        """Plots rows as nested rectangles: each row an `id`, inside the row of its
        `parent`."""
        if not series_name:
            raise DHError("required parameter is not set: series_name")
        if not t:
            raise DHError("required parameter is not set: t")
        if not id:
            raise DHError("required parameter is not set: id")
        if not parent:
            raise DHError("required parameter is not set: parent")
        args = dict(
            series_name=series_name,
            t=t,
            id=id,
            parent=parent,
            value=value,
            label=label,
            hover_text=hover_text,
            color=color,
        )
        _set_of(args, {name: (str,) for name in args} | {"t": _TABLE})
        edit = self._edit()
        edit.j_treemap(series_name, _table(t, "treemapPlot"), id, parent, value, label, hover_text, color)
        return self._done(edit)

    # -- series

    def series(
        self,
        name: str = None,
        axes: int = None,
        group: int = None,
        multi_series_key: List[Any] = None,
        color: Union[str, int, Color] = None,
        tool_tip_pattern: str = None,
        x_tool_tip_pattern: str = None,
        y_tool_tip_pattern: str = None,
        error_bar_color: Union[str, int, Color] = None,
        gradient_visible: bool = None,
        naming_function: Callable = None,
    ) -> "Figure":
        """Chooses a series of the axes by its name, or changes the chosen series."""
        args = dict(
            name=name,
            axes=axes,
            group=group,
            multi_series_key=multi_series_key,
            color=color,
            tool_tip_pattern=tool_tip_pattern,
            x_tool_tip_pattern=x_tool_tip_pattern,
            y_tool_tip_pattern=y_tool_tip_pattern,
            error_bar_color=error_bar_color,
            gradient_visible=gradient_visible,
            naming_function=naming_function,
        )
        types = dict(
            name=(str,),
            axes=(int,),
            group=(int,),
            multi_series_key=(list,),
            color=_COLOR,
            tool_tip_pattern=(str,),
            x_tool_tip_pattern=(str,),
            y_tool_tip_pattern=(str,),
            error_bar_color=_COLOR,
            gradient_visible=(bool,),
            naming_function=(_Callable,),
        )
        left = _set_of(args, types)
        _no_key(left)

        def refuse(java):
            def call(edit, *values):
                raise NotPorted(f"io.deephaven.plot.Figure#{java}")

            return call

        def set_color(edit, value):  # seriesColor(Paint), (int), (String)
            series = edit._selected_series()
            css = _color(value)
            if not series.multi:
                # a multi-series keeps the color and tells no client
                series.line_color = css
                series.point_color = css

        def set_tool_tip(edit, value):  # toolTipPattern(String)
            series = edit._selected_series()
            if not series.multi:
                series.x_tool_tip_pattern = value
                series.y_tool_tip_pattern = value

        def set_x_tool_tip(edit, value):  # xToolTipPattern(String)
            edit._selected_series().x_tool_tip_pattern = value

        def set_y_tool_tip(edit, value):  # yToolTipPattern(String)
            edit._selected_series().y_tool_tip_pattern = value

        def set_error_bar_color(edit, value):  # errorBarColor(Paint), (int), (String)
            series = edit._selected_series()
            if series.kind not in ("xyerr", "caterr") or series.multi:
                raise NotPorted(
                    "io.deephaven.plot.Figure#errorBarColor", "the color of error bars a series does not have"
                )
            # kept by Java and told to no client
            _color(value)

        def set_gradient(edit, value):  # gradientVisible(boolean)
            edit._selected_series().gradient_visible = value

        rules = [
            (("axes",), refuse("series(int)")),
            (("name",), _Edit.j_series_name),
            (("group",), refuse("group")),
            (("color",), set_color),
            (("tool_tip_pattern",), set_tool_tip),
            (("x_tool_tip_pattern",), set_x_tool_tip),
            (("y_tool_tip_pattern",), set_y_tool_tip),
            (("error_bar_color",), set_error_bar_color),
            (("gradient_visible",), set_gradient),
            (("naming_function",), refuse("seriesNamingFunction")),
        ]
        return self._apply(left, rules, args)

    def line(
        self,
        multi_series_key: List[Any] = None,
        color: Union[str, int, Color] = None,
        style: Union[str, LineStyle] = None,
        visible: int = None,
    ) -> "Figure":
        """Changes the line of the chosen series."""
        args = dict(multi_series_key=multi_series_key, color=color, style=style, visible=visible)
        types = dict(multi_series_key=(list,), color=_COLOR, style=(str, LineStyle), visible=(int,))
        left = _set_of(args, types)
        _no_key(left)

        def set_color(edit, value):  # lineColor(Paint), (int), (String)
            edit._selected_series().line_color = _color(value)

        def set_visible(edit, value):  # linesVisible(Boolean)
            edit._selected_series().lines_visible = bool(value)

        rules = [
            (("color",), set_color),
            # the style of a line is kept by Java and told to no client
            (("style",), lambda edit, value: edit._selected_series()),
            (("visible",), set_visible),
        ]
        return self._apply(left, rules, args)

    def point(
        self,
        t: Union[Table, SelectableDataSet] = None,
        category: Union[str, List[str], List[int], List[float]] = None,
        multi_series_key: List[Any] = None,
        color: Union[
            str,
            int,
            Color,
            List[str],
            List[int],
            List[Color],
            Callable,
            Dict[Any, str],
            Dict[Any, int],
            Dict[Any, Color],
        ] = None,
        label: Union[str, List[str], Callable, Dict[Any, str]] = None,
        shape: Union[str, Shape, List[str], List[Shape], Callable, Dict[Any, str], Dict[Any, Shape]] = None,
        size: Union[int, float, List[int], List[float], Callable, Dict[Any, int], Dict[Any, float]] = None,
        label_format: str = None,
        visible: int = None,
    ) -> "Figure":
        """Changes the points of the chosen series."""
        args = dict(
            t=t,
            category=category,
            multi_series_key=multi_series_key,
            color=color,
            label=label,
            shape=shape,
            size=size,
            label_format=label_format,
            visible=visible,
        )
        types = dict(
            t=_TABLE,
            category=_DATA,
            multi_series_key=(list,),
            color=(str, int, Color, list, _Callable, dict),
            label=(str, list, _Callable, dict),
            shape=(str, Shape, list, _Callable, dict),
            size=(int, float, list, _Callable, dict),
            label_format=(str,),
            visible=(int,),
        )
        left = _set_of(args, types)
        _no_key(left)
        for name in ("t", "category"):
            if name in left:
                raise NotPorted("io.deephaven.plot.Figure#pointColor(Table, String) and the like", "a value for each point")
        for name in ("color", "label", "shape", "size"):
            if isinstance(args[name], (list, dict)) or callable(args[name]):
                java = "point" + name[0].upper() + name[1:]
                raise NotPorted(f"io.deephaven.plot.Figure#{java}", "a value for each point")

        def set_color(edit, value):  # pointColor(Paint), (int), (String)
            series = edit._selected_series()
            series.point_color = _color(value)
            # Java tells a client the color text of a multi-series as its point label
            series.point_color_text = value if isinstance(value, str) else None

        def set_label(edit, value):  # pointLabel(Object)
            series = edit._selected_series()
            if not series.multi:
                series.point_label = value

        def set_shape(edit, value):  # pointShape(String), (Shape)
            series = edit._selected_series()
            if isinstance(value, Shape):
                if not series.multi:
                    raise NotPorted(
                        "io.deephaven.plot.Figure#pointShape(Shape)",
                        "Java tells a client the identity of a Java object for it; give the name of the shape",
                    )
                series.point_shape = value.value
            elif series.multi:
                series.point_shape = value
            else:
                name = value.upper()
                if name not in _SHAPES:
                    raise JavaError(
                        f"java.lang.IllegalArgumentException: Not a valid shape: `{value}`; "
                        f"valid shapes: [{', '.join(_SHAPES)}]"
                    )
                series.point_shape = name

        def set_size(edit, value):  # pointSize(int), (double)
            edit._selected_series().point_size = float(value)

        def set_format(edit, value):  # pointLabelFormat(String)
            edit._selected_series().point_label_format = value

        def set_visible(edit, value):  # pointsVisible(Boolean)
            edit._selected_series().points_visible = bool(value)

        rules = [
            (("color",), set_color),
            (("label",), set_label),
            (("label_format",), set_format),
            (("shape",), set_shape),
            (("size",), set_size),
            (("visible",), set_visible),
        ]
        return self._apply(left, rules, args)

    def func(self, xmin: float = None, xmax: float = None, npoints: int = None) -> "Figure":
        """Refused: a series of a function is not ported, so there is none to change."""
        _set_of(dict(xmin=xmin, xmax=xmax, npoints=npoints), dict(xmin=(float,), xmax=(float,), npoints=(int,)))
        raise NotPorted("io.deephaven.plot.Figure#funcRange")


def _no_key(left: set) -> None:
    if "multi_series_key" in left:
        raise NotPorted(
            "io.deephaven.plot.Figure#lineColor(Paint, Object...) and the other methods with `multiSeriesKey`",
            "a change to one series of a multi-series",
        )


def _color(value) -> Any:
    """The CSS text of a color, or None for what Java makes no color of.

    Behavior: `io.deephaven.gui.color.Color#color`, `io.deephaven.plot.util.PlotUtils#intToColor`
    """
    if isinstance(value, int) and not isinstance(value, bool) and value < 0:
        return None
    try:
        return _css_of(value)
    except ValueError as error:
        raise JavaError(str(error)) from None


def _table(value, java: str) -> Table:
    if not isinstance(value, Table):
        raise NotPorted(f"io.deephaven.plot.Figure#{java}(Comparable, SelectableDataSet, ...)")
    return value


def _name(value, java: str) -> str:
    if not isinstance(value, str):
        raise JavaError("no matching Java method overloads found")
    return value


def _errors(sources: list, by) -> list:
    """The sources of error bars, which are columns of a table and not of a multi-series."""
    for _, _, column in sources:
        _name(column, "errorBarXY")
    if by is None:
        return sources
    raise NotPorted(
        "io.deephaven.plot.Figure#errorBarXYBy, #errorBarXBy and #errorBarYBy", "error bars of a multi-series"
    )


def _arrays(java: str):
    def call(edit, *values):
        raise NotPorted(
            f"io.deephaven.plot.Figure#{java}",
            "data that is not a table; Java draws it for no client and tells the client an error",
        )

    return call


def _describe(model: _Model, entries: list):
    """The `FigureDescriptor` of a figure as protobuf JSON, with the tables and the
    partitioned tables its sources name by their places.

    Behavior: `io.deephaven.figure.FigureWidgetTranslator#translate`
    """
    # which columns each table of the figure is read for, in the order of first use
    used: Dict[int, list] = {}
    order: list = []
    for chart in model.charts:
        for axes in chart.axes:
            for series in axes.series:
                if series.table not in used:
                    used[series.table] = []
                    order.append(series.table)
                for _, _, column in series.sources:
                    if column not in used[series.table]:
                        used[series.table].append(column)
    place: Dict[int, int] = {}
    tables: list = []
    partitioned: list = []
    for entry in order:
        kind, table, key = entries[entry]
        columns = used[entry]
        if kind == "plain":
            place[entry] = len(tables)
            tables.append(table.view(columns))
        elif kind == "cat":
            (category,) = key
            values = [column for column in columns if column != category]
            place[entry] = len(tables)
            tables.append(_category_table(table, [category], values).view([category, _CAT_ORDER] + values))
        elif kind == "cathist":
            (category,) = key
            counted = table.update_view(f"{_CAT_ORDER} = k").agg_by(
                [agg.first(_CAT_ORDER), agg.count_("Count")], by=[category]
            )
            place[entry] = len(tables)
            tables.append(counted)
        elif kind == "by":
            (by,) = key
            place[entry] = len(partitioned)
            partitioned.append(table.partition_by(list(by)))
        else:
            category, by = key
            keys = [category] + list(by)
            values = [column for column in table.column_names if column not in keys]
            place[entry] = len(partitioned)
            partitioned.append(_category_table(table, keys, values).partition_by(list(by)))

    description: Dict[str, Any] = {"update_interval": model.update_interval, "cols": model.cols, "rows": model.rows}
    if model.title is not None:
        description["title"] = model.title
    if model.title_font is not None:
        description["title_font"] = model.title_font
    if model.title_color is not None:
        description["title_color"] = model.title_color
    description["charts"] = [_describe_chart(model, chart, place) for chart in model.charts]
    return description, tables, partitioned


def _describe_chart(model: _Model, chart: _Chart, place: Dict[int, int]) -> dict:
    axis_descriptions = {}
    for dim in range(2):
        first = True
        for axis in chart.axis[dim]:
            if axis.type is None:
                continue
            described: Dict[str, Any] = {
                "id": axis.id,
                "format_type": axis.type,
                "type": "XY"[dim],
                "position": (("BOTTOM", "TOP"), ("LEFT", "RIGHT"))[dim][0 if first else 1],
                "log": axis.log,
                "min_range": _number(axis.min),
                "max_range": _number(axis.max),
                "minor_ticks_visible": axis.minor_ticks_visible,
                "major_ticks_visible": axis.major_ticks_visible,
                "minor_tick_count": axis.minor_tick_count,
                "gap_between_major_ticks": _number(axis.gap),
                "tick_label_angle": _number(axis.tick_label_angle),
                "invert": axis.invert,
            }
            for name, value in (
                ("label", axis.label),
                ("label_font", axis.label_font),
                ("format_pattern", axis.format_pattern),
                ("color", axis.color),
            ):
                if value is not None:
                    described[name] = value
            if axis.tick_locations is not None:
                described["major_tick_locations"] = [_number(location) for location in axis.tick_locations]
            first = False
            axis_descriptions[axis.id] = described
    series_list = []
    multi_list = []
    for axes in chart.axes:
        ids = [chart.axis[dim][axes.axis[dim]] for dim in range(2)]
        for axis in ids:
            if axis.type is None:
                raise NotPorted(
                    "io.deephaven.figure.FigureWidgetTranslator#translate",
                    f"axes without a plot: axis {axis.id} of the chart at row {chart.row}, column "
                    f"{chart.column} has none. Java shows such a figure and fails every fetch of it",
                )
        style = axes.plot_style or _DEFAULT_STYLE[chart.type]
        for series in axes.series:
            sources = []
            for kind, dim, column in series.sources:
                source: Dict[str, Any] = {
                    "axis_id": "-1" if dim is None else ids[dim].id,
                    "type": kind,
                    "column_name": column,
                }
                if series.multi:
                    source["partitioned_table_id"] = place[series.table]
                else:
                    source["table_id"] = place[series.table]
                    source["partitioned_table_id"] = -1
                sources.append(source)
            if series.multi:
                multi_list.append(_describe_multi(series, style, sources))
            else:
                series_list.append(_describe_series(series, style, sources))
    described_chart: Dict[str, Any] = {
        "colspan": chart.colspan,
        "rowspan": chart.rowspan,
        "series": series_list,
        "multi_series": multi_list,
        # Java gives the axes in the order of a `HashMap` of their ids
        "axes": [axis_descriptions[key] for key in _hash_map_order(list(axis_descriptions))],
        "chart_type": chart.type,
        "title": chart.title or "",
        "show_legend": chart.show_legend,
        "column": chart.column,
        "row": chart.row,
    }
    for name, value in (
        ("title_font", chart.title_font),
        ("title_color", chart.title_color),
        ("legend_font", chart.legend_font),
        ("legend_color", chart.legend_color),
    ):
        if value is not None:
            described_chart[name] = value
    return described_chart


def _describe_series(series: _Series, style: str, sources: list) -> dict:
    described: Dict[str, Any] = {
        "plot_style": style,
        "name": series.name,
        "gradient_visible": bool(series.gradient_visible),
        "data_sources": sources,
    }
    size = series.point_size
    if size is None and series.kind in ("cat", "caterr", "treemap"):
        # a category series has a size that is no number until one is set
        size = math.nan
    for name, value in (
        ("lines_visible", series.lines_visible),
        ("shapes_visible", series.points_visible),
        ("line_color", series.line_color),
        ("point_label_format", series.point_label_format),
        ("x_tool_tip_pattern", series.x_tool_tip_pattern),
        ("y_tool_tip_pattern", series.y_tool_tip_pattern),
        ("shape_label", series.point_label),
        ("shape_size", None if size is None else _number(size)),
        ("shape_color", series.point_color),
        ("shape", series.point_shape),
    ):
        if value is not None:
            described[name] = value
    return described


def _describe_multi(series: _Series, style: str, sources: list) -> dict:
    def default(name: str, value) -> dict:
        return {} if value is None else {name: value}

    return {
        "plot_style": style,
        "name": series.name,
        "line_color": default("default_string", series.line_color),
        "point_color": default("default_string", series.point_color),
        "lines_visible": default("default_bool", series.lines_visible),
        "points_visible": default("default_bool", series.points_visible),
        "gradient_visible": default("default_bool", series.gradient_visible),
        "point_label_format": default("default_string", series.point_label_format),
        "x_tool_tip_pattern": default("default_string", series.x_tool_tip_pattern),
        "y_tool_tip_pattern": default("default_string", series.y_tool_tip_pattern),
        # Java writes the text of the point color here, "null" when there is none
        # (`FigureWidgetTranslator` reads `pointColorSeriesNameToStringMap` for the label)
        "point_label": {"default_string": "null" if series.point_color_text is None else series.point_color_text},
        "point_size": default("default_double", None if series.point_size is None else _number(series.point_size)),
        "point_shape": default("default_string", series.point_shape),
        "data_sources": [
            {key: value for key, value in source.items() if key != "table_id"} for source in sources
        ],
    }
