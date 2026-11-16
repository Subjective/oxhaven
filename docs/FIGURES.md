# Figures

A figure is what a script plots: charts in a grid, the axes of each chart and the series of
each pair of axes. A client fetches it as an object of type `Figure` and draws it. This file
is the design of task 7.11 and the record of what Java 42.9 was seen to do.

## Decision

`tasks/7.11-figures.md` asked whether the legacy `deephaven.plot` API is worth building.
It is built, for the plots whose data is a table. It needs no part of Java's `Plot` on the
server: the model of a figure is a few hundred lines of Python, and the server only checks
and serves the result. `deephaven.plot.express` is the other route and stays open: it
needs object types (7.10) and a JS plugin, see [the trial](#the-plugin-route-a-trial).

## Parts

| Part | Where | Does |
| --- | --- | --- |
| The package | `crates/oxhaven-server/python/deephaven/plot/` | `Figure` and the value types its methods take (`PlotStyle`, `Color`, `Colors`, `Font`, `FontStyle`, `LineStyle`, `Shape`, the axis formats). Keeps the model, makes the tables the series read, writes the description |
| The handle | `console/python/figure.rs` | `_oxhaven.figure(description, tables, partitioned)`: reads the description, which Python writes as the JSON of the protocol message, and makes the server's figure |
| The value | `figure/mod.rs` | `FigureWidget`: a `FigureDescriptor` with the tables and partitioned tables its sources name by place. Refuses a source that names a place or a column that is not there |
| The object type | `figure/mod.rs`, `FigureType` | fetch-only: the bytes of the descriptor, and as references the tables and then the partitioned tables |

A figure is immutable: every method of `Figure` returns a new figure. Only `show()` makes a
server object, so only a shown figure is a variable a client is told of, as in Java. A shown
figure that comes back to Python from the server (a reference of a plugin, for one) has no
model, and a change to it is refused.

## How `figure.py` is produced

The reference generates its `figure.py` from the Java `Figure` interface: each method takes
every parameter of a group of Java methods and chooses the Java calls by the parameters that
are set. That file is under the Deephaven Community License (`docs/CONSOLE.md`, "Licence"),
so nothing of it is copied, and the same line is followed as for the rest of the package:
names, signatures and the texts of errors are reproduced, and the code and the docstrings
are written here.

Ours is written by hand. Each method has a table of rules, a set of parameters with the
Java call it stands for, in the order the reference tries them; the two errors of the
reference's dispatch (`unsupported parameter combination: {...}` and
`Improper input type: name=... type=... supported=[...]`) come from one helper each. What a
Java call does to the figure is written from what Java 42.9 answered, below.

## What was observed on Java 42.9

Recorded with scratch scripts against a Java server with a Python console: each piece of
plotting code was run with `ExecuteCommand`, the figure fetched with `FetchObject`, the
descriptor decoded, and every referenced table read with `DoGet` (a partitioned table: its
descriptor). 173 of the records are the fixture of the tests,
`crates/oxhaven-server/src/console/python/fixtures/figures-java-42.9.jsonl`: per line the
name of the case, its code, and the last line of the error or the bytes of the descriptor
with what the tables held. `oracle/` is not touched.

The cursor. A figure has a current chart, current axes, a current axis and a current series
(`FigureImpl#make`). A plot method makes its series in the current axes, making the chart
and the axes if there is none; `new_chart`, `new_axes`, `x_twin`, `y_twin`, `chart`, `axes`,
`axis`, `series` move the cursor.

The descriptor.

- `update_interval` is 1000; `title` is told when set; the grid is `rows` and `cols`.
- A chart has `title: ""` unless set, `show_legend`, its place and its spans. Its type is
  that of its first plot: XY, CATEGORY, PIE (a pie), TREEMAP. Another type in the same chart
  fails: `Attempting to create inconsistent plot types: XY, CATEGORY`. `plot_ohlc` makes an XY
  chart whose axes have the style OHLC.
- The axes of a chart are told in the order of a `java.util.HashMap` of their ids: `Y0`,
  `X0`, `Y1`, `X1`, `Y2`, `X2`. The first X axis is at the BOTTOM and later ones at the
  TOP, the first Y axis at the LEFT and later ones at the RIGHT. Every axis has
  `format_type: NUMBER`; a client knows a time axis by the `column_type` of its sources,
  `java.time.Instant`.
- The style of a series is that of its axes: LINE for XY, BAR for a category chart,
  HISTOGRAM after a histogram, which so changes every series of those axes.
- A series colour is both `line_color` and `shape_color`; a point colour is `shape_color`.
  A category series has `shape_size: nan`. The name of a shape is upper-cased and checked.
- `ticks(gap)` shows major ticks, `ticks_minor(n)` minor ones. A font is told as its family.
- Kept by Java and told to no client: an axis `format`, the grid flags of a chart, the label
  format of a pie, a line style, `max_rows` of a title, the orientation of a chart, the font
  of ticks, and for a multi-series its colour, its tool tip pattern and its point labels.
- A multi-series (`by`) carries a map of defaults per setting. Its `point_label` default is
  the text of the point colour, or `"null"`.
- A removed chart frees its cells and stays in the figure that is sent.

The tables.

- An XY series reads its source table as it is, through a view of the columns the figure
  uses. Columns of short, int, long, float, double and Instant are numeric; byte, char,
  Boolean and String fail with `Unsupported numeric data type: columnName=... type=...`.
- A category series reads `update_view("__CAT_ORDER = k").agg_by([first(__CAT_ORDER),
  last(values)], by=category)`, and a category histogram the count per category.
- A histogram reads a table of bins computed once
  (`HistogramCalculator`/`DiscretizedRangeEqual`): the rows are the bins in the order rows
  first fell into them.
- Series on the same table share a table; two `by` plots on the same table and keys share
  a partitioned table.

Errors. A plot's error is `Plot Information: [Figure: F. ]Chart: T. [Series: s. ]` and the
text. `show()` of a figure with a chart that has no plot fails with `No plots created yet.`

## Differences

| What | Java | Here |
| --- | --- | --- |
| The order of the tables of a figure | that of a map keyed by identity: it changes from run to run | the order the plots first use them; the tests renumber Java's |
| The order of the columns of a table a figure reads | that of a `HashSet` | the order of first use |
| The `__SERIES_NAME<hash>` column of the constituents of a multi-series | present | absent: no client reads it |
| Axes without a plot (`new_axes()` and no plot, a twin of nothing) | `show()` succeeds, and every fetch fails with `INVALID_ARGUMENT` (a `NullPointerException` in `FigureWidgetTranslator#translate`) | `show()` is refused |
| A chart where the figure has one (`new_chart(row, col)` twice) | both charts are sent, or an `IndexOutOfBoundsException` | refused |
| The text of a `DHError` | `message : message`, then the trace | the message once (the package's `DHError`, not this task's) |
| A `by` column that is not in the table | `Exception message         : Cannot find variable or class Nope` after a Java trace | `RuntimeError: Cannot find variable or class Nope` |

## Refused

Each is a `NotPorted` error that names the Java method.

| What | Java method | Java |
| --- | --- | --- |
| Data that is not a table: lists and arrays for `plot_xy`, `plot_xy_hist`, `plot_cat`, `plot_cat_hist`, `plot_ohlc`, `plot_pie`, error bars | `Figure#plot`, `#errorBarX`, `#errorBarY`, `#errorBarXY`, `#histPlot`, `#catPlot`, `#catErrorBar`, `#catHistPlot`, `#ohlcPlot`, `#piePlot` | draws it for no client: the descriptor has a source without a table and the error `OpenAPI does not presently support source...` |
| A function plot, and its range | `Figure#plot(Comparable, DoubleUnaryOperator)`, `#funcRange` | the same |
| A `SelectableDataSet` as the table | the overloads `(Comparable, SelectableDataSet, ...)`; `Selectables#oneClick` | one-click filters |
| A table of counted bins | `Figure#histPlot(Comparable, Table)` | |
| A histogram of fewer than one bin | `Figure#histPlot` | computes bins without bounds |
| Error bars of a multi-series | `Figure#errorBarXBy`, `#errorBarYBy`, `#errorBarXYBy`, `#catErrorBarBy` | makes the series and tells a client an error for it |
| A change to one series of a multi-series (`multi_series_key`) | `Figure#lineColor(Paint, Object...)` and the like | |
| `series(axes=...)`, `group`, `naming_function` | `Figure#series(int)`, `#group`, `#seriesNamingFunction` | |
| The colour of error bars a series does not have | `Figure#errorBarColor` | |
| A `Shape` object for the points of one series | `Figure#pointShape(Shape)` | tells the client `io.deephaven.gui.shape.JShapes@<identity>`; the name of the shape as a string works |
| A value for each point: lists, dicts, functions, and a table with a column, for colour, label, shape and size | `Figure#pointColor`, `#pointLabel`, `#pointShape`, `#pointSize` and their `(Table, String)` forms | |
| Business time and transforms of an axis | `Figure#businessTime`, `#transform`; `AxisTransforms` | |
| A horizontal chart | `Figure#plotOrientation` | keeps it and tells no client: the descriptor is that of the vertical chart. Refused so that a script is not shown a chart it did not ask for; `"vertical"` is accepted |
| A title read from a table | `Figure#chartTitle(Table, String...)` | |
| Axes that do not have two dimensions | `Figure#newAxes(int)` | |
| A chart on an occupied cell; a chart that grows over another | `ChartArray#addChart`, `ChartArray#resizeChart` | see Differences |
| The font families of the machine | `Font#availableFontFamilies` | |
| A change to a figure that reached Python as a server object | `Figure` | the model is not kept with the object |

`save` raises Java's own `UnsupportedOperationException: class
io.deephaven.plot.BaseFigureImpl does not implement save`, and `x_axis(min="X")` Java's
`no matching Java method overloads found`.

Downsampling of the series of a chart is `RunChartDownsample` of the table service and no
part of this.

## In the web IDE

Checked with a Playwright script against both servers, with the web client Java 42.9 serves
(`--web-root`): the figures are fetched through the JS API (`getObject`, `subscribe`, the
arrays of every source) and opened in `/iframe/chart/`.

| Figure | Java and Oxhaven |
| --- | --- |
| XY line, scatter | the same JS figure; one `scattergl` trace of 20 points |
| Category bars | a `bar` trace of 2 |
| Histogram | a `bar` trace of 5 |
| OHLC | an `ohlc` trace of 20 |
| Pie | a `pie` trace of 2 |
| Two charts, a twin axis, a time axis, a figure title | three `scattergl` traces |
| `by` | two `scattergl` traces of 10, `by: a` and `by: b` |

The series of a `by` plot open one constituent each. The JS client finds a constituent by
uploading its key as a table of one row, through `BrowserFlightService/OpenDoPut` and
`NextDoPut`, on every page and transport. The server did not have that service; its `DoPut`
pair is added with this task (`flight/browser.rs`), and without it the chart stays empty.

## For the owner

Java behaviours that look like defects; each is reproduced or refused as the tables say:

- The `point_label` default of a multi-series is the text of its point colour.
- `pointShape(Shape)` on one series sends the identity string of a Java object.
- A figure with axes that have no plot is shown and can never be fetched.
- A removed chart is still sent.
- A histogram of no bins has bins without bounds.
- A second chart on a cell is kept beside the first, or fails with an index error.
- The tables of a figure are numbered in an order that changes from run to run.

## The plugin route: a trial

`deephaven-plugin-plotly-express` 0.22.0 and `deephaven-plugin-ui` 0.43.2 were installed
without their dependency on the reference's `deephaven-core`, in a scratch environment
beside this server, and the same steps were run on Java 42.9 with the plugin. Nothing was
fixed but the first point.

- Both plugins install their package inside `deephaven` (`deephaven/plot/express`,
  `deephaven/ui`). The package of this server is in the binary, so they were not found and
  the server did not start. The importer now looks for a module the binary does not hold in
  the directories of the package's name on `sys.path` (`_oxhaven_bootstrap.py`).
- `deephaven.plot.express` then needs three modules this server does not have:
  `deephaven.server.executors` (`submit_task`), `deephaven.calendar` (`calendar`) and `jpy`
  (`io.deephaven.time.DateTimeUtils` when it is imported;
  `io.deephaven.engine.table.impl.util.ColumnsToRowsTransform` and `jpy.array` in its
  example data). With stand-ins for the three in the scratch environment:
  - its object type `deephaven.plot.express.DeephavenFigure` is registered, and `dx.line`,
    `dx.bar` and `dx.pie` of a table make figures that are variables of that type;
  - a client that opens `dx.line` over `MessageStream` is sent what Java sends: the
    figure as JSON, with the same mapping of columns to the trace, and one `Table`
    reference;
  - `dx.scatter(..., by=...)` fails: `LivenessScope.manage` of this package refuses a
    partitioned table;
  - `dx.histogram` fails: its formula makes a Java object
    (`new io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual(...)`);
  - no JS plugin is registered, so `/js-plugins/manifest.json` lists nothing (Java lists
    `@deephaven/js-plugin-plotly-express`) and the web IDE has nothing to draw the object
    with. The refusal is not in the log: the plugin registers through a wrapper of
    `deephaven-plugin-utilities` that hides it.
- `deephaven.ui` stops at its imports: `RollupTable` and `TreeTable` of `deephaven.table`,
  `deephaven.configuration` (`get_configuration`) and `deephaven.server.executors`.

Only the imports were listed for the whole of each plugin (by reading its source); the
calls it makes on what it imports were tried for the five plots above and no further.

## Not started

- JS plugins: `CallbackAdapter#registerJsPlugin`, `/js-plugins/manifest.json` and the files
  it lists (`docs/OBJECTS.md`, "Refused").
- What the trial above lists as missing.
- `OpenDoExchange` and `OpenHandshake` of `BrowserFlightService`.
