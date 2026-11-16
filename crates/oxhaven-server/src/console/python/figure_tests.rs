//! Tests of `deephaven.plot`: the figures of the Python console.
//!
//! `fixtures/figures-java-42.9.jsonl` is what Java 42.9 answered to each piece of plotting
//! code: the error of the command, or the bytes of the `FigureDescriptor` a client fetched
//! and what the references of the fetch held. The same code is run here and must give the
//! same; `docs/FIGURES.md` says how the file was recorded and what is allowed to differ.

use std::collections::BTreeMap;

use oxhaven_proto::script::FigureDescriptor;
use prost::Message;
use serde_json::Value;

use super::tests::Python;
use crate::figure::{figure_of, Figure};

const JAVA: &str = include_str!("fixtures/figures-java-42.9.jsonl");

/// What the recorded code names: the tables and the imports.
const SETUP: &str = r#"
from deephaven import empty_table
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle
from deephaven.plot import Font, FontStyle, Shape, DecimalAxisFormat, NanosAxisFormat, LineStyle, Color, Colors
t = empty_table(20).update(["X = i", "Y = i * 2.0", "Z = (double)(i % 5)", "S = i % 2 == 0 ? `a` : `b`", "G = i % 3", "T = '2024-01-01T00:00:00Z' + i * SECOND", "O = 1.0 + i", "H = 3.0 + i", "L = 0.5 + i", "C = 2.0 + i"])
u = empty_table(5).update(["X = i", "W = i * 3.0"])
"#;

/// Writes what the tables of `fig` hold to a file, as the recording did for Java: per
/// table, the text of every value by column, and nothing for a column of instants.
const DUMP: &str = r#"
def _dump_figure(fig, path):
    import json
    from deephaven.table import Table
    tables = []
    for handle in fig.j_object.tables():
        table = Table(handle)
        held = {}
        for column in table.columns:
            held[column.name] = None if column.data_type.j_name == "java.time.Instant" else []
        for row in table.iter_dict():
            for name, value in row.items():
                if held[name] is not None:
                    held[name].append(str(value))
        tables.append(held)
    with open(path, "w") as out:
        json.dump(tables, out)
"#;

/// The line of an error text that names what failed: the last that names an error or an
/// exception and is no frame of a Java stack, as the recording took it.
fn error_line(text: &str) -> String {
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with("\tat "))
        .collect();
    let interesting = lines
        .iter()
        .rev()
        .find(|line| line.contains("Error") || line.contains("Exception"));
    interesting
        .or(lines.last())
        .map(|line| line.trim().to_string())
        .unwrap_or_default()
}

fn run_case(python: &Python, code: &str) -> Option<String> {
    let code = if code.contains('\n') {
        format!("fig = None\n{code}")
    } else {
        format!("fig = None\nfig = {code}")
    };
    python.console.evaluate(&code).error
}

fn shown(python: &Python) -> Figure {
    let value = python.server.state.scope.get("fig").expect("fig is set");
    figure_of(&value).expect("fig is a figure").clone()
}

/// What the tables of the scope's `fig` hold, as [`DUMP`] writes it.
fn dumped(python: &Python, name: &str) -> Vec<Value> {
    let path =
        std::env::temp_dir().join(format!("oxhaven-figure-{}-{name}.json", std::process::id()));
    python.run(&format!("_dump_figure(fig, {:?})", path.to_str().unwrap()));
    let text = std::fs::read_to_string(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    serde_json::from_str(&text).unwrap()
}

/// Java's description with every table named by the place the same table has here.
/// Java numbers the tables of a figure in the order of a map keyed by identity, which
/// changes from run to run; here they are numbered as the plots first use them.
fn renumbered(java: &FigureDescriptor, java_tables: &[Value], ours: &[Value]) -> FigureDescriptor {
    let mut taken = vec![false; ours.len()];
    let places: Vec<i32> = java_tables
        .iter()
        .map(|table| {
            let place = (0..ours.len())
                .find(|&place| !taken[place] && &ours[place] == table)
                .unwrap_or_else(|| panic!("no table here holds {table}; here: {ours:?}"));
            taken[place] = true;
            place as i32
        })
        .collect();
    let mut out = java.clone();
    for chart in &mut out.charts {
        for series in &mut chart.series {
            for source in &mut series.data_sources {
                if !source.column_name.is_empty() {
                    source.table_id = places[source.table_id as usize];
                }
            }
        }
    }
    out
}

/// The cases Java answered with a figure and Oxhaven refuses, each with the refusal;
/// `docs/FIGURES.md` lists them for the owner.
const REFUSED: &[(&str, &str)] = &[(
    "e_chart_opts",
    "deephaven._java_time.NotPorted: not ported: io.deephaven.plot.Figure#plotOrientation: a horizontal chart",
)];

#[tokio::test]
async fn plotting_code_gives_what_java_gave() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(DUMP);
    let mut figures = 0;
    let mut errors = 0;
    let mut refusals = 0;
    let mut wrong: BTreeMap<String, String> = BTreeMap::new();
    for line in JAVA.lines() {
        let record: Value = serde_json::from_str(line).unwrap();
        let name = record["name"].as_str().unwrap();
        let code = record["code"].as_str().unwrap();
        let error = run_case(&python, code);
        if let Some(expected) = record.get("error") {
            errors += 1;
            let got = error.as_deref().map(error_line);
            if got.as_deref() != expected.as_str() {
                wrong.insert(name.to_string(), format!("java: {expected}\nhere: {got:?}"));
            }
            continue;
        }
        if let Some((_, refusal)) = REFUSED.iter().find(|(refused, _)| *refused == name) {
            refusals += 1;
            let got = error
                .as_deref()
                .and_then(|text| text.trim_end().lines().last());
            if got != Some(*refusal) {
                wrong.insert(name.to_string(), format!("not refused: {got:?}"));
            }
            continue;
        }
        if let Some(error) = error {
            wrong.insert(name.to_string(), format!("failed: {error}"));
            continue;
        }
        figures += 1;
        let figure = shown(&python);
        let java_tables = record["tables"].as_array().unwrap();
        let ours = dumped(&python, name);
        if java_tables.len() != ours.len() {
            wrong.insert(
                name.to_string(),
                format!("java has {} tables, here {}", java_tables.len(), ours.len()),
            );
            continue;
        }
        let bytes: Vec<u8> = {
            let hex = record["data"].as_str().unwrap();
            (0..hex.len() / 2)
                .map(|at| u8::from_str_radix(&hex[2 * at..2 * at + 2], 16).unwrap())
                .collect()
        };
        let java = FigureDescriptor::decode(&bytes[..]).unwrap();
        // the fields of this build of the protocol are all that Java sent
        assert_eq!(java.encode_to_vec(), bytes, "{name}");
        let java = renumbered(&java, java_tables, &ours);
        // compared as bytes: a size that is not a number is not equal to itself
        if java.encode_to_vec() != figure.descriptor().encode_to_vec() {
            wrong.insert(
                name.to_string(),
                format!("java: {java:#?}\nhere: {:#?}", figure.descriptor()),
            );
            continue;
        }
        let partitioned: Vec<Value> = figure
            .partitioned()
            .iter()
            .map(|table| {
                let columns: Vec<&str> = table
                    .constituent_definition()
                    .columns()
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                serde_json::json!({"keys": table.key_columns(), "columns": columns})
            })
            .collect();
        if &partitioned != record["partitioned"].as_array().unwrap() {
            wrong.insert(
                name.to_string(),
                format!("java: {}\nhere: {partitioned:?}", record["partitioned"]),
            );
        }
    }
    let report: Vec<String> = wrong
        .iter()
        .map(|(name, what)| format!("##### {name}\n{what}"))
        .collect();
    assert!(wrong.is_empty(), "{}", report.join("\n"));
    // the file was read: these are the counts of its records
    assert_eq!((figures, errors, refusals), (132, 40, REFUSED.len()));
}

/// Plotting code that is refused, with what the refusal names: the Java method that is
/// not ported and, after a colon, which use of it.
const NOT_PORTED: &[(&str, &str)] = &[
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").x_axis(business_time=True).show()"#,
        "io.deephaven.plot.Figure#xBusinessTime",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y", by=["S"]).line(color="RED", multi_series_key=["a"]).show()"#,
        "io.deephaven.plot.Figure#lineColor(Paint, Object...) and the other methods with `multiSeriesKey`: a change to one series of a multi-series",
    ),
    (
        r#"Figure().plot_cat(series_name="c", t=t, category="S", y="Y", y_low="L", y_high="H", by=["G"]).show()"#,
        "io.deephaven.plot.Figure#catErrorBarBy: Java makes the series and tells a client an error for it",
    ),
    (
        r#"Figure().new_axes(dim=1).plot_xy(series_name="s", t=t, x="X", y="Y").show()"#,
        "io.deephaven.plot.Figure#newAxes(int): axes that do not have two dimensions",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y", y_low="L", y_high="H", by=["S"]).show()"#,
        "io.deephaven.plot.Figure#errorBarXYBy, #errorBarXBy and #errorBarYBy: error bars of a multi-series",
    ),
    (
        r#"Figure().plot_xy(series_name="s", function=lambda x: x * 2).show()"#,
        "io.deephaven.plot.Figure#plot(Comparable, DoubleUnaryOperator)",
    ),
    (
        r#"Figure().plot_xy_hist(series_name="h", t=t, x="Y", nbins=0).show()"#,
        "io.deephaven.plot.Figure#histPlot: a histogram of no bins, for which Java computes bins without bounds",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y", by=["S"]).series(naming_function=lambda key: "n").show()"#,
        "io.deephaven.plot.Figure#seriesNamingFunction",
    ),
    (
        "Figure(rows=1, cols=2).new_chart(row=0, col=0).new_chart(row=0, col=0).show()",
        "io.deephaven.plot.ChartArray#addChart: a chart where the figure has one. Java keeps both charts in the figure it sends, or fails with an IndexOutOfBoundsException",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").point(color=["RED", "BLUE"]).show()"#,
        "io.deephaven.plot.Figure#pointColor: a value for each point",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").point(shape=Shape.SQUARE).show()"#,
        "io.deephaven.plot.Figure#pointShape(Shape): Java tells a client the identity of a Java object for it; give the name of the shape",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").series(axes=0).show()"#,
        "io.deephaven.plot.Figure#series(int)",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").series(error_bar_color="RED", group=2).show()"#,
        "io.deephaven.plot.Figure#group",
    ),
    (
        r#"Figure().x_twin().plot_xy(series_name="b", t=t, x="X", y="Z").show()"#,
        "io.deephaven.figure.FigureWidgetTranslator#translate: axes without a plot: axis Y0 of the chart at row 0, column 0 has none. Java shows such a figure and fails every fetch of it",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").new_axes().show()"#,
        "io.deephaven.figure.FigureWidgetTranslator#translate: axes without a plot: axis X1 of the chart at row 0, column 0 has none. Java shows such a figure and fails every fetch of it",
    ),
    (
        r#"shown = Figure().plot_xy(series_name="s", t=t, x="X", y="Y").show()
fig = Figure(j_figure=shown.j_object).plot_xy(series_name="b", t=t, x="X", y="Z")"#,
        "io.deephaven.plot.Figure: changes to a figure that reached Python as an object of the server; what it plots is not kept",
    ),
    (
        r#"Figure().plot_cat(series_name="c", category=["a", "b"], y=[1.0, 2.0]).show()"#,
        "io.deephaven.plot.Figure#catPlot: data that is not a table; Java draws it for no client and tells the client an error",
    ),
    (
        r#"Figure().plot_cat_hist(series_name="c", category=["a", "b"]).show()"#,
        "io.deephaven.plot.Figure#catHistPlot: data that is not a table; Java draws it for no client and tells the client an error",
    ),
    (
        r#"Figure().plot_xy(series_name="s", x=[1, 2], x_low=[0, 1], x_high=[2, 3], y=[1.0, 2.0]).show()"#,
        "io.deephaven.plot.Figure#errorBarX: data that is not a table; Java draws it for no client and tells the client an error",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").func(xmin=0.0, xmax=1.0).show()"#,
        "io.deephaven.plot.Figure#funcRange",
    ),
    (
        r#"Figure().plot_xy_hist(series_name="h", x=[1.0, 2.0], nbins=2).show()"#,
        "io.deephaven.plot.Figure#histPlot: data that is not a table; Java draws it for no client and tells the client an error",
    ),
    (
        r#"Figure().plot_ohlc(series_name="o", x=[1, 2], open=[1.0, 2.0], high=[1.0, 2.0], low=[1.0, 2.0], close=[1.0, 2.0]).show()"#,
        "io.deephaven.plot.Figure#ohlcPlot: data that is not a table; Java draws it for no client and tells the client an error",
    ),
    (
        r#"Figure().plot_pie(series_name="p", category=["a", "b"], y=[1.0, 2.0]).show()"#,
        "io.deephaven.plot.Figure#piePlot: data that is not a table; Java draws it for no client and tells the client an error",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").point(label=["a", "b"]).show()"#,
        "io.deephaven.plot.Figure#pointLabel: a value for each point",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").point(size=lambda x: 1).show()"#,
        "io.deephaven.plot.Figure#pointSize: a value for each point",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").point(t=t, color="S").show()"#,
        "io.deephaven.plot.Figure#pointColor(Table, String) and the like: a value for each point",
    ),
    (
        r#"Figure(rows=1, cols=2).new_chart().plot_xy(series_name="a", t=t, x="X", y="Y").new_chart().plot_xy(series_name="b", t=t, x="X", y="Z").chart(index=0).chart(col_span=2).show()"#,
        "io.deephaven.plot.ChartArray#resizeChart: a chart that grows over another chart",
    ),
    (
        r#"Figure().plot_xy(series_name="s", x=[1, 2, 3], y=[2.0, 4.0, 6.0]).show()"#,
        "io.deephaven.plot.Figure#plot: data that is not a table; Java draws it for no client and tells the client an error",
    ),
];

/// Plotting code that fails, with the last line of the error. Java 42.9 ends the first
/// with `Exception message         : Cannot find variable or class Nope`, repeats the
/// message of a `DHError` after ` : `, and words the others the same.
const FAILS: &[(&str, &str)] = &[
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y", by=["Nope"]).show()"#,
        "RuntimeError: Cannot find variable or class Nope",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").y_axis(min=1.5).x_axis(max=7).x_axis().show()"#,
        "deephaven.dherror.DHError: Improper input type: name=max type=<class 'int'> supported=['str', 'float']",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").y_axis(t=t, min="X").show()"#,
        "RuntimeError: no matching Java method overloads found",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").save(path="/tmp/x.png")"#,
        "RuntimeError: java.lang.UnsupportedOperationException: class io.deephaven.plot.BaseFigureImpl does not implement save",
    ),
    (
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").x_axis(min="X").show()"#,
        "RuntimeError: no matching Java method overloads found",
    ),
    (
        r#"Figure().plot_xy(series_name=None, t=t, x="X", y="Y").show()"#,
        "deephaven.dherror.DHError: required parameter is not set: series_name",
    ),
];

fn last_line(python: &Python, code: &str) -> Option<String> {
    let error = run_case(python, code)?;
    error.trim_end().lines().last().map(str::to_string)
}

#[tokio::test]
async fn what_is_not_ported_is_refused_by_its_java_method() {
    let python = Python::start().await;
    python.run(SETUP);
    for (code, refusal) in NOT_PORTED {
        assert_eq!(
            last_line(&python, code),
            Some(format!(
                "deephaven._java_time.NotPorted: not ported: {refusal}"
            )),
            "{code}"
        );
        // nothing was shown
        assert!(python.server.state.scope.get("fig").is_none(), "{code}");
    }
    for (code, error) in FAILS {
        assert_eq!(last_line(&python, code).as_deref(), Some(*error), "{code}");
    }
    // the same calls with what is ported are not refused
    for code in [
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").chart(orientation="vertical").show()"#,
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").point(shape="square").show()"#,
        r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y").x_axis(log=True).show()"#,
        r#"Figure().new_axes(dim=2).plot_xy(series_name="s", t=t, x="X", y="Y").show()"#,
        r#"Figure().plot_xy_hist(series_name="h", t=t, x="Y", nbins=1).show()"#,
    ] {
        assert_eq!(last_line(&python, code), None, "{code}");
        assert!(python.server.state.scope.get("fig").is_some(), "{code}");
    }
}

/// Observed on Java 42.9: a figure that was not shown is no variable a client is told of,
/// and what is made from a shown figure is not shown.
#[tokio::test]
async fn only_a_shown_figure_is_a_variable_of_the_session() {
    let python = Python::start().await;
    python.run(SETUP);
    let figures = |code: &str| -> Vec<String> {
        let changes = python.run(code);
        let listed = changes.created.iter().chain(&changes.updated);
        listed
            .filter(|variable| variable.type_name == "Figure")
            .map(|variable| variable.name.clone())
            .collect()
    };
    let plot = r#"Figure().plot_xy(series_name="s", t=t, x="X", y="Y")"#;
    assert_eq!(figures(&format!("unshown = {plot}")), [] as [&str; 0]);
    assert_eq!(figures(&format!("shown = {plot}.show()")), ["shown"]);
    assert_eq!(
        figures(r#"more = shown.plot_xy(series_name="s2", t=t, x="X", y="Z")"#),
        [] as [&str; 0]
    );
    assert_eq!(figures("both = more.show()"), ["both"]);
    for name in ["unshown", "more"] {
        let value = python.server.state.scope.get(name);
        assert!(
            value.is_none_or(|value| figure_of(&value).is_none()),
            "{name}"
        );
    }

    let series = |name: &str| -> Vec<String> {
        let value = python.server.state.scope.get(name).unwrap();
        let figure = figure_of(&value).unwrap();
        let chart = &figure.descriptor().charts[0];
        chart
            .series
            .iter()
            .map(|series| series.name.clone())
            .collect()
    };
    // a figure is not changed by what is made from it
    assert_eq!(series("shown"), ["s"]);
    assert_eq!(series("both"), ["s", "s2"]);
    // both series read the one table
    let value = python.server.state.scope.get("both").unwrap();
    assert_eq!(figure_of(&value).unwrap().tables().len(), 1);
    // the handle of the server is that of a shown figure only
    python.run("assert shown.j_object is not None and unshown.j_object is None");
}
