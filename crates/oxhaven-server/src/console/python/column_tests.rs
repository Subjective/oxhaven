//! Tests of `deephaven.dtypes`, `deephaven.column`, `deephaven.time`, `new_table` and
//! `TableDefinition`. They import only the standard library (the `python-console` job
//! installs nothing); the numpy and pandas forms are script scenarios (`dtypes_*`, `time_*`).
//!
//! The expectations are what the Java reference answered to the same code, except where a
//! comment says how Java differs.

use arrow::compute::cast;
use arrow::datatypes::DataType;
use oxhaven_testkit::TableSnapshot;

use super::tests::Python;

/// `name: type = cell, ...` of each column of the scope variable `name`; a null cell is empty,
/// and an instant is its nanoseconds since the epoch.
fn described(python: &Python, name: &str) -> Vec<String> {
    let table = python.table(name);
    let snapshot = TableSnapshot::current(table.as_ref()).unwrap();
    let columns = table.definition().columns().iter();
    columns
        .zip(&snapshot.columns)
        .map(|(column, values)| {
            let values = match values.data_type() {
                DataType::Timestamp(_, _) => cast(values, &DataType::Int64).unwrap(),
                _ => values.clone(),
            };
            let cells: Vec<String> = (0..values.len())
                .map(|row| arrow::util::display::array_value_to_string(&values, row).unwrap())
                .collect();
            format!(
                "{}: {:?} = {}",
                column.name,
                column.column_type,
                cells.join(", ")
            )
        })
        .collect()
}

/// The error of code that must fail.
fn error(python: &Python, code: &str) -> String {
    let changes = python.console.evaluate(code);
    changes
        .error
        .unwrap_or_else(|| panic!("no error from {code}"))
}

#[tokio::test]
async fn new_table_builds_a_column_of_each_type_the_engine_has() {
    let python = Python::start().await;
    python.run(
        "from deephaven import new_table\n\
         from deephaven.column import *\n\
         import deephaven.dtypes as dht\n\
         t = new_table([\n\
         \x20   bool_col('Z', [True, None, 0]),\n\
         \x20   byte_col('B', [1, None, 200]),\n\
         \x20   char_col('C', [97, None, 98]),\n\
         \x20   char_col('K', 'aéz'),\n\
         \x20   short_col('S', [None, 300, 2**16 + 5]),\n\
         \x20   int_col('I', [7, None, 2**32 + 1]),\n\
         \x20   long_col('L', [2**62, None, -1]),\n\
         \x20   float_col('F', [1.5, None, 2]),\n\
         \x20   double_col('D', [None, 0.1, 3]),\n\
         \x20   string_col('T', ['x', None, '']),\n\
         \x20   datetime_col('When', ['2024-01-02T03:04:05.123456789 UTC', None, 1]),\n\
         \x20   InputColumn('A', dht.int32_array, input_data=[[1, None], None, []]),\n\
         \x20   InputColumn('N', dht.string_array, input_data=[['p', None], None, []]),\n\
         ])",
    );
    // an integer beyond its type wraps, as jpy converts it; None is the null of the type,
    // and a string is a char column of its UTF-16 units
    assert_eq!(
        described(&python, "t"),
        [
            "Z: Boolean = true, , false",
            "B: Byte = 1, , -56",
            "C: Char = 97, , 98",
            "K: Char = 97, 233, 122",
            "S: Short = , 300, 5",
            "I: Int = 7, , 1",
            "L: Long = 4611686018427387904, , -1",
            "F: Float = 1.5, , 2.0",
            "D: Double = , 0.1, 3.0",
            "T: String = x, , ",
            "When: Instant = 1704164645123456789, , 1",
            // None in an array of a primitive type is 0, as jpy converts it
            "A: Array(Int) = [1, 0], , []",
            "N: Array(String) = [p, ], , []",
        ]
    );
}

#[tokio::test]
async fn a_null_constant_in_a_column_is_a_null() {
    let python = Python::start().await;
    python.run(
        "from deephaven import new_table\n\
         from deephaven.constants import *\n\
         from deephaven.column import *\n\
         assert (NULL_BYTE, NULL_SHORT, NULL_INT, NULL_LONG) == (-128, -32768, -2**31, -2**63)\n\
         assert NULL_CHAR == 65535 and NULL_DOUBLE == -1.7976931348623157e308\n\
         assert NULL_FLOAT == -3.4028234663852886e38\n\
         t = new_table([\n\
         \x20   byte_col('B', [NULL_BYTE, 1]), short_col('S', [NULL_SHORT, 1]),\n\
         \x20   int_col('I', [NULL_INT, 1]), long_col('L', [NULL_LONG, 1]),\n\
         \x20   char_col('C', [NULL_CHAR, 1]), float_col('F', [NULL_FLOAT, 1]),\n\
         \x20   double_col('D', [NULL_DOUBLE, 1]),\n\
         ])",
    );
    assert_eq!(
        described(&python, "t"),
        [
            "B: Byte = , 1",
            "S: Short = , 1",
            "I: Int = , 1",
            "L: Long = , 1",
            "C: Char = , 1",
            "F: Float = , 1.0",
            "D: Double = , 1.0",
        ]
    );
}

#[tokio::test]
async fn new_table_fails_with_the_text_of_the_reference() {
    let python = Python::start().await;
    python.run(
        "from deephaven import new_table\n\
         from deephaven.column import *\n\
         import deephaven.dtypes as dht",
    );
    let failed = |code: &str, expected: &str| {
        let text = error(&python, code);
        assert!(text.contains(expected), "{code}: {text}");
    };
    failed(
        "new_table([int_col('Id', ['x'])])",
        "DHError: failed to create an InputColumn (Id). : failed to create a Java int array. : \
         'str' object cannot be interpreted as an integer",
    );
    failed(
        "new_table([string_col('S', [1])])",
        "DHError: failed to create an InputColumn (S).",
    );
    failed(
        "new_table([long_col('L', [2**63])])",
        "DHError: failed to create an InputColumn (L).",
    );
    failed(
        "new_table([int_col('1d', [1])])",
        "DHError: failed to create an InputColumn (1d). : \
         io.deephaven.api.util.NameValidator$InvalidNameException: Invalid column name \"1d\"",
    );
    failed(
        "new_table([int_col('A', [1]), int_col('B', [1, 2])])",
        "DHError: failed to create a new time table. : java.lang.IllegalArgumentException: \
         All columns must be the same size",
    );
    // Java builds these columns; the contracts have no column type for them
    for (code, java_type) in [
        ("pyobj_col('P', [1, 'a'])", "org.jpy.PyObject"),
        ("jobj_col('J', [None])", "java.lang.Object"),
        (
            "InputColumn('D', dht.LocalDate, input_data=[None])",
            "java.time.LocalDate",
        ),
        (
            "InputColumn('D', dht.BigDecimal, input_data=[None])",
            "java.math.BigDecimal",
        ),
        (
            "InputColumn('D', dht.Duration, input_data=[None])",
            "java.time.Duration",
        ),
    ] {
        failed(
            code,
            &format!(
                "not ported: io.deephaven.engine.table.ColumnDefinition#fromGenericType: the \
                 engine has no column type for {java_type}"
            ),
        );
    }
    assert!(python.server.state.scope.get("t").is_none());
}

#[tokio::test]
async fn a_later_column_of_a_name_replaces_the_earlier_one_in_place() {
    let python = Python::start().await;
    python.run(
        "from deephaven import new_table\n\
         from deephaven.column import *\n\
         t = new_table([int_col('A', [1]), string_col('B', ['b']), double_col('A', [2.5])])",
    );
    assert_eq!(
        described(&python, "t"),
        ["A: Double = 2.5", "B: String = b"]
    );
}

#[tokio::test]
async fn dtypes_and_column_definitions_are_named_as_java_names_them() {
    let python = Python::start().await;
    python.run(
        "import deephaven.dtypes as d\n\
         from deephaven.column import col_def, ColumnDefinition, ColumnType\n\
         from deephaven.dherror import DHError\n\
         assert [repr(x) for x in (d.bool_, d.byte, d.int8, d.short, d.int16, d.char, d.int32,\n\
         \x20   d.long, d.int64, d.float32, d.single, d.double, d.float64, d.string, d.Instant)] == [\n\
         \x20   'java.lang.Boolean', 'byte', 'byte', 'short', 'short', 'char', 'int', 'long', 'long',\n\
         \x20   'float', 'float', 'double', 'double', 'java.lang.String', 'java.time.Instant']\n\
         assert d.int64 is d.long and d.float64 is d.double\n\
         assert [repr(x) for x in (d.long_array, d.string_array, d.instant_array)] == [\n\
         \x20   '[J', '[Ljava.lang.String;', '[Ljava.time.Instant;']\n\
         assert d.int32.is_primitive and not d.string.is_primitive and not d.bool_.is_primitive\n\
         assert d.null_remap(d.int32)(None) == -2**31 and d.null_remap(d.int32)(5) == 5\n\
         assert d.null_remap(d.char)(None) == 65535\n\
         try:\n\
         \x20   d.null_remap(d.string)\n\
         \x20   raise AssertionError('null_remap of a class')\n\
         except TypeError as e:\n\
         \x20   assert str(e) == 'null_remap() must be called with a primitive DType', e\n\
         try:\n\
         \x20   d.int32(3)\n\
         \x20   raise AssertionError('a primitive type called')\n\
         except DHError as e:\n\
         \x20   assert str(e).startswith('primitive type int is not callable.'), e\n\
         a = d.array(d.int32, [1, None, 2**32 + 3])\n\
         assert list(a) == [1, 0, 3], a\n\
         assert list(d.array(d.int32, [1, None], remap=lambda v: 9 if v is None else v)) == [1, 9]\n\
         c = col_def('A', d.int32)\n\
         assert str(c) == 'ColumnDefinition {name=A, dataType=int, componentType=null, columnType=Normal}'\n\
         assert (c.name, c.data_type, c.component_type, c.column_type) == ('A', d.int32, None, ColumnType.NORMAL)\n\
         assert c == col_def('A', d.int32) and c != col_def('A', d.long) and c != col_def('B', d.int32)\n\
         assert hash(c) == hash(col_def('A', d.int32))\n\
         v = col_def('V', d.long_array)\n\
         assert str(v) == 'ColumnDefinition {name=V, dataType=class [J, componentType=long, columnType=Normal}'\n\
         assert v.component_type is d.long\n\
         p = col_def('P', d.string, column_type=ColumnType.PARTITIONING)\n\
         assert str(p).endswith('dataType=class java.lang.String, componentType=null, columnType=Partitioning}')",
    );
}

#[tokio::test]
async fn a_table_definition_is_a_mapping_of_column_definitions() {
    let python = Python::start().await;
    python.run(
        "from deephaven import new_table, empty_table\n\
         from deephaven.table import TableDefinition\n\
         from deephaven.column import col_def, int_col, string_col\n\
         from deephaven.dherror import DHError\n\
         import deephaven.dtypes as d\n\
         t = new_table([int_col('A', [1]), string_col('B', ['x'])])\n\
         td = TableDefinition({'A': d.int32, 'B': d.string})\n\
         assert t.definition == td and td == TableDefinition([col_def('A', d.int32), col_def('B', d.string)])\n\
         assert td == TableDefinition(td) and td != TableDefinition({'B': d.string, 'A': d.int32})\n\
         assert list(td) == list(td.keys()) == ['A', 'B'] and len(td) == 2\n\
         assert 'A' in td and 'C' not in td and td['B'] == col_def('B', d.string)\n\
         assert [c.name for c in td.values()] == ['A', 'B']\n\
         assert [(k, v.data_type) for k, v in td.items()] == [('A', d.int32), ('B', d.string)]\n\
         assert str(td) == ('TableDefinition {columns=[ColumnDefinition {name=A, dataType=int, '\n\
         \x20   'componentType=null, columnType=Normal}, ColumnDefinition {name=B, dataType=class '\n\
         \x20   'java.lang.String, componentType=null, columnType=Normal}]}'), str(td)\n\
         assert empty_table(1).update(['X = ii', 'Y = `a`']).definition == TableDefinition({'X': d.long, 'Y': d.string})\n\
         meta = td.table\n\
         for bad, text in ((5, \"Unexpected TableDefinitionLike type: <class 'int'>\"),):\n\
         \x20   try:\n\
         \x20       TableDefinition(bad)\n\
         \x20       raise AssertionError(bad)\n\
         \x20   except DHError as e:\n\
         \x20       assert str(e).startswith(text), e\n\
         try:\n\
         \x20   TableDefinition([col_def('A', d.int32), col_def('A', d.string)])\n\
         \x20   raise AssertionError('duplicate names')\n\
         except RuntimeError as e:\n\
         \x20   assert 'Supplied ColumnDefinitions include duplicate names [A]' in str(e), e",
    );
    assert_eq!(
        described(&python, "meta")[..2],
        [
            "Name: String = A, B",
            "DataType: String = int, java.lang.String"
        ]
    );
}

#[tokio::test]
async fn time_text_is_parsed_and_printed_as_java_does() {
    let python = Python::start().await;
    python.run(
        "import datetime\n\
         from deephaven.dherror import DHError\n\
         from deephaven.time import *\n\
         def texts(*values):\n\
         \x20   return [str(v) for v in values]\n\
         got = texts(to_j_instant('2024-03-10T01:30:00 ET'), to_j_instant('2024-01-02T03:04:05.123456789Z'),\n\
         \x20   to_j_instant('2024-01-02T03:04 UTC'),\n\
         \x20   to_j_duration('PT1H30M'), to_j_duration('PT-1.5S'), to_j_period('P1Y2M3D'), to_j_period('P2W'),\n\
         \x20   to_j_time_zone('ET'), to_j_time_zone('UTC'), to_j_time_zone('+05:30'),\n\
         \x20   to_j_local_date('2023-02-29'), to_j_local_time('12:34:56.789'), to_j_local_time('01:02'),\n\
         \x20   to_j_zdt('2024-03-10T03:30:00 ET'), to_j_zdt('2024-11-03T01:30:00 ET'))\n\
         assert got == ['2024-03-10T06:30:00Z', '2024-01-02T03:04:05.123456789Z', '2024-01-02T03:04:00Z',\n\
         \x20   'PT1H30M', 'PT-1.5S', 'P1Y2M3D', 'P14D',\n\
         \x20   'America/New_York', 'UTC', '+05:30',\n\
         \x20   '2023-02-28', '12:34:56.789', '01:02',\n\
         \x20   '2024-03-10T03:30-04:00[America/New_York]', '2024-11-03T01:30-04:00[America/New_York]'], got\n\
         assert all(f(None) is None for f in (to_j_instant, to_j_duration, to_j_period, to_j_time_zone,\n\
         \x20   to_j_local_date, to_j_local_time, to_j_zdt, to_date, to_time, to_datetime, to_timedelta))\n\
         utc = datetime.timezone.utc\n\
         got = texts(to_j_instant(datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=utc)), to_j_instant(1700000000000),\n\
         \x20   to_datetime(to_j_instant('2024-01-02T03:04:05.123456 UTC')), to_date(to_j_local_date('2024-01-02')),\n\
         \x20   to_time(to_j_local_time('12:34:56.789')), to_timedelta(to_j_duration('PT1.5S')),\n\
         \x20   to_j_duration(datetime.timedelta(days=1, microseconds=5)), to_j_duration(1_500_000_000),\n\
         \x20   to_j_local_date(datetime.date(2024, 2, 29)), to_j_local_time(datetime.time(1, 2, 3, 4)))\n\
         assert got == ['2024-01-02T03:04:05Z', '1970-01-01T00:28:20Z',\n\
         \x20   '2024-01-02 03:04:05.123456', '2024-01-02',\n\
         \x20   '12:34:56.789000', '0:00:01.500000',\n\
         \x20   'PT24H0.000005S', 'PT1.5S',\n\
         \x20   '2024-02-29', '01:02:03.000004'], got\n\
         for call, text in (\n\
         \x20   (lambda: to_j_instant('2024-13-01T00:00:00 UTC'), 'Cannot parse instant: 2024-13-01T00:00:00 UTC'),\n\
         \x20   (lambda: to_j_time_zone('Nowhere/None'), 'Cannot parse time zone: Nowhere/None'),\n\
         \x20   (lambda: to_j_duration('1 hour'), 'Cannot parse duration: 1 hour'),\n\
         \x20   (lambda: to_j_period('PT1H'), 'Cannot parse period: PT1H'),\n\
         \x20   (lambda: to_j_local_date('2023-13-01'), 'Cannot parse local date: 2023-13-01'),\n\
         ):\n\
         \x20   try:\n\
         \x20       call()\n\
         \x20       raise AssertionError(text)\n\
         \x20   except DHError as e:\n\
         \x20       assert text in str(e), e\n\
         time_zone_alias_add('PAR', 'Europe/Paris')\n\
         try:\n\
         \x20   assert str(to_j_instant('2024-01-02T09:00:00 PAR')) == '2024-01-02T08:00:00Z'\n\
         finally:\n\
         \x20   assert time_zone_alias_rm('PAR') is True\n\
         assert time_zone_alias_rm('PAR') is False\n\
         now = dh_now()\n\
         assert abs(now.toEpochMilli() - datetime.datetime.now(utc).timestamp() * 1e3) < 60e3\n\
         before = str(datetime.datetime.now(utc).date())\n\
         today = dh_today(to_j_time_zone('UTC'))\n\
         assert today in (before, str(datetime.datetime.now(utc).date())), today\n\
         assert str(dh_time_zone()) != ''\n\
         from deephaven import new_table\n\
         from deephaven.column import datetime_col\n\
         t = new_table([datetime_col('T', [to_j_instant('2024-03-10T01:30:00 ET'),\n\
         \x20   datetime.datetime(2024, 1, 2, tzinfo=utc), '2024-01-02T03:04:05 JP', None])])",
    );
    assert_eq!(
        described(&python, "t"),
        // 2024-03-10T06:30:00Z, 2024-01-02T00:00:00Z, 2024-01-01T18:04:05Z
        ["T: Instant = 1710052200000000000, 1704153600000000000, 1704132245000000000, "]
    );
}
