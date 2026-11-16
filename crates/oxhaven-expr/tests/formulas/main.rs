//! Formulas evaluated through the `FormulaCompiler` and `Formula` contracts, against a
//! static table backed by Arrow arrays.
//!
//! Expected values were derived by reading the Java method each test cites, in
//! `io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils` (QLFU) unless the test
//! says otherwise.

mod arithmetic;
mod arrays;
mod comparison;
mod corpus;
mod external;
mod formula;
mod gaps;
mod logic;
mod recorded;
mod tagged;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt64Array,
};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::expr::{FormulaCompiler, FormulaError};
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{
    ColumnSource, Table, TableAttributes, TableFailure, UpdateListener,
};
use oxhaven_expr::Compiler;
use std::sync::Arc;

/// A column's type and its values, one per row.
type Column = (ColumnType, ArrayRef);

fn booleans<const N: usize>(values: [Option<bool>; N]) -> Column {
    (
        ColumnType::Boolean,
        Arc::new(BooleanArray::from(values.to_vec())),
    )
}

fn bytes<const N: usize>(values: [Option<i8>; N]) -> Column {
    (ColumnType::Byte, Arc::new(Int8Array::from(values.to_vec())))
}

fn chars<const N: usize>(values: [Option<char>; N]) -> Column {
    let units: Vec<Option<u16>> = values
        .iter()
        .map(|c| c.map(|c| u16::try_from(u32::from(c)).expect("a char in the basic plane")))
        .collect();
    (ColumnType::Char, Arc::new(UInt16Array::from(units)))
}

fn shorts<const N: usize>(values: [Option<i16>; N]) -> Column {
    (
        ColumnType::Short,
        Arc::new(Int16Array::from(values.to_vec())),
    )
}

fn ints<const N: usize>(values: [Option<i32>; N]) -> Column {
    (ColumnType::Int, Arc::new(Int32Array::from(values.to_vec())))
}

fn longs<const N: usize>(values: [Option<i64>; N]) -> Column {
    (
        ColumnType::Long,
        Arc::new(Int64Array::from(values.to_vec())),
    )
}

fn floats<const N: usize>(values: [Option<f32>; N]) -> Column {
    (
        ColumnType::Float,
        Arc::new(Float32Array::from(values.to_vec())),
    )
}

fn doubles<const N: usize>(values: [Option<f64>; N]) -> Column {
    (
        ColumnType::Double,
        Arc::new(Float64Array::from(values.to_vec())),
    )
}

fn strings<const N: usize>(values: [Option<&str>; N]) -> Column {
    (
        ColumnType::String,
        Arc::new(StringArray::from(values.to_vec())),
    )
}

fn instants<const N: usize>(values: [Option<i64>; N]) -> Column {
    (
        ColumnType::Instant,
        Arc::new(
            TimestampNanosecondArray::from(values.to_vec())
                .with_data_type(ColumnType::Instant.arrow_type()),
        ),
    )
}

/// Booleans without nulls, which is what every comparison returns.
fn flags<const N: usize>(values: [bool; N]) -> Column {
    booleans(values.map(Some))
}

/// A column whose current values are `current` and whose previous values are `previous`,
/// both indexed by row key.
struct TestColumn {
    column_type: ColumnType,
    current: ArrayRef,
    previous: ArrayRef,
}

fn take(values: &ArrayRef, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
    let positions = UInt64Array::from_iter_values(rows.iter().map(|key| key as u64));
    arrow::compute::take(values.as_ref(), &positions, None).map_err(|e| TableFailure {
        message: e.to_string(),
    })
}

impl ColumnSource for TestColumn {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        take(&self.current, rows)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        take(&self.previous, rows)
    }
}

/// A column that cannot be read.
struct FailingColumn(ColumnType);

impl ColumnSource for FailingColumn {
    fn column_type(&self) -> &ColumnType {
        &self.0
    }

    fn chunk(&self, _rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        Err(TableFailure {
            message: "disk on fire".into(),
        })
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }
}

/// A static table whose row keys are `0..size`.
struct TestTable {
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
    rows: Arc<RowSet>,
}

impl TestTable {
    fn new<const N: usize>(columns: [(&str, Column); N]) -> Self {
        Self::with_previous(columns.map(|(name, column)| (name, column.clone(), column)))
    }

    /// Columns given as name, current values, previous values.
    fn with_previous<'a>(columns: impl IntoIterator<Item = (&'a str, Column, Column)>) -> Self {
        let columns: Vec<_> = columns.into_iter().collect();
        let size = columns.first().map_or(0, |(_, column, _)| column.1.len());
        let definition = TableDefinition::new(
            columns
                .iter()
                .map(|(name, column, _)| ColumnDefinition::new(*name, column.0.clone()))
                .collect(),
        )
        .expect("distinct column names");
        let columns = columns
            .into_iter()
            .map(|(_, (column_type, current), (_, previous))| {
                assert_eq!(current.len(), size, "columns have one value per row");
                assert_eq!(previous.len(), size, "columns have one value per row");
                Arc::new(TestColumn {
                    column_type,
                    current,
                    previous,
                }) as Arc<dyn ColumnSource>
            })
            .collect();
        Self {
            definition,
            columns,
            rows: Arc::new(RowSet::flat(size as u64)),
        }
    }

    /// Evaluates `formula` for every row. A formula that does not compile is a test bug.
    fn evaluate(&self, formula: &str) -> Result<Column, TableFailure> {
        let compiler = Compiler;
        let expr = compiler
            .parse(formula)
            .unwrap_or_else(|e| panic!("{formula} did not parse: {e}"));
        let compiled = compiler
            .compile(&expr, &self.definition)
            .unwrap_or_else(|e| panic!("{formula} did not compile: {e}"));
        let values = compiled.evaluate(self, &self.rows, false)?;
        Ok((compiled.result_type().clone(), values))
    }

    /// Asserts that `formula` has the type of `expected` and evaluates to its values.
    fn assert(&self, formula: &str, expected: Column) {
        let actual = self
            .evaluate(formula)
            .unwrap_or_else(|e| panic!("{formula} failed: {e}"));
        assert_eq!(actual.0, expected.0, "result type of {formula}");
        assert_eq!(
            actual.1.data_type(),
            expected.1.data_type(),
            "Arrow type of {formula}"
        );
        assert_eq!(
            render(&actual.1),
            render(&expected.1),
            "values of {formula}"
        );
    }

    /// The error from parsing and compiling `formula`.
    fn compile_error(&self, formula: &str) -> FormulaError {
        let compiler = Compiler;
        let result = compiler
            .parse(formula)
            .and_then(|expr| compiler.compile(&expr, &self.definition));
        match result {
            Ok(_) => panic!("{formula} compiled"),
            Err(error) => error,
        }
    }
}

impl Dependency for TestTable {
    fn satisfied(&self, _step: u64) -> bool {
        true
    }
}

impl Table for TestTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn row_set(&self) -> Arc<RowSet> {
        self.rows.clone()
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        self.rows.clone()
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        self.columns[index].clone()
    }

    fn is_refreshing(&self) -> bool {
        false
    }

    fn attributes(&self) -> TableAttributes {
        TableAttributes::default()
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        None
    }

    fn add_listener(&self, _listener: &Arc<dyn UpdateListener>) {}
}

/// Each element as text, so that two arrays compare equal exactly when a client would see
/// the same values: null only equals null, any NaN equals any NaN, and `-0.0` differs from
/// `0.0`.
fn render(array: &ArrayRef) -> Vec<String> {
    fn each<T: std::fmt::Debug>(values: impl Iterator<Item = Option<T>>) -> Vec<String> {
        values
            .map(|value| match value {
                Some(value) => format!("{value:?}"),
                None => "null".to_string(),
            })
            .collect()
    }
    match array.data_type() {
        DataType::Boolean => each(array.as_boolean().iter()),
        DataType::Int8 => each(array.as_primitive::<Int8Type>().iter()),
        DataType::UInt16 => each(array.as_primitive::<UInt16Type>().iter()),
        DataType::Int16 => each(array.as_primitive::<Int16Type>().iter()),
        DataType::Int32 => each(array.as_primitive::<Int32Type>().iter()),
        DataType::Int64 => each(array.as_primitive::<Int64Type>().iter()),
        DataType::Float32 => each(array.as_primitive::<Float32Type>().iter()),
        DataType::Float64 => each(array.as_primitive::<Float64Type>().iter()),
        DataType::Utf8 => each(array.as_string::<i32>().iter()),
        DataType::Timestamp(_, _) => each(array.as_primitive::<TimestampNanosecondType>().iter()),
        DataType::List(_) => each(
            array
                .as_list::<i32>()
                .iter()
                .map(|cell| cell.map(|cell| render(&cell).join(", "))),
        ),
        other => panic!("no rendering for {other}"),
    }
}

/// Asserts `left op right` for two columns named `L` and `R`.
fn assert_binary(formula: &str, left: Column, right: Column, expected: Column) {
    TestTable::new([("L", left), ("R", right)]).assert(formula, expected);
}
