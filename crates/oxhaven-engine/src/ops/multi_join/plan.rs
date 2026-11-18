//! The arguments of a multi join, checked against its tables.

use super::{MultiJoinError, MultiJoinInput};
use crate::ops::join::keys::{Cell, Key, KeyColumns};
use crate::ops::key_table::{is_ported_key, same_object_vector_class};
use crate::update::ModifiedColumnTransformer;
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array};
use arrow::compute::cast;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::ModifiedColumnSet;
use std::sync::Arc;

/// What one table gives to the join.
pub(super) struct TablePlan {
    /// The table's key columns, in the order of [`Plan::keys`].
    pub(super) keys: KeyColumns,
    /// Every column the table's matches name, for the modified columns of an update.
    pub(super) key_sources: Vec<usize>,
    /// The added columns: the name in the result and the column of the table.
    pub(super) additions: Vec<(String, usize)>,
    /// The first added column the table does not have, which Java finds only after it has
    /// read the table's keys.
    pub(super) missing: Option<MultiJoinError>,
    /// Set when the table's key columns have the first table's types in another order.
    /// Java finds that when it first reads a row of the table, so an empty table passes.
    pub(super) unreadable: Option<MultiJoinError>,
}

impl TablePlan {
    /// The key of each row of `rows`, in row order. A `Boolean` is its byte, so that it
    /// equals the `byte` key column of another table.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MultiJoinTableImpl.MultiJoinInputHelper#keySources,
    /// which reads through io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive
    pub(super) fn read_keys(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<Vec<Key>, TableFailure> {
        let mut keys = self.keys.read(table, rows, use_prev)?;
        for cell in keys.iter_mut().flatten() {
            if let Cell::Bool(value) = cell {
                *cell = Cell::Int(i64::from(*value));
            }
        }
        Ok(keys)
    }

    /// The columns of the table that it adds to the result.
    pub(super) fn added_sources(&self) -> Vec<usize> {
        self.additions.iter().map(|(_, source)| *source).collect()
    }
}

/// A join's arguments, checked.
pub(super) struct Plan {
    /// The key columns of the result: the names the first table gives them and the types
    /// its columns have.
    pub(super) keys: Vec<(String, ColumnType)>,
    pub(super) tables: Vec<TablePlan>,
}

/// The type Java's hash table holds a key column of `column_type` as.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive
fn primitive(column_type: &ColumnType) -> ColumnType {
    match column_type {
        ColumnType::Boolean => ColumnType::Byte,
        ColumnType::Instant => ColumnType::Long,
        other => other.clone(),
    }
}

fn same_set<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    a.iter().all(|item| b.contains(item)) && b.iter().all(|item| a.contains(item))
}

impl Plan {
    /// Behavior: the constructor of io.deephaven.engine.table.impl.MultiJoinTableImpl up to
    /// its choice of a join, and
    /// io.deephaven.engine.table.impl.MultiJoinTableImpl.MultiJoinInputHelper, in Java's
    /// order: the key columns every table must have, then the key names and key types of
    /// each table against the first, then the names of the added columns.
    pub(super) fn new(inputs: &[MultiJoinInput]) -> Result<Plan, MultiJoinError> {
        // Per table: the result name of each key column and the column it is read from.
        // Of two matches with one result name the later one names the column.
        let mut matched: Vec<Vec<(String, usize)>> = Vec::with_capacity(inputs.len());
        for (table, input) in inputs.iter().enumerate() {
            let definition = input.table.definition();
            let mut keys: Vec<(String, usize)> = Vec::new();
            for pair in &input.on {
                let Some(source) = definition.index_of(&pair.right) else {
                    return Err(MultiJoinError::NoSuchColumn {
                        table,
                        column: pair.right.clone(),
                    });
                };
                match keys.iter_mut().find(|(name, _)| *name == pair.left) {
                    Some(earlier) => earlier.1 = source,
                    None => keys.push((pair.left.clone(), source)),
                }
            }
            matched.push(keys);
        }

        let names = |keys: &[(String, usize)]| -> Vec<String> {
            keys.iter().map(|(name, _)| name.clone()).collect()
        };
        let type_of = |table: usize, source: usize| -> ColumnType {
            let columns = inputs[table].table.definition().columns();
            columns[source].column_type.clone()
        };
        let first_names = names(&matched[0]);
        let first_types: Vec<ColumnType> = matched[0]
            .iter()
            .map(|(_, source)| type_of(0, *source))
            .collect();
        let first_primitives: Vec<ColumnType> = first_types.iter().map(primitive).collect();

        let mut tables = Vec::with_capacity(inputs.len());
        for (table, keys) in matched.iter().enumerate() {
            let this_names = names(keys);
            if !same_set(&first_names, &this_names) {
                return Err(MultiJoinError::KeyColumnMismatch {
                    table,
                    first: first_names,
                    this: this_names,
                });
            }
            // This table's key columns in the first table's order.
            let ordered: Vec<usize> = first_names
                .iter()
                .filter_map(|name| keys.iter().find(|(n, _)| n == name).map(|(_, s)| *s))
                .collect();
            let this_types: Vec<ColumnType> = ordered
                .iter()
                .map(|source| type_of(table, *source))
                .collect();
            let this_primitives: Vec<ColumnType> = this_types.iter().map(primitive).collect();
            for (first, this) in first_types.iter().zip(&this_types) {
                if first != this && same_object_vector_class(first, this) {
                    unported!(
                        "io.deephaven.engine.table.impl.MultiJoinTableImpl#MultiJoinTableImpl",
                        "key columns of the vector types {first:?} and {this:?}: Java takes \
                         the pair (one class, ObjectVector)"
                    );
                }
            }
            if !same_set(&first_primitives, &this_primitives) {
                return Err(MultiJoinError::KeyTypeMismatch {
                    table,
                    first: first_primitives,
                    this: this_primitives,
                });
            }
            if let Some(this) = this_types.iter().find(|this| !is_ported_key(this)) {
                unported!(
                    "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
                    "a key column of type {this:?}: Java keys vectors of float or double by \
                     the class of the vector objects (-0.0 and 0.0), and arrays and vectors \
                     of arrays by the identity of the arrays"
                );
            }
            let input = &inputs[table];
            let definition = input.table.definition();
            let key_sources = input
                .on
                .iter()
                .filter_map(|pair| definition.index_of(&pair.right))
                .collect();
            tables.push(TablePlan {
                keys: KeyColumns { columns: ordered },
                key_sources,
                additions: Vec::new(),
                missing: None,
                unreadable: None,
            });
        }

        // The result's columns so far and the table that added each; `None` for a key.
        let mut used: Vec<(String, Option<usize>)> = first_names
            .iter()
            .map(|name| (name.clone(), None))
            .collect();
        for (table, input) in inputs.iter().enumerate() {
            let definition = input.table.definition();
            let defaults: Vec<super::JoinAddition>;
            let add = if input.add.is_empty() {
                defaults = definition
                    .columns()
                    .iter()
                    .filter(|column| !input.on.iter().any(|pair| pair.right == column.name))
                    .map(|column| super::JoinAddition::same(column.name.as_str()))
                    .collect();
                &defaults
            } else {
                &input.add
            };
            for addition in add {
                if let Some((_, earlier)) = used.iter().find(|(name, _)| *name == addition.name) {
                    return Err(MultiJoinError::ColumnConflict {
                        column: addition.name.clone(),
                        earlier: *earlier,
                        table,
                    });
                }
                used.push((addition.name.clone(), Some(table)));
                let plan = &mut tables[table];
                match definition.index_of(&addition.source) {
                    Some(source) => plan.additions.push((addition.name.clone(), source)),
                    None if plan.missing.is_none() => {
                        plan.missing = Some(MultiJoinError::NoSuchColumn {
                            table,
                            column: addition.source.clone(),
                        });
                    }
                    None => {}
                }
            }
        }

        // Java's hash table is made for the first table's key types. A table whose key
        // types are those in another order passes the checks above and cannot be read.
        for (table, plan) in tables.iter_mut().enumerate() {
            for (position, source) in plan.keys.columns.iter().enumerate() {
                let this = type_of(table, *source);
                if primitive(&this) != first_primitives[position] {
                    plan.unreadable = Some(MultiJoinError::KeyTypeOrder {
                        table,
                        column: first_names[position].clone(),
                        first: first_types[position].clone(),
                        this,
                    });
                    break;
                }
            }
        }

        Ok(Plan {
            keys: first_names.into_iter().zip(first_types).collect(),
            tables,
        })
    }

    pub(super) fn key_types(&self) -> Vec<ColumnType> {
        self.keys.iter().map(|(_, t)| t.clone()).collect()
    }

    /// The first added column that a table does not have.
    pub(super) fn missing(&self) -> Option<MultiJoinError> {
        self.tables.iter().find_map(|table| table.missing.clone())
    }

    /// The result's columns over `tables`: the key columns, then each table's added
    /// columns.
    pub(super) fn definition(&self, tables: &[Arc<dyn Table>]) -> TableDefinition {
        let mut columns: Vec<ColumnDefinition> = self
            .keys
            .iter()
            .map(|(name, column_type)| ColumnDefinition::new(name.as_str(), column_type.clone()))
            .collect();
        for (plan, table) in self.tables.iter().zip(tables) {
            for (name, source) in &plan.additions {
                let column_type = table.definition().columns()[*source].column_type.clone();
                columns.push(ColumnDefinition::new(name.as_str(), column_type));
            }
        }
        match TableDefinition::new(columns) {
            Ok(definition) => definition,
            Err(e) => panic!("the result's columns passed the name checks: {e}"),
        }
    }

    /// Per table, how its modified columns show in the result, and all the result columns
    /// it adds.
    ///
    /// Behavior: the `newModifiedColumnSetTransformer` and `newModifiedColumnSet` calls of
    /// io.deephaven.engine.table.impl.MultiJoinTableImpl#bucketedMultiJoin
    pub(super) fn modified_columns(&self) -> Vec<(ModifiedColumnTransformer, ModifiedColumnSet)> {
        let mut next = self.keys.len();
        self.tables
            .iter()
            .map(|plan| {
                let results = next..next + plan.additions.len();
                next = results.end;
                let pairs = plan.additions.iter().zip(results.clone());
                let transformer = ModifiedColumnTransformer::new(
                    pairs.map(|((_, source), result)| (*source, [result])),
                );
                (transformer, ModifiedColumnSet::of(results))
            })
            .collect()
    }
}

/// `values` of a key column of type `from`, as the result's key column of type `to` shows
/// them. The two differ only where a table has a `long` key for another's `Instant`, or a
/// `byte` for a `Boolean`.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ReinterpretUtils#convertToOriginalType
/// and io.deephaven.util.BooleanUtils#byteAsBoolean, #booleanAsByte
pub(super) fn as_key_type(
    values: ArrayRef,
    from: &ColumnType,
    to: &ColumnType,
) -> Result<ArrayRef, TableFailure> {
    if from == to {
        return Ok(values);
    }
    let unreadable = || TableFailure {
        message: format!("a key column of type {from:?} that is not stored as one"),
    };
    match (from, to) {
        (ColumnType::Byte, ColumnType::Boolean) => {
            let bytes = values.as_any().downcast_ref::<Int8Array>();
            let bytes = bytes.ok_or_else(unreadable)?;
            let booleans: BooleanArray = bytes.iter().map(|byte| byte.map(|b| b > 0)).collect();
            Ok(Arc::new(booleans))
        }
        (ColumnType::Boolean, ColumnType::Byte) => {
            let booleans = values.as_any().downcast_ref::<BooleanArray>();
            let booleans = booleans.ok_or_else(unreadable)?;
            let bytes: Int8Array = booleans.iter().map(|b| b.map(i8::from)).collect();
            Ok(Arc::new(bytes))
        }
        _ => cast(&values, &to.arrow_type()).map_err(|e| TableFailure {
            message: format!("a key column of type {from:?} as {to:?}: {e}"),
        }),
    }
}
