//! The proxy of a partitioned table, through the console of a running server. What Java
//! answers is in `docs/PARTITIONED.md`, "The proxy", and in the scenarios
//! `oracle/scenarios/python/partitioned_proxy_*`.

use super::tests::{described, Python};

const SETUP: &str = "\
from deephaven import DHError, empty_table, input_table, new_table
from deephaven.column import int_col
def change(ids, keys, values):
    return new_table([int_col('Id', ids), int_col('K', keys), int_col('V', values)])
";

/// The operations a table method sends to the engine are recorded once, on a table
/// without rows, and run again in the cycle for a constituent that appears: with the
/// variables the formulas had at the call, whatever they are by then.
#[tokio::test]
async fn a_constituent_of_a_later_cycle_gets_the_variables_of_the_call() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "rows = input_table(init_table=change([1, 2], [1, 2], [10, 20]), key_cols='Id')\n\
         px = rows.partition_by('K').proxy()\n\
         a = 100\n\
         def local():\n    b = 1000\n    return px.update('X = V + a + b').where('X > a')\n\
         made = local()\n\
         merged = made.target.merge()\n\
         calls = []\n\
         traced = px.target.transform(lambda t: calls.append(t.size) or t.head(1))\n\
         traced_merged = traced.merge()\n\
         a = 0\n\
         del local\n\
         rows.add(change([3, 4], [3, 1], [30, 40]))",
    );
    assert_eq!(
        described(&python, "merged"),
        [
            "Id: Int = 1, 4, 2, 3",
            "K: Int = 1, 1, 2, 3",
            "V: Int = 10, 40, 20, 30",
            "X: Int = 1110, 1140, 1120, 1130"
        ]
    );
    // the function of a transform is called in the cycle; the proxy's method is not: a
    // recording has no step for the empty table the method is tried on
    python.run(
        "assert sorted(calls) == [0, 1, 1, 1], calls\n\
         assert made.target.table.size == 3 and not made.target.table.is_failed",
    );
}

/// A table that the operation reads besides the constituent is the same table in every
/// run of the recording, and a refreshing one is a dependency of the result.
#[tokio::test]
async fn a_table_the_operation_reads_is_the_same_in_every_cycle() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "rows = input_table(init_table=change([1, 2], [1, 2], [10, 20]), key_cols='Id')\n\
         other = input_table(init_table=change([1], [1], [7]).view(['K', 'S = V']), key_cols='K')\n\
         px = rows.partition_by('K').proxy()\n\
         joined = px.natural_join(other, 'K', 'S').view(['Id', 'S']).target.merge()\n\
         rows.add(change([3], [3], [30]))\n\
         other.add(change([3, 2], [3, 2], [9, 8]).view(['K', 'S = V']))",
    );
    assert_eq!(
        described(&python, "joined"),
        ["Id: Int = 1, 2, 3", "S: Int = 7, 8, 9"]
    );
}

/// A filter is applied to the underlying table when it reads nothing but key columns
/// that the constituents are known to hold, and otherwise to every constituent.
#[tokio::test]
async fn a_filter_of_key_columns_is_a_filter_of_the_underlying_table() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "src = empty_table(6).update(['K = i % 3', 'V = i'])\n\
         pt = src.partition_by('K')\n\
         px = pt.proxy()\n\
         low = 1\n\
         sizes = lambda proxy: (proxy.target.table.size, proxy.target.merge().size)\n\
         assert sizes(px.where('K >= low')) == (2, 4)\n\
         assert sizes(px.where(['K >= low', 'V > 3'])) == (2, 2)\n\
         assert sizes(px.where('K >= low && V > 3')) == (3, 2)\n\
         assert sizes(px.update('K = K + 0').where('K >= low')) == (3, 4)\n\
         assert sizes(pt.sort('K').proxy().where('K >= low')) == (3, 4)\n\
         assert sizes(pt.filter('K < 2').proxy().where('K >= low')) == (1, 2)",
    );
    let error = python
        .console
        .evaluate("px.where('Nope > 1')")
        .error
        .unwrap_or_default();
    assert!(
        error.contains("where operation on the PartitionedTableProxy failed."),
        "{error}"
    );
}
