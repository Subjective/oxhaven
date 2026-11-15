//! Input tables and table writers, through the console of a running server. The Python
//! here imports the standard library only. What Java answers is in the task's notes
//! (`tasks/6.10-python-input-tables.md`) and in the scenarios
//! `oracle/scenarios/python/input_*`.

use std::time::{Duration, Instant};

use super::super::tests::{described, Python};

const SETUP: &str = "\
from deephaven import DHError, DynamicTableWriter, dtypes, empty_table, input_table, new_table
from deephaven.column import double_col, int_col, string_col
from deephaven.table_factory import InputTable
def ks(keys, texts):
    return new_table([int_col('K', keys), string_col('S', texts)])
def kv(keys, values):
    return new_table([int_col('K', keys), double_col('V', values)])
";

/// The text of the error the code fails with.
fn error(python: &Python, code: &str) -> String {
    let error = python.console.evaluate(code).error;
    error.unwrap_or_else(|| panic!("no error: {code}"))
}

/// Runs `code`, an assertion, until it holds: what the writer thread does comes after the
/// command that asked for it.
fn eventually(python: &Python, code: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let error = python.console.evaluate(code).error;
        match error {
            None => return,
            Some(error) if Instant::now() >= deadline => panic!("never held: {code}\n{error}"),
            Some(_) => std::thread::sleep(Duration::from_millis(5)),
        }
    }
}

#[tokio::test]
async fn a_command_reads_the_rows_it_added_to_an_append_only_input_table() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "rows = input_table({'K': dtypes.int32, 'S': dtypes.string})\n\
         total = rows.count_by('N')\n\
         assert rows.size == 0\n\
         rows.add(ks([1, 2], ['a', 'b']))\n\
         assert rows.size == 2, rows.size\n\
         rows.add(ks([1], ['again']))\n\
         assert rows.size == 3, rows.size\n\
         assert (rows.key_names, rows.value_names) == ([], ['K', 'S'])",
    );
    assert_eq!(
        described(&python, "rows"),
        ["K: Int = 1, 2, 1", "S: String = a, b, again"]
    );
    assert_eq!(described(&python, "total"), ["N: Long = 3"]);
    let rows = python.table("rows");
    let attributes = rows.attributes();
    assert!(attributes.add_only && attributes.append_only);
    assert!(rows.input_table().is_some());
}

#[tokio::test]
async fn a_keyed_input_table_replaces_and_deletes_by_key() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "keyed = input_table(init_table=kv([1, 2, 3, 2], [1.0, 2.0, 3.0, 2.5]), key_cols='K')\n\
         assert keyed.size == 3, keyed.size\n\
         assert (keyed.key_names, keyed.value_names) == (['K'], ['V'])\n\
         large = keyed.where('V > 1.5')\n\
         keyed.add(kv([2, 4], [0.5, 4.0]))\n\
         assert (keyed.size, large.size) == (4, 2), (keyed.size, large.size)\n\
         keyed.delete(kv([3, 9], [0.0, 0.0]).view('K'))\n\
         assert (keyed.size, large.size) == (3, 1), (keyed.size, large.size)\n\
         through = InputTable(large.j_table)\n\
         through.add(kv([5], [5.0]))\n\
         assert keyed.size == 4, keyed.size",
    );
    assert_eq!(
        described(&python, "keyed"),
        ["K: Int = 1, 2, 4, 5", "V: Double = 1.0, 0.5, 4.0, 5.0"]
    );
    assert_eq!(
        described(&python, "large"),
        ["K: Int = 4, 5", "V: Double = 4.0, 5.0"]
    );
}

#[tokio::test]
async fn what_cannot_be_done_is_a_dherror_with_the_text_of_the_reference() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "rows = input_table({'K': dtypes.int32, 'S': dtypes.string})\n\
         keyed = input_table(init_table=kv([1], [1.0]), key_cols='K')",
    );
    for (code, text) in [
        (
            "rows.delete(ks([1], ['a']).view('K'))",
            "delete data in the InputTable failed.",
        ),
        ("rows.add(kv([1], [1.0]))", "add to InputTable failed."),
        (
            "rows.add_async(kv([1], [1.0]))",
            "async add to InputTable failed.",
        ),
        (
            "keyed.delete_async(kv([1], [1.0]))",
            "async delete data in the InputTable failed.",
        ),
        (
            "InputTable(empty_table(1).j_table)",
            "the provided table input is not suitable for input tables.",
        ),
        (
            "input_table()",
            "failed to create an in-memory InputTable. : either column definitions or init \
             table should be provided.",
        ),
        (
            "input_table({'K': dtypes.int32}, kv([1], [1.0]))",
            "failed to create an in-memory InputTable. : both column definitions and init \
             table are provided.",
        ),
        (
            "input_table({'K': dtypes.int32}, key_cols='Nope')",
            "failed to create an in-memory InputTable.",
        ),
    ] {
        let error = error(&python, code);
        assert!(error.contains("DHError"), "{code}: {error}");
        assert!(error.contains(text), "{code}: {error}");
    }
    // Nothing above changed a table.
    python.run("assert (rows.size, keyed.size) == (0, 1), (rows.size, keyed.size)");
}

#[tokio::test]
async fn asynchronous_changes_are_made_after_the_command_in_the_order_asked() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "keyed = input_table(init_table=kv([1], [1.0]), key_cols='K')\n\
         ended = []\n\
         keyed.add_async(kv([1, 2], [10.0, 20.0]), on_success=lambda: ended.append('added'))\n\
         keyed.delete_async(kv([2], [0.0]).view('K'), on_success=lambda: ended.append('deleted'))\n\
         keyed.add_async(kv([1], [11.0]), lambda: ended.append('added again'))\n\
         keyed.add_async(kv([], []), lambda: ended.append('nothing'))\n\
         in_command = (keyed.size, list(ended))",
    );
    // Java: `(1, [])`; no cycle runs while a command does not wait for one.
    python.run("assert in_command == (1, []), in_command");
    eventually(&python, "assert len(ended) == 4, ended");
    python.run("assert sorted(ended) == ['added', 'added again', 'deleted', 'nothing'], ended");
    assert_eq!(
        described(&python, "keyed"),
        ["K: Int = 1", "V: Double = 11.0"]
    );
}

#[tokio::test]
async fn a_synchronous_change_is_made_after_the_asynchronous_ones_asked_before_it() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "keyed = input_table(init_table=kv([1], [1.0]), key_cols='K')\n\
         keyed.add_async(kv([1], [2.0]))\n\
         keyed.add_async(kv([1, 2], [3.0, 3.0]))\n\
         keyed.add(kv([1], [4.0]))\n\
         assert keyed.size == 2, keyed.size",
    );
    assert_eq!(
        described(&python, "keyed"),
        ["K: Int = 1, 2", "V: Double = 4.0, 3.0"]
    );
}

#[tokio::test]
async fn a_callback_that_raises_does_not_stop_the_changes_after_it() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "rows = input_table({'K': dtypes.int32, 'S': dtypes.string})\n\
         ended = []\n\
         def fails():\n\
         \x20   raise ValueError('a callback failed')\n\
         rows.add_async(ks([1], ['a']), fails)\n\
         rows.add_async(ks([2], ['b']), lambda: ended.append('ok'))",
    );
    eventually(&python, "assert ended == ['ok'], ended");
    assert_eq!(
        described(&python, "rows"),
        ["K: Int = 1, 2", "S: String = a, b"]
    );
}

#[tokio::test]
async fn a_writer_s_rows_are_in_its_table_after_the_command() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "writer = DynamicTableWriter({'I': dtypes.int32, 'B': dtypes.byte, 'D': dtypes.double, \
         'S': dtypes.string, 'F': dtypes.bool_})\n\
         written = writer.table\n\
         writer.write_row(1, 2, 3, 'a', True)\n\
         writer.write_row(3.7, 300, 1, None, None)\n\
         writer.write_row(None, -1.5, 2.5, 'c', False)\n\
         in_command = written.size",
    );
    // Java: 0
    python.run("assert in_command == 0, in_command");
    eventually(&python, "assert written.size == 3, written.size");
    assert_eq!(
        described(&python, "written"),
        [
            "I: Int = 1, 3, ",
            "B: Byte = 2, 44, -1",
            "D: Double = 3.0, 1.0, 2.5",
            "S: String = a, , c",
            "F: Boolean = true, , false",
        ]
    );
    let written = python.table("written");
    let attributes = written.attributes();
    assert!(attributes.add_only && attributes.append_only);
    assert!(written.input_table().is_none());

    for (code, text) in [
        (
            "writer.write_row(1, 2)",
            "Incompatible logRowPermissive call, values length=2 != setters=5",
        ),
        (
            "writer.write_row(True, 2, 3.0, 'a', True)",
            "java.lang.ClassCastException",
        ),
        (
            "writer.write_row(1, 2, 3.0, 4, True)",
            "java.lang.ClassCastException",
        ),
        (
            "writer.write_row(1, 2, 3.0, 'a', 1)",
            "java.lang.ClassCastException",
        ),
    ] {
        let error = error(&python, code);
        assert!(error.contains("failed to write a row."), "{code}: {error}");
        assert!(error.contains(text), "{code}: {error}");
    }
    // A refused row is not written, in part or whole; a closed writer still writes.
    python.run("with writer:\n    pass\nwriter.write_row(9, 9, 9, 'z', True)");
    eventually(&python, "assert written.size == 4, written.size");
}

#[tokio::test]
async fn what_the_engine_lacks_is_refused_by_the_name_of_the_java_method() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "from deephaven import TableReplayer, function_generated_table, ring_table\n\
         t = empty_table(1)",
    );
    for (code, text) in [
        (
            "ring_table(t, 10)",
            "failed to create a ring table. : not ported: \
             io.deephaven.engine.table.impl.sources.ring.RingTableTools#of",
        ),
        (
            "function_generated_table(lambda: t, refresh_interval_ms=1000)",
            "not ported: io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory#create",
        ),
        (
            "function_generated_table(lambda: t)",
            "Either refresh_interval_ms or source_tables must be provided!",
        ),
        (
            "TableReplayer('2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z')",
            "failed to create a replayer. : not ported: \
             io.deephaven.engine.table.impl.replay.Replayer#Replayer(Instant, Instant)",
        ),
    ] {
        let error = error(&python, code);
        assert!(error.contains("DHError"), "{code}: {error}");
        assert!(error.contains(text), "{code}: {error}");
    }
}
