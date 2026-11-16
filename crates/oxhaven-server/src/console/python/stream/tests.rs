//! `deephaven.stream` through the console of a running server. What Java answers was
//! observed on Java 42.9 (`tasks/7.4.2-stream-adapter.md`).

use std::time::{Duration, Instant};

use oxhaven_contracts::graph::UpdateGraph;

use super::super::tests::Python;

const SETUP: &str = "\
from deephaven import DHError, dtypes, empty_table, new_table
from deephaven.column import col_def, double_col, int_col, string_col
from deephaven.liveness_scope import liveness_scope
from deephaven.stream import blink_to_append_only, stream_to_append_only
from deephaven.stream.table_publisher import TablePublisher, table_publisher
EV = []
def raised(f):
    try:
        f()
    except Exception as e:
        return (type(e).__name__, str(e))
    raise AssertionError('no error')
";

/// Runs an update cycle, then `code`, an assertion, until it holds. The test server's graph
/// runs a cycle only when asked.
fn eventually(python: &Python, code: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        python.server.state.graph.run_cycle();
        let error = python.console.evaluate(code).error;
        match error {
            None => return,
            Some(error) if Instant::now() >= deadline => panic!("never held: {code}\n{error}"),
            Some(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

#[tokio::test]
async fn a_table_publisher_feeds_a_blink_table_and_ends_on_failure_or_release() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "t, p = table_publisher('p', {'A': dtypes.int32, 'S': dtypes.string},\n\
         \x20   on_shutdown_callback=lambda: EV.append('shutdown'))\n\
         assert isinstance(p, TablePublisher)\n\
         assert (t.is_blink, t.is_refreshing, t.is_failed, p.is_alive, t.size) == \
         (True, True, False, True, 0)\n\
         a = blink_to_append_only(t)\n\
         assert blink_to_append_only(t).j_table.is_same(a.j_table)\n\
         assert not a.is_blink and a.is_refreshing\n\
         p.add(new_table([string_col('S', ['x', 'y']), int_col('A', [1, 2]), \
         double_col('Extra', [1.0, 2.0])]))\n\
         missing = raised(lambda: p.add(new_table([int_col('A', [1])])))\n\
         assert missing == ('RuntimeError', 'io.deephaven.engine.table.impl.NoSuchColumnException: \
         Unknown column names [S], available column names are [A]'), missing\n\
         assert raised(lambda: p.add('x'))[0] == 'AttributeError'\n\
         chunk = raised(lambda: table_publisher('q', {'A': dtypes.int32}, chunk_size=0))\n\
         assert chunk == ('RuntimeError', \
         'java.lang.IllegalArgumentException: chunkSize must be positive'), chunk\n\
         refused = raised(lambda: blink_to_append_only(empty_table(3)))\n\
         assert refused[0] == 'DHError', refused\n\
         assert 'failed to create an append-only table.' in refused[1], refused\n\
         assert 'Input is not a blink table!' in refused[1], refused\n\
         assert raised(lambda: stream_to_append_only(a))[0] == 'DHError'\n\
         assert t.size == 0",
    );
    eventually(&python, "assert a.size == 2, a.size");
    eventually(&python, "assert t.size == 0, t.size");
    python.run(
        "p.add(new_table([string_col('S', ['z']), int_col('A', [3])]))\n\
         p.publish_failure(ValueError('boom'))\n\
         assert p.is_alive and EV == []",
    );
    eventually(
        &python,
        "assert t.is_failed and a.is_failed and not p.is_alive and EV == ['shutdown'], \
         (t.is_failed, a.is_failed, p.is_alive, EV)",
    );
    // The rows that arrived with the failure never show.
    python.run(
        "assert a.size == 2, a.size\n\
         p.add(new_table([string_col('S', ['v']), int_col('A', [5])]))\n\
         p.publish_failure('again')\n\
         assert EV == ['shutdown']",
    );

    // An exception in the flush callback fails the table; what the callback adds shows.
    python.run(
        "FL = []\n\
         def flush(pub):\n\
         \x20   FL.append(pub.is_alive)\n\
         \x20   if len(FL) == 1:\n\
         \x20       pub.add(new_table([int_col('A', [42])]))\n\
         \x20   if len(FL) == 3:\n\
         \x20       raise RuntimeError('flush raised')\n\
         t2, p2 = table_publisher('p2', [col_def('A', dtypes.int32)], on_flush_callback=flush,\n\
         \x20   on_shutdown_callback=lambda: EV.append('shutdown2'))\n\
         a2 = blink_to_append_only(t2)",
    );
    eventually(
        &python,
        "assert t2.is_failed and not p2.is_alive and 'shutdown2' in EV, (t2.is_failed, EV)",
    );
    python.run("assert a2.size == 1 and FL == [True, True, True], (a2.size, FL)");

    // A liveness scope that releases the table ends the stream at once.
    python.run(
        "with liveness_scope():\n\
         \x20   t3, p3 = table_publisher('p3', {'A': dtypes.int32}, \
         on_shutdown_callback=lambda: EV.append('shutdown3'))\n\
         assert not p3.is_alive and EV[-1] == 'shutdown3', EV",
    );
}
