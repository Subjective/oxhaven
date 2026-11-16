//! The table of a stream of records: the thread that polls, which publishes what it decodes
//! to a stream table of the engine.
//!
//! Java builds this from a `KafkaStreamPublisher` and a `StreamToBlinkTableAdapter`, with
//! `BlinkTableTools#blinkToAppendOnly` or `RingTableTools#of` on top for an append-only or
//! a ring table. Here the engine's `stream_table` is the adapter.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::table::{
    blink_to_append_only, ring_table, stream_table, StreamCallbacks, StreamPublisher,
};

use crate::{KafkaError, Plan, RecordSource, TableType};

fn failure(error: impl ToString) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

fn now_nanos() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH);
    since.map_or(0, |since| since.as_nanos() as i64)
}

/// Behavior: io.deephaven.kafka.ingest.KafkaIngester#consumerLoop and #pollOnce, which end
/// when the publisher is shut down. A record that cannot become a row fails the table, as
/// Java's `StreamConsumer#acceptFailure` does, and ends the loop.
fn poll_loop(
    mut source: Box<dyn RecordSource>,
    plan: Plan,
    publisher: StreamPublisher,
    stopped: Arc<AtomicBool>,
) {
    while !stopped.load(Ordering::Acquire) {
        let decoded = source.poll().and_then(|records| {
            if records.is_empty() {
                Ok(None)
            } else {
                plan.decode(&records, now_nanos()).map(Some)
            }
        });
        let added = match decoded {
            Ok(None) => Ok(()),
            Ok(Some(batch)) => publisher.add(batch),
            Err(error) => Err(failure(error)),
        };
        if let Err(failure) = added {
            publisher.fail(failure);
            return;
        }
    }
}

/// The table of the records `source` returns, with the columns of `plan`. A thread named
/// after `topic` polls the source until the table is dropped or fails.
///
/// Behavior: io.deephaven.kafka.KafkaTools#consumeToTable, after the consumer is made
pub fn consume_from(
    graph: Arc<dyn UpdateGraph>,
    plan: Plan,
    source: Box<dyn RecordSource>,
    table_type: TableType,
    topic: &str,
) -> Result<Arc<dyn Table>, KafkaError> {
    let stopped = Arc::new(AtomicBool::new(false));
    let stop = stopped.clone();
    let callbacks = StreamCallbacks {
        on_flush: None,
        // Behavior: KafkaStreamPublisher#shutdown, which shuts the ingester down
        on_shutdown: Some(Box::new(move || stop.store(true, Ordering::Release))),
    };
    let (blink, publisher) = stream_table(graph, plan.definition().clone(), callbacks);
    std::thread::Builder::new()
        .name(format!("Kafka-{topic}"))
        .spawn(move || poll_loop(source, plan, publisher, stopped))
        .map_err(|e| KafkaError::Broker(format!("cannot start the polling thread: {e}")))?;
    let blink: Arc<dyn Table> = blink;
    match table_type {
        TableType::Blink => Ok(blink),
        // Behavior: io.deephaven.kafka.KafkaTools.TableType.Append, which is
        // BlinkTableTools#blinkToAppendOnly of the blink table
        TableType::Append => {
            let append = blink_to_append_only(&blink, u64::MAX);
            Ok(append.map_err(|failure| KafkaError::Invalid(failure.message))?)
        }
        // Behavior: io.deephaven.engine.table.impl.sources.ring.RingTableTools#of
        TableType::Ring(capacity) => {
            let ring = ring_table(&blink, capacity, true);
            Ok(ring.map_err(|failure| KafkaError::Invalid(failure.message))?)
        }
    }
}
