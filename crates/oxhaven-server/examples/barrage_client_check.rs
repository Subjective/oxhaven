//! The Barrage client of this crate behind a line protocol, for the manual check against
//! a Deephaven server in `tasks/13.1-barrage-client/check.py`.
//!
//! ```text
//! barrage_client_check <host> <port> [<pre-shared key>]
//! ```
//!
//! Commands arrive on standard input, one per line, and each answers on standard output
//! with lines that end in a line `end`:
//!
//! | Command | Does |
//! | --- | --- |
//! | `snapshot <id> <variable>` | Snapshots the table of the server's variable and keeps it as `<id>` |
//! | `subscribe <id> <variable> [nulls] [list]` | Subscribes to it, with Deephaven nulls or columns as lists if asked |
//! | `keep <id> <blink id>` | Keeps every row a subscribed blink table shows, as `<id>` |
//! | `dump <id>` | Runs a cycle of the local graph and prints the table's rows as text |
//! | `digest <id>` | As `dump`, printing the number of rows and a hash of their text |
//! | `describe <id>` | Prints the table's column types and attributes |
//! | `drop <id>` | Forgets the table |
//! | `close` | Closes the session |
//!
//! The text of a cell is defined in `cell` below; `check.py` writes the same text for the
//! same value as `pydeephaven` reads it.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time64NanosecondType, TimeUnit, TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::graph::{locked, UpdateGraph};
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::table::blink_to_append_only;
use oxhaven_server::barrage_client::{
    scope_ticket, BarrageSession, ClientConfig, SnapshotOptions, SubscriptionOptions,
};

/// The text of one cell: the same for the same value however it arrived.
fn cell(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Boolean => array.as_boolean().value(row).to_string(),
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row).to_string(),
        DataType::Float32 => {
            let value = array.as_primitive::<Float32Type>().value(row);
            if value.is_nan() {
                "nan".to_string()
            } else {
                format!("f{:08x}", value.to_bits())
            }
        }
        DataType::Float64 => {
            let value = array.as_primitive::<Float64Type>().value(row);
            if value.is_nan() {
                "nan".to_string()
            } else {
                format!("d{:016x}", value.to_bits())
            }
        }
        DataType::Utf8 => {
            let text = array.as_string::<i32>().value(row);
            let mut out = String::from("'");
            for character in text.chars() {
                match character {
                    '\\' => out.push_str("\\\\"),
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    other => out.push(other),
                }
            }
            out
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .value(row)
            .to_string(),
        DataType::Date32 => array.as_primitive::<Date32Type>().value(row).to_string(),
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_primitive::<Time64NanosecondType>()
            .value(row)
            .to_string(),
        DataType::Duration(TimeUnit::Nanosecond) => array
            .as_primitive::<DurationNanosecondType>()
            .value(row)
            .to_string(),
        DataType::Binary => array
            .as_binary::<i32>()
            .value(row)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        DataType::Struct(_) => {
            let fields: Vec<String> = array
                .as_struct()
                .columns()
                .iter()
                .map(|field| cell(field.as_ref(), row))
                .collect();
            format!("{{{}}}", fields.join("|"))
        }
        DataType::List(_) => {
            let elements = array.as_list::<i32>().value(row);
            let texts: Vec<String> = (0..elements.len())
                .map(|index| cell(elements.as_ref(), index))
                .collect();
            format!("[{}]", texts.join(","))
        }
        other => format!("<{other}>"),
    }
}

/// The rows of `table` as lines of tab-separated cells, read under the graph's lock.
fn rows(table: &Arc<dyn Table>) -> Result<Vec<String>, String> {
    let read = || -> Result<Vec<String>, String> {
        if table.is_failed() {
            return Err("the table has failed".to_string());
        }
        let keys = table.row_set();
        let columns = (0..table.definition().columns().len())
            .map(|index| table.column(index).chunk(&keys))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|failure| failure.message)?;
        Ok((0..keys.size() as usize)
            .map(|row| {
                columns
                    .iter()
                    .map(|column| cell(column.as_ref(), row))
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect())
    };
    match table.update_graph() {
        Some(graph) => locked(graph.as_ref(), read),
        None => read(),
    }
}

/// FNV-1a over the lines, each followed by a line feed.
fn digest(lines: &[String]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for line in lines {
        for byte in line.bytes().chain(std::iter::once(b'\n')) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

struct Check {
    session: BarrageSession,
    graph: Arc<SteppedUpdateGraph>,
    tables: HashMap<String, Arc<dyn Table>>,
}

impl Check {
    fn table(&self, id: &str) -> Result<&Arc<dyn Table>, String> {
        self.tables
            .get(id)
            .ok_or_else(|| format!("no table '{id}'"))
    }

    async fn run(&mut self, words: &[&str]) -> Result<Vec<String>, String> {
        match words {
            ["snapshot", id, variable] => {
                let table = self
                    .session
                    .snapshot(&scope_ticket(variable), SnapshotOptions::resolver())
                    .await
                    .map_err(|problem| problem.to_string())?;
                self.tables.insert(id.to_string(), table);
                Ok(vec![])
            }
            ["subscribe", id, variable, flags @ ..] => {
                let options = SubscriptionOptions {
                    use_deephaven_nulls: flags.contains(&"nulls"),
                    columns_as_list: flags.contains(&"list"),
                    min_update_interval_ms: 50,
                    ..SubscriptionOptions::default()
                };
                let table = self
                    .session
                    .subscribe(&scope_ticket(variable), options)
                    .await
                    .map_err(|problem| problem.to_string())?;
                self.tables.insert(id.to_string(), table);
                Ok(vec![])
            }
            ["keep", id, blink] => {
                let kept = blink_to_append_only(self.table(blink)?, u64::MAX)
                    .map_err(|failure| failure.message)?;
                self.tables.insert(id.to_string(), kept);
                Ok(vec![])
            }
            ["dump", id] => {
                self.graph.run_cycle();
                rows(self.table(id)?)
            }
            ["digest", id] => {
                self.graph.run_cycle();
                let lines = rows(self.table(id)?)?;
                Ok(vec![format!("{} {:016x}", lines.len(), digest(&lines))])
            }
            ["describe", id] => {
                let table = self.table(id)?;
                let attributes = table.attributes();
                let mut lines: Vec<String> = table
                    .definition()
                    .columns()
                    .iter()
                    .map(|column| format!("{} {:?}", column.name, column.column_type))
                    .collect();
                lines.push(format!(
                    "refreshing={} blink={} add_only={} append_only={} flat={}",
                    table.is_refreshing(),
                    attributes.blink,
                    attributes.add_only,
                    attributes.append_only,
                    table.is_flat()
                ));
                Ok(lines)
            }
            ["drop", id] => {
                self.tables.remove(*id);
                Ok(vec![])
            }
            ["close"] => {
                self.session
                    .close()
                    .await
                    .map_err(|problem| problem.to_string())?;
                Ok(vec![])
            }
            other => Err(format!("unknown command: {}", other.join(" "))),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let (host, port, key) = match arguments.as_slice() {
        [host, port] => (host, port.parse::<u16>()?, None),
        [host, port, key] => (host, port.parse::<u16>()?, Some(key)),
        _ => return Err("usage: barrage_client_check <host> <port> [<pre-shared key>]".into()),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let graph = SteppedUpdateGraph::new();
    let mut config = ClientConfig::new(host.clone(), port);
    if let Some(key) = key {
        config = config.with_psk(key);
    }
    let local: Arc<dyn UpdateGraph> = graph.clone();
    let session = runtime.block_on(BarrageSession::connect(config, local, None))?;
    let mut check = Check {
        session,
        graph,
        tables: HashMap::new(),
    };
    let mut out = std::io::stdout().lock();
    writeln!(out, "ready")?;
    out.flush()?;
    for line in std::io::stdin().lock().lines() {
        let line = line?;
        let words: Vec<&str> = line.split_whitespace().collect();
        if words.is_empty() {
            continue;
        }
        match runtime.block_on(check.run(&words)) {
            Ok(lines) => {
                for line in lines {
                    writeln!(out, " {line}")?;
                }
                writeln!(out, "end")?;
            }
            Err(problem) => writeln!(out, "error {}", problem.replace('\n', " "))?,
        }
        out.flush()?;
    }
    Ok(())
}
