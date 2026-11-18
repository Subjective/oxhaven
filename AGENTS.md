# AGENTS.md

Oxhaven is a Rust implementation of the Deephaven Community Core server: wire-compatible
(gRPC, Arrow Flight, Barrage), with a reimplemented engine.

Before changing anything, read, in this order:

1. `PORTING.md`: the rules for every change, including commit and pull request style.
2. `PARITY.md`: what "matches Java" means and which differences are allowed.
3. `crates/oxhaven-contracts`: the types and traits between crates.
4. Your task file in `tasks/`.

## Layout

| Path | Holds |
| --- | --- |
| `crates/oxhaven-contracts` | Shared types and traits. Protected. |
| `crates/oxhaven-testkit` | Reference implementations of the contracts, self-consistency checker |
| `crates/oxhaven-rowset` | Row sets, shift data, tracking row sets |
| `crates/oxhaven-column` | Arrow-backed column sources |
| `crates/oxhaven-engine` | Update graph, tables, table operations, tables read from storage |
| `crates/oxhaven-expr` | Formula parser, type checker, evaluator |
| `crates/oxhaven-parquet` | Parquet files as tables |
| `crates/oxhaven-csv` | CSV files read as tables and written from them |
| `crates/oxhaven-kafka` | Kafka topics consumed into tables (`docs/KAFKA.md`) |
| `crates/oxhaven-proto` | Generated protocol types |
| `crates/oxhaven-server` | gRPC services, sessions, Barrage |
| `bench/` | End-to-end benchmark against Deephaven (`docs/BENCHMARK.md`), soak test (`docs/SOAK.md`) |
| `oracle/` | Differential harness, fuzzer, recorded Java results. Protected. |
| `tasks/` | One file per task: scope, inputs, owned paths, gate command, status |

## Commands

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                           # quick seed budget, as on a pull request
OXHAVEN_TEST_SEEDS=full cargo test --workspace   # every seed, as in the nightly workflow
```
