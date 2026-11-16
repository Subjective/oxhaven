# Source map

Where each part of the Java reference lands in Oxhaven. Paths on the left are in
`deephaven/deephaven-core` at the tag pinned in `oracle/REFERENCE.md`. Task ids refer to `tasks/`.

"Not ported" means Rust needs no counterpart: the code exists to serve the JVM, or is generated
per primitive type and replaced by one generic implementation.

## Engine

| Java | Oxhaven | Tasks |
| --- | --- | --- |
| `engine/rowset` (`RowSet`, `WritableRowSet`, `RowSetShiftData`) | `oxhaven-contracts::rowset`, `::shift` | 0.4, 1.2 |
| `engine/rowset` (`TrackingRowSet`, builders) | `oxhaven-rowset` | 1.3 |
| `engine/rowset/impl/rsp`, `sortedranges`, `singlerange` | `oxhaven-rowset` internals, later | 7.7 |
| `engine/chunk` | Arrow arrays; not ported | |
| `engine/table/.../impl/sources` | `oxhaven-column` | 1.4 |
| `engine/api` (`TableUpdate`, `ModifiedColumnSet`, `TableDefinition`) | `oxhaven-contracts::update`, `::schema` | 0.4, 1.5 |
| `engine/api` (`Table`, `ColumnSource`, `TableUpdateListener`) | `oxhaven-contracts::table` | 0.4 |
| `engine/updategraph`, `PeriodicUpdateGraph`, `MergedListener` | `oxhaven-contracts::graph`, `oxhaven-engine::graph` | 0.4, 1.6 |
| `QueryTable`, `BaseTable`, `InMemoryTable`, `TimeTable`, input tables | `oxhaven-engine::table` | 1.8 |
| `TableUpdateValidator`, `EvalNugget`, `TstUtils` | `oxhaven-testkit` | 1.7 |
| `WhereListener`, `select/*Filter` | `oxhaven-engine::ops::filter` | 3.2 |
| `SortOperation`, `SortListener`, `SliceLikeOperation` | `oxhaven-engine::ops::sort` | 3.3 |
| `impl/sort`, `impl/ssa`, `impl/ssms` (generated kernels) | generic code inside the operators; not ported file by file | 3.3, 4.4 |
| `SelectOrUpdateListener`, `select/analyzers` | `oxhaven-engine::ops::select` | 3.4 |
| `TableToolsMergeHelper`, `Union*`, `FlattenOperation` | `oxhaven-engine::ops::merge` | 3.5 |
| `impl/by` | `oxhaven-engine::ops::agg` | 3.6, 4.1 |
| `NaturalJoinHelper`, `impl/naturaljoin` | `oxhaven-engine::ops::join` | 4.2 |
| `MultiJoinTableImpl`, `impl/multijoin` | `oxhaven-engine::ops::multi_join` | 4.12 |
| `CrossJoinHelper`, `impl/join` | `oxhaven-engine::ops::crossjoin` | 4.3 |
| `AsOfJoinHelper`, `impl/asofjoin` | `oxhaven-engine::ops::asof` | 4.4 |
| `impl/rangejoin` | `oxhaven-engine::ops::rangejoin` | 4.5 |
| `impl/updateby` | `oxhaven-engine::ops::updateby` | 4.6 |
| `UngroupOperation`, `engine/vector` | `oxhaven-engine::ops::ungroup` | 4.7 |
| `DynamicWhereFilter`, select-distinct | `oxhaven-engine::ops::distinct` | 4.8 |
| `impl/snapshot` | `oxhaven-engine::ops::snapshot` | 4.9 |
| `impl/hierarchical` (rollup, tree) | `oxhaven-engine::ops::hierarchical` | 7.8, 7.9 |
| `impl/partitioned` | `oxhaven-engine::ops::partitioned` | 7.6 |
| `impl/lang/QueryLanguageParser`, `select/DhFormulaColumn`, `ConditionFilter` | `oxhaven-expr` | 3.1, 5.2, 5.3, 5.5 |
| `engine/function`, `engine/time` | `oxhaven-expr::functions` | 5.4 |
| `TableDefaults#formatColumns`, `ColumnFormatting`, `SelectColumnFactory#getFormatExpression` | `oxhaven-engine::ops::select::format` | 4.21 |
| `engine/util/ColorUtilImpl`, `gui/color/Color` | `oxhaven-expr::functions::color`, `oxhaven-expr::types::color` | 4.21 |
| `engine/function` over vectors and arrays (`Numeric`, `Basic`, `Sort`), `engine/vector` methods in formulas | `oxhaven-expr::functions::vectors`, `oxhaven-engine::ops::select::ArrayFormula` | 5.6 |
| `engine/tuple`, `engine/tuplesource` | Rust tuples and generic keys; not ported | |
| `QueryCompiler*`, formula class generation, `engine/context` | not ported | |
| `impl/locations`, `sources/regioned`, `SourceTable`, `PartitionAwareSourceTable` | `oxhaven-engine::source` (`docs/STORAGE.md`); `impl/dataindex` not ported | 7.1 |

## Server and wire

| Java | Oxhaven | Tasks |
| --- | --- | --- |
| `proto/proto-backplane-grpc`, Barrage schema | `oxhaven-proto` | 0.6 |
| `server/session`, `server/auth` | `oxhaven-server::session` | 2.1 |
| `server/table/ops` | `oxhaven-server::table` | 2.2, then one handler per operator task |
| `server/arrow` (Flight; of `BrowserFlightServiceGrpcBinding` the `DoPut` pair) | `oxhaven-server::flight` | 2.3, 7.8 |
| `server/barrage`, `extensions/barrage` | `oxhaven-server::barrage` | 2.4 |
| `server/table/inputtables` | `oxhaven-server::inputtable` | 2.5 |
| `server/config`, `server/browserstreaming` | `oxhaven-server::config`, `::transport` | 2.6 |
| `server/console` | `oxhaven-server::console` | 6.1 (`6.1-console-design`), 6.2, 6.13 to 6.15 |
| `server/object`, `plugin/` | `oxhaven-server::object` | 7.10, 7.11 |
| `server/notebook` (storage service) | `oxhaven-server::storage` | 6.1 (`6.1-ide-services`), 6.17 |
| `server/hierarchicaltable` | `oxhaven-server::hierarchical` | 7.8, 7.9 |
| `server/partitionedtable` | `oxhaven-server::partitioned` | 7.6 |
| `server/appmode` | `oxhaven-server::application` | 6.18 |
| `py/server/deephaven` (Python API over jpy) | `deephaven` package over PyO3 | 6.1 (`6.1-console-design`), 6.3 to 6.12, 6.16 |
| Dagger modules, `Configuration`, `Base`, `Util`, `IO`, `log-factory` | standard crates; not ported | |

## Extensions

| Java | Oxhaven | Tasks |
| --- | --- | --- |
| `extensions/parquet` | `oxhaven-parquet` | 7.1, 7.2 |
| `extensions/csv`, the `deephaven-csv` library | `oxhaven-csv` | 7.3 |
| `extensions/kafka` | Kafka provider | 7.4, 7.5 |
| `extensions/iceberg`, `jdbc`, `flight-sql`, `protobuf`, `json`, `s3` | out of scope for now | |
| `Plot`, `py/server/deephaven/plot`, `server/plugin/type` (`FigureWidgetTypePlugin`) | `deephaven.plot` of the Python package, `oxhaven-server::figure`; plots of tables only (`docs/FIGURES.md`) | 7.11 |
| `engine/sql`, `sql/`, Groovy, Java plugins | out of scope | |

## Clients

Not ported. The Python, Java, C++, Go, R and JS clients and the web IDE run unmodified against
Oxhaven and serve as test drivers.
