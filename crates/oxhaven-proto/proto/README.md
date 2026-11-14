# Vendored protocol definitions

These files are copied byte for byte from the reference release pinned in `oracle/REFERENCE.md`
(Deephaven Community Core 42.9) and from the two artifacts that release builds its protocol
against. They are the one exception to the rule against copying reference source: the wire
protocol has to match exactly. Do not edit them. Changing the pin is its own task, and replaces
these files wholesale.

| Path | Source | Role |
| --- | --- | --- |
| `deephaven-core/deephaven_core/proto/*.proto` | `deephaven/deephaven-core` tag `v42.9`, `proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/` | Deephaven gRPC services and messages |
| `deephaven-core/LICENSE`, `NOTICE` | same tag, `proto/proto-backplane-grpc/` | License of the above |
| `barrage/Barrage.fbs` | `deephaven/barrage` tag `v1.0.0`, `format/Barrage.fbs` | Barrage flatbuffers schema |
| `barrage/BrowserFlight.proto` | `io.deephaven.barrage:barrage-core:1.0.0` jar from Maven Central | BrowserFlight service |
| `barrage/LICENSE` | `deephaven/barrage` tag `v1.0.0` | License of the two files above |
| `arrow/Flight.proto` | `org.apache.arrow:flight-core:18.3.0` jar from Maven Central | Import of `BrowserFlight.proto` only |
| `arrow/LICENSE`, `NOTICE` | same jar, `META-INF/` | License of the above |

The Barrage and Arrow versions are the ones the reference pins in `gradle/libs.versions.toml` at
`v42.9` (`deephaven-barrage = "1.0.0"`, `arrow = "18.3.0"`). `BrowserFlight.proto` and
`Flight.proto` are taken from the published jars because that is where the reference build itself
takes them from (`proto/proto-backplane-grpc/build.gradle`, the `download` configuration).
`Barrage.fbs` is not in the published jars; the copy at tag `v1.0.0` is identical to the one at
commit `919d155c4652a32d40cea43c84deca87f05e4a43`, which the 1.0.0 jars record as their build
commit.

SHA-256 of the files that do not come from the reference checkout:

```
8d79b91ca1d89d5149b3232d83f3453f3687a1878ea9513f22acb96a4326a1c6  barrage/Barrage.fbs
50c50ff850377a66215a84a244834ecfa1e0328e443fd93f2a8be76ff2ad47da  barrage/BrowserFlight.proto
b5923f85465861ed28044608c369b6b744e7d987393645b4a6fed87574f66b39  arrow/Flight.proto
```

## How they are compiled

- The `.proto` files are compiled on every build by `build.rs` with `protox` and
  `tonic-prost-build`; no `protoc` is needed. `Flight.proto` only resolves the import: its types
  are mapped to the `arrow-flight` crate and no code generated from it is used.
- `Barrage.fbs` is compiled ahead of time and the output is checked in at
  `src/barrage/Barrage_generated.rs`, because `flatc` is a native program with no Rust
  equivalent. To regenerate it, from `crates/oxhaven-proto`:

  ```bash
  flatc --rust -o src/barrage proto/barrage/Barrage.fbs
  ```

  Use the `flatc` release whose version equals the `flatbuffers` crate version in the root
  `Cargo.toml` (currently 25.12.19, from
  <https://github.com/google/flatbuffers/releases/tag/v25.12.19>). Commit the output as is; it is
  excluded from `rustfmt`. `tests/barrage.rs` fails if the schema and the generated code disagree
  on tables or fields.
