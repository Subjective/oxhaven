# End-to-end benchmark against Deephaven

How fast Oxhaven runs table operations compared with the Java reference, Deephaven 42.9, both
driven the same way by a client over gRPC. Task 8.1 (`tasks/8.1-benchmark.md`) built the
runner, `bench/bench.py`, and made the first measurement below. The only earlier measurement is
the row set micro-benchmark in [ROWSET.md](ROWSET.md).

The short reading: every case gave the same answer on both servers. At 1M rows Oxhaven is
within about 2x of Java in the less loaded run (up to 4x in the other) on formulas, filters, low- and medium-cardinality aggregations,
`join`, `head`/`tail` and `ungroup`, and faster on the Arrow Flight read path and on a string
match filter. It is 18x to 40x slower on `natural_join` and `exact_join`, 8x to 31x on
`select_distinct`, 4x to 10x on `sort` of an int column and on `last_by`, and 3x to 7x on
high-cardinality aggregations and `group_by`. At 10M rows the formulas and filters fall to 3x
to 8x, mostly because Java runs them on 2 threads. The slow paths share one cause: they keep
per-row state in ordered trees of heap-allocated keys (`BTreeMap`, `BTreeSet`, boxed byte
strings) where Java uses flat arrays and open-addressed hash tables. Oxhaven was written
correct-first (PORTING.md, rule 5) and has never been tuned; Java has. The machine was shared
with other jobs during both runs, so ratios under about 2x are within the noise.

Since then, task 9.1 has made `natural_join` and `exact_join` 14x to 88x faster than they
were in this first measurement, which brings them to Java's times or below them; see
[After task 9.1](#after-task-91-natural-and-exact-joins-2026-11-17). Task 9.3 has made
`sort` 3x to 19x faster, below Java's earlier times at both sizes; see
[After task 9.3](#after-task-93-2026-11-17-sort-and-reads-through-a-row-redirection). The tables and the
reading of the first measurement are kept below as they were written.

## What is measured

One **case** is an operation on a table that lives on the server. A case runs in three parts:

1. **Setup**, not timed: the case's input is made on the server. For every static case it is a
   fresh `head(n)` of the source table, so that no repetition reuses another's result: Java
   memoizes `where`, `sort`, aggregations and `select` of plain columns per parent table
   (`QueryTable#memoizeResult`), and `head` (`SliceLikeOperation`) is not memoized.
2. **The operation**, timed at the client with a monotonic clock from the request to its
   completion, as below.
3. **The check**, not timed, after the last repetition: the result's row count, the sum of each
   numeric column, a sum of the first numeric column weighted by row position modulo 1009 (so
   that row order counts), and the first and last three rows. The case is reported as the same
   result only when every part agrees on both servers (floating point sums within a relative
   1e-9).

Each case runs `warm-up` untimed repetitions and then the timed ones; the table reports the
median, minimum and maximum of the timed ones. The cases run interleaved: case 1 on Java, case
1 on Oxhaven, case 2 on Java, and so on, so that a change of the machine's load affects both.

**When an operation is complete.** For every case that returns a table, the client call
returns when the server has answered the export with `ExportedTableCreationResponse`, which
carries the result's size. Both servers send it only after the operation has made its result
table. What "made" means is the same on both: `update` stores its formula columns (Java
`QueryTable#update`, Oxhaven `ops::select`), and filters, sorts, aggregations and joins compute
their row sets, permutations, groups and redirections before they answer; the values of a
sorted or joined column are read through a redirection when someone reads them, on both. The
other two kinds of case say what they wait for in the table below.

| case | input (setup) | timed operation | complete when |
| --- | --- | --- | --- |
| `build` | none | `empty_table(n).update(` the 7 source formulas `)` | export answered |
| `update_arith` | source | `update(["C = A * 2 + 1", "D = B * 1.5 - A", "E = Id % 7"])` | export answered |
| `update_string` | source | `update(['S2 = S + "_x"'])` | export answered |
| `update_cond` | source | `update(["F = A > 50000 ? B : -B"])` | export answered |
| `where_range` | source | `where("A >= 20000 && A < 60000")` (40% of rows) | export answered |
| `where_string` | source | `` where("S in `s1`, `s42`, `s999`") `` (0.3%) | export answered |
| `where_conj` | source | `where(["K10 = 3", "B > 50000.0", "A < 50000"])` | export answered |
| `sort_int`, `sort_two`, `sort_string` | source | `sort("A")`, `sort(["K10", "B"])`, `sort("S")` | export answered |
| `sum_by_*`, `avg_by_*`, `count_by_*` | `view(["K = <key>", "A", "B"])` of the source | `sum_by("K")`, `avg_by("K")`, `count_by("N", "K")` | export answered |
| `agg_by_multi` | source | `agg_by([sum, avg, min, max, count], by="K10k")` | export answered |
| `last_by` | source | `last_by("K10k")` (all 7 columns) | export answered |
| `natural_join_small`, `exact_join_small` | source | join on `K10k` to a right table of 10,000 unique keys | export answered |
| `natural_join_large`, `exact_join_large` | source | join on `K1M` to a right table of 1,000,000 unique keys | export answered |
| `join_on_key` | source | `join(right, on="K10k", joins="R1")`, right table of 10,000 unique keys | export answered |
| `aj` | source | `aj(right, on=["K10", "Id >= Stamp"])`, right table of n/10 rows | export answered |
| `select_distinct_one`, `select_distinct_two` | source | `select_distinct("K10k")`, `select_distinct(["K10", "S"])` (10,000 groups each) | export answered |
| `head_half`, `tail_half` | source | `head(n/2)`, `tail(n/2)` | export answered |
| `group_by` | `view(["K10k", "A"])` of the source | `group_by("K10k")` | export answered |
| `ungroup` | `view(["K10k", "A"]).group_by("K10k")`, made once | `ungroup("A")` (n rows back) | export answered |
| `doget_1m` | the first 1M rows of `Id`, `A`, `B` | `to_arrow()`: Arrow Flight DoGet | the client has every record batch |
| `ticking_append_where_sum_by` | an append-only input table `(K int, V long)` with `where("V % 2 == 0").sum_by("K")` downstream; a batch of 100,000 rows imported | add the batch, wait for the barrier, fetch the 100-row result | see "Ticking" |

The source table has n rows:

```
Id   = ii                                    long, 0..n-1
K10  = (int)(ii % 10)                        10 keys
K10k = (int)((ii * 7919) % 10000)            10,000 keys, spread
K1M  = (int)((ii * 104729) % 1000000)        1,000,000 keys, spread (all unique at n = 1M)
A    = (int)((ii * 31337) % 100003)          int
B    = ((ii * 15485863) % 1000003) / 7.0     double
S    = "s" + (ii % 1000)                     string, 1,000 values
```

"low", "med" and "high" in the aggregation case names are `K10`, `K10k` and `K1M`.

### Ticking

One ticking case only, because it is the one that both servers can be driven through
identically and whose end can be observed from the client without timing the update graph's
clock. An input table without keys gets a fixed batch of 100,000 rows per repetition (imported
before the timer starts), and a `where` + `sum_by` over 100 keys is kept up to date downstream.
The timer covers the `AddTableToInputTable` call, then two changes to a second, private input
table (the oracle's barrier, `_Barrier` in `oracle/harness/runner.py`: when the second returns,
the cycle that applied the batch has finished on both servers), then a DoGet of the 100-row
result. The check compares the fetched sums with the sums the client computes for every row
added so far. Both servers start a cycle as soon as an input table is changed
(`PeriodicUpdateGraph#requestRefresh` and its Oxhaven counterpart), so the time is not the
1-second cycle period. It is dominated by round trips and cycle overhead, not by the 100,000-row
`where` and `sum_by`, so it says little about incremental throughput; a `time_table` was left
out because nothing can make the two servers tick in step.

## How to run it

```bash
# Python environment: the oracle's pins (oracle/requirements.txt), in a venv anywhere.
python3 -m venv /tmp/bench-venv && /tmp/bench-venv/bin/pip install -r oracle/requirements.txt

# Java: the oracle's own way (oracle/bin/start-java.sh), here with a larger heap for 10M rows.
ORACLE_HEAP=5g oracle/bin/start-java.sh 10868          # needs oracle/.venv; or run
# oracle/bin/java_server.py --port 10868 --psk oxhaven --heap 5g with the venv's python

# Oxhaven: a release build, without the Python console (the cases need no console).
cargo build --release -p oxhaven-server
target/release/oxhaven-server --host 127.0.0.1 --port 10869 --psk oxhaven &

/tmp/bench-venv/bin/python bench/bench.py run \
    --server java=dh+psk://oxhaven@127.0.0.1:10868 \
    --server oxhaven=dh+psk://oxhaven@127.0.0.1:10869 \
    --out run1.json                       # --sizes, --warmup, --reps, --cases 'sort_*,aj'
/tmp/bench-venv/bin/python bench/bench.py report run1.json run2.json            # min-max, load
/tmp/bench-venv/bin/python bench/bench.py report --compact run1.json run2.json  # medians only
```

The first `--server` is the base of the ratio. The defaults are 1M and 10M rows, 2 warm-up
and 5 timed repetitions at 1M rows, 1 and 3 at 10M rows; one run took 14 to 15 minutes on the
machine below. A failed case is recorded, not dropped: `refused` for `UNIMPLEMENTED` or
`INVALID_ARGUMENT`, `error` otherwise, with the code and message in the JSON.

## The machine and the servers

Measured on 2026-11-17, two complete runs one after the other (run 1 10:36 to 10:51, run 2
10:52 to 11:06), on the same two server processes, started once before run 1.

- **Machine:** 4 vCPUs (Intel Xeon @ 2.10 GHz), 15 GB RAM, Linux 6.18. Client and both
  servers on the same host, over loopback.
- **Load: not clean.** Other agents shared the machine throughout: an engine test build and its
  test binary (up to 3 cores), and for part of the time a fuzzer against another Java server.
  The one-minute load average, recorded before each case, was 2.9 to 3.9 during the 1M-row cases
  of run 1, 4.2 to 8.2 during those of run 2, and 3.4 to 9.2 during the 10M-row cases of both
  runs; the benchmark itself, one busy server and the client, accounts for one to two of that. With 4 cores, a single-threaded operation got
  between half a core and a whole one. Read differences under about 1.5x between the runs, and
  ratios under about 2x, as noise. The full table (`bench/results/2026-11-17.md`) has the
  min-max of every case and the load for each case and run.
- **Java:** Deephaven 42.9, the `deephaven-server` wheel of `oracle/requirements.txt`, started
  as the oracle starts it (`oracle/bin/java_server.py`: `-XX:ActiveProcessorCount=2`, PSK
  authentication) but with a 5 GB heap (`--heap 5g`) instead of the oracle's 2 GB, chosen for the 10M-row
  tables; the resident size of the Java process reached 4.6 GB during them. 2 GB was not tried. OpenJDK 21.0.12.1 with its default collector, Deephaven's default
  configuration otherwise, so its operation thread pool and update graph have 2 threads.
- **Oxhaven:** commit `c24fd2a2` (main at `6f744d8f` plus the runner), built with
  `cargo build --release --locked -p oxhaven-server` and the default release profile (no LTO,
  debug assertions off), without the `python` feature, run with default options
  (`--cycle-ms 1000`). The gRPC runtime is Tokio's default multi-threaded one (7 threads in the
  process on 4 cores); the engine has no parallel code, so a table operation computes on one thread.
- **Client:** pydeephaven 42.9 and pyarrow 26.0.1 on Python 3.13, sessions opened with
  `oracle/harness/connect.py`.
- **Repetitions:** 2 warm-up and 5 timed at 1M rows; 1 warm-up and 3 timed at 10M rows.

## Results

Every case gave the same result on both servers in both runs; no case was refused and none
failed.

Median milliseconds of each run, run 1 / run 2. Ratio = oxhaven / java; above 1 means oxhaven is slower.

### 1,000,000 rows

| case | java | oxhaven | ratio | same result |
| --- | ---: | ---: | ---: | --- |
| `build` | 173 / 139 | 328 / 397 | 1.90 / 2.87 | yes / yes |
| `update_arith` | 43.7 / 31.8 | 54.9 / 73.5 | 1.26 / 2.31 | yes / yes |
| `update_string` | 100 / 52.8 | 172 / 209 | 1.72 / 3.96 | yes / yes |
| `update_cond` | 17.7 / 26.5 | 24.6 / 25.1 | 1.39 / 0.95 | yes / yes |
| `where_range` | 32.0 / 15.0 | 28.9 / 42.3 | 0.90 / 2.81 | yes / yes |
| `where_string` | 16.6 / 18.3 | 8.9 / 9.5 | 0.54 / 0.52 | yes / yes |
| `where_conj` | 17.9 / 14.3 | 21.9 / 24.3 | 1.22 / 1.70 | yes / yes |
| `sort_int` | 116 / 153 | 607 / 619 | 5.25 / 4.04 | yes / yes |
| `sort_two` | 232 / 251 | 529 / 623 | 2.28 / 2.48 | yes / yes |
| `sort_string` | 280 / 317 | 416 / 451 | 1.48 / 1.42 | yes / yes |
| `sum_by_low` | 20.1 / 18.9 | 38.2 / 37.2 | 1.89 / 1.97 | yes / yes |
| `avg_by_low` | 21.2 / 20.1 | 33.5 / 36.8 | 1.58 / 1.83 | yes / yes |
| `count_by_low` | 13.6 / 17.7 | 27.6 / 38.2 | 2.03 / 2.16 | yes / yes |
| `sum_by_med` | 47.9 / 50.5 | 60.6 / 88.5 | 1.26 / 1.75 | yes / yes |
| `avg_by_med` | 53.6 / 60.7 | 81.2 / 91.5 | 1.52 / 1.51 | yes / yes |
| `count_by_med` | 20.6 / 18.3 | 45.8 / 67.3 | 2.22 / 3.68 | yes / yes |
| `sum_by_high` | 183 / 194 | 1126 / 1080 | 6.17 / 5.58 | yes / yes |
| `avg_by_high` | 194 / 199 | 1018 / 1161 | 5.25 / 5.83 | yes / yes |
| `count_by_high` | 106 / 133 | 807 / 927 | 7.62 / 6.98 | yes / yes |
| `agg_by_multi` | 76.2 / 103 | 124 / 130 | 1.62 / 1.27 | yes / yes |
| `last_by` | 20.8 / 23.0 | 207 / 193 | 9.95 / 8.36 | yes / yes |
| `natural_join_small` | 28.7 / 25.4 | 655 / 699 | 22.82 / 27.58 | yes / yes |
| `natural_join_large` | 125 / 125 | 2325 / 2578 | 18.57 / 20.59 | yes / yes |
| `exact_join_small` | 25.9 / 24.7 | 680 / 687 | 26.30 / 27.86 | yes / yes |
| `exact_join_large` | 140 / 123 | 2575 / 2455 | 18.45 / 19.92 | yes / yes |
| `join_on_key` | 365 / 314 | 594 / 586 | 1.63 / 1.87 | yes / yes |
| `aj` | 213 / 185 | 603 / 535 | 2.82 / 2.89 | yes / yes |
| `select_distinct_one` | 16.6 / 13.8 | 323 / 276 | 19.50 / 19.96 | yes / yes |
| `select_distinct_two` | 30.4 / 29.5 | 285 / 249 | 9.36 / 8.44 | yes / yes |
| `head_half` | 2.0 / 1.8 | 0.87 / 0.91 | 0.43 / 0.51 | yes / yes |
| `tail_half` | 2.3 / 1.9 | 1.1 / 1.0 | 0.47 / 0.54 | yes / yes |
| `group_by` | 109 / 79.6 | 366 / 314 | 3.35 / 3.94 | yes / yes |
| `ungroup` | 5.7 / 2.5 | 2.0 / 3.9 | 0.34 / 1.54 | yes / yes |
| `doget_1m` | 73.9 / 64.7 | 29.6 / 29.6 | 0.40 / 0.46 | yes / yes |
| `ticking_append_where_sum_by` | 46.9 / 54.9 | 79.6 / 96.7 | 1.70 / 1.76 | yes / yes |

### 10,000,000 rows

| case | java | oxhaven | ratio | same result |
| --- | ---: | ---: | ---: | --- |
| `build` | 1738 / 2577 | 5398 / 5397 | 3.11 / 2.09 | yes / yes |
| `update_arith` | 179 / 355 | 1050 / 993 | 5.87 / 2.80 | yes / yes |
| `update_string` | 1094 / 1460 | 2145 / 2145 | 1.96 / 1.47 | yes / yes |
| `update_cond` | 71.9 / 89.7 | 392 / 401 | 5.46 / 4.47 | yes / yes |
| `where_range` | 52.1 / 48.5 | 428 / 402 | 8.22 / 8.29 | yes / yes |
| `where_string` | 121 / 161 | 82.0 / 96.2 | 0.68 / 0.60 | yes / yes |
| `where_conj` | 92.9 / 80.3 | 274 / 267 | 2.95 / 3.33 | yes / yes |
| `sort_int` | 1175 / 1255 | 11775 / 11292 | 10.02 / 9.00 | yes / yes |
| `sort_two` | 2779 / 2810 | 11711 / 11201 | 4.21 / 3.99 | yes / yes |
| `sort_string` | 2546 / 2372 | 6837 / 6652 | 2.69 / 2.80 | yes / yes |
| `sum_by_low` | 166 / 166 | 332 / 288 | 2.00 / 1.73 | yes / yes |
| `avg_by_low` | 205 / 158 | 325 / 332 | 1.59 / 2.11 | yes / yes |
| `count_by_low` | 144 / 103 | 329 / 226 | 2.29 / 2.18 | yes / yes |
| `sum_by_med` | 589 / 379 | 817 / 550 | 1.39 / 1.45 | yes / yes |
| `avg_by_med` | 574 / 480 | 892 / 692 | 1.55 / 1.44 | yes / yes |
| `count_by_med` | 265 / 150 | 469 / 351 | 1.77 / 2.35 | yes / yes |
| `sum_by_high` | 912 / 871 | 3496 / 2813 | 3.84 / 3.23 | yes / yes |
| `avg_by_high` | 1392 / 1021 | 3360 / 2761 | 2.41 / 2.70 | yes / yes |
| `count_by_high` | 498 / 437 | 2691 / 2448 | 5.40 / 5.60 | yes / yes |
| `agg_by_multi` | 882 / 743 | 1353 / 1409 | 1.53 / 1.89 | yes / yes |
| `last_by` | 258 / 221 | 2120 / 1928 | 8.20 / 8.73 | yes / yes |
| `natural_join_small` | 344 / 297 | 9510 / 8522 | 27.62 / 28.70 | yes / yes |
| `natural_join_large` | 584 / 514 | 20378 / 17393 | 34.90 / 33.84 | yes / yes |
| `exact_join_small` | 243 / 212 | 9804 / 7920 | 40.39 / 37.30 | yes / yes |
| `exact_join_large` | 674 / 500 | 19910 / 16681 | 29.56 / 33.39 | yes / yes |
| `join_on_key` | 3982 / 2355 | 7532 / 6350 | 1.89 / 2.70 | yes / yes |
| `aj` | 2677 / 1822 | 6570 / 5729 | 2.45 / 3.14 | yes / yes |
| `select_distinct_one` | 103 / 100 | 3201 / 2767 | 31.15 / 27.58 | yes / yes |
| `select_distinct_two` | 321 / 249 | 2680 / 2385 | 8.34 / 9.56 | yes / yes |
| `head_half` | 2.7 / 3.6 | 0.85 / 4.0 | 0.32 / 1.10 | yes / yes |
| `tail_half` | 1.7 / 1.5 | 4.0 / 4.2 | 2.40 / 2.88 | yes / yes |
| `group_by` | 1209 / 934 | 8797 / 6709 | 7.28 / 7.19 | yes / yes |
| `ungroup` | 2.9 / 2.3 | 17.8 / 9.2 | 6.15 / 4.06 | yes / yes |
| `doget_1m` | 88.7 / 70.5 | 39.1 / 44.2 | 0.44 / 0.63 | yes / yes |
| `ticking_append_where_sum_by` | 51.4 / 39.6 | 113 / 96.4 | 2.21 / 2.44 | yes / yes |

## After task 9.1: natural and exact joins (2026-11-17)

Task 9.1 (`tasks/9.1-fast-joins.md`) rewrote how `natural_join` and `exact_join` find their
matches and how a result's row redirection is stored. This block measures that change only.

**What was compared.** Two Oxhaven servers on the machine above, run by the same runner with
the join cases only (`--cases 'natural_join_*,exact_join_*,join_on_key,aj'`), interleaved case
by case: "before" is `main` at `03f8dbb8`, "after" is the branch of task 9.1, both built with
`cargo build --release --locked -p oxhaven-server`. Two complete runs, 12:08 to 12:15 and
12:15 to 12:23, with other agents building on the machine (one-minute load 2.7 to 4.4 in run
1 and 5.0 to 7.6 in run 2). Every case gave the same result on both servers in both runs.
Java was not run again: the "java (earlier)" column repeats the medians of the first
measurement above, taken two hours earlier under another load, so the last column is an
indication and not a measurement of one run. The raw results and the table with min-max and
load per case are in `bench/results/2026-11-17-task-9.1*`.

Median milliseconds of each run, run 1 / run 2.

| case | rows | before | after | before / after | java (earlier) | after / java (earlier) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `natural_join_small` | 1M | 692 / 790 | 7.9 / 10.5 | 88 / 75 | 28.7 / 25.4 | 0.3 to 0.4 |
| `natural_join_large` | 1M | 2286 / 2521 | 122 / 180 | 19 / 14 | 125 / 125 | 1.0 to 1.4 |
| `exact_join_small` | 1M | 640 / 705 | 8.4 / 8.8 | 76 / 80 | 25.9 / 24.7 | 0.3 to 0.4 |
| `exact_join_large` | 1M | 2115 / 2614 | 139 / 154 | 15 / 17 | 140 / 123 | 1.0 to 1.3 |
| `join_on_key` | 1M | 536 / 645 | 536 / 671 | 1.0 / 1.0 | 365 / 314 | 1.5 to 2.1 |
| `aj` | 1M | 474 / 595 | 441 / 475 | 1.1 / 1.3 | 213 / 185 | 2.1 to 2.6 |
| `natural_join_small` | 10M | 7352 / 8528 | 146 / 194 | 50 / 44 | 344 / 297 | 0.4 to 0.7 |
| `natural_join_large` | 10M | 16780 / 18570 | 558 / 698 | 30 / 27 | 584 / 514 | 1.0 to 1.4 |
| `exact_join_small` | 10M | 8068 / 8804 | 161 / 174 | 50 / 51 | 243 / 212 | 0.7 to 0.8 |
| `exact_join_large` | 10M | 15182 / 18875 | 581 / 643 | 26 / 29 | 674 / 500 | 0.9 to 1.3 |
| `join_on_key` | 10M | 6514 / 7448 | 6400 / 6903 | 1.0 / 1.1 | 3982 / 2355 | 1.6 to 2.9 |
| `aj` | 10M | 6356 / 6518 | 5326 / 5667 | 1.2 / 1.2 | 2677 / 1822 | 2.0 to 3.1 |

**What changed.**

- The keys of both tables are numbered by a hash table, a chunk of key columns at a time
  (`ops/join/key_table.rs`): a single primitive, boolean or time column as 64-bit integers kept
  in the table itself, a single string column as bytes borrowed from the chunk, and any other
  combination as rows of Arrow's row format. No key is allocated per row, and the hash has a
  fixed seed.
- A join of two static tables makes one pass over each table and keeps nothing per key or per
  row but the redirection. With a refreshing left table and a static right table it keeps the
  first and last right row per key. Only a refreshing right table makes it keep the left rows
  of each key, and a key with one row costs no set.
- `WritableRowRedirection` (`oxhaven-column/src/redirect.rs`) keeps its mappings in a flat
  array indexed by row key while the keys are dense (at most four array cells per mapping, the
  factor of Java's `JoinControl#getRedirectionType`) and in an ordered map otherwise, and moves
  between the two by itself; previous mappings are kept apart for the cycle, as in Java's
  `ContiguousWritableRowRedirection`. A whole result's redirection is made in one piece, and for
  the row keys `0..n` it is the vector of right row keys itself. Every operator that uses a
  redirection gets the array without a change of its own, which is why `aj` is 7% to 20%
  faster here.

**Where the time went, and where it goes now.** The first measurement's reading of the cause
was right in kind. Measured with `cargo run --release -p oxhaven-engine --example join-phases`
on the same machine (one run of the code before and two of the code after, at a load of 4.6 to 7.8), for 10M left rows and 10,000 right keys:

| part | before | after |
| --- | ---: | ---: |
| read the left keys into one `Vec<Cell>` per row | 1047 ms | none |
| left rows into the ordered map and the per-key sets | 5355 ms | none |
| look up the left keys in the hash table | none | 95 to 133 ms |
| right row of each left row | (in the map time) | 50 to 58 ms |
| the redirection | 762 ms (10M `BTreeMap` inserts) | 14 to 16 ms (one vector) |
| the whole join | 7632 ms | 214 to 229 ms |

So the ordered map with its sets was 70% of the time, the keys 14% and the redirection 10%.
The other shapes of the example, whole joins of 10M left rows, before and after: 1,000,000
right keys 15931 ms and 828 to 1255 ms; a string key with 1,000 values 6196 ms and 175 to
255 ms; an int and a string key together (the row-format path) 10631 ms and 471 to 490 ms; a
refreshing left table 7608 ms and 519 to 546 ms; a refreshing right table 7367 ms and 328 to
390 ms, and ten cycles that each add 100 right rows and so change 100,000 result rows 1368 ms
and 325 to 373 ms. In a debug build the 1M-row join with 10,000 right keys went from 2416 ms
to 185 ms and the one with 1,000,000 right keys from 7438 ms to 1400 ms.

**What did not get faster.** `join_on_key` is unchanged: the cross join (`ops/crossjoin`) has
its own keyed state and still reads a `Vec<Cell>` per row. `aj` gained only what the flat
redirection gives it. The times above are for making the join; reading an added column of all
10M rows back through the redirection takes 256 to 331 ms (666 to 737 ms before), because
`RedirectedSource` gathers every row key into a vector and then takes the values one by one.

**The next bottlenecks of these joins.** With 1,000,000 right keys, most of the time
is the lookup of 10M left keys in a table that no longer fits in cache and the second
random read, of the key's right row, that follows it; Java's time is the same within the noise, so there is
little left without a change of method (partitioning the keys, or prefetching). For a
refreshing right table it is the set of left rows per key (a `BTreeSet` each) and one lock and
one ordered-map insertion per changed row of the redirection. A join's key table also never
forgets a key, as Java's does not; the ordered map it replaces dropped a key when its last row
left.

## Reading the results

No profiler was available on the machine (`perf` is not installed), so the causes below come
from reading the code path of each slow case, not from a profile. They are the likely
reasons, not measured ones.

**Close to Java, or faster (ratio below about 2 in run 1, the less loaded run at 1M rows,
and below 4 in run 2):** the formulas of `update` at 1M rows, `where_string` (a match filter
on the string column, faster on Oxhaven at both sizes), low and medium cardinality `sum_by`/`avg_by`/`count_by`, `agg_by_multi`,
`sort_string` at 1M rows, `join_on_key`, `head`/`tail`, and `doget_1m`: Oxhaven sends 1M rows
over Arrow Flight in about 30-45 ms against Java's 65-90 ms, because its columns already are
Arrow arrays and Java converts its chunks to Arrow buffers. The ticking case is 1.7x to 2.4x,
40-55 ms against 80-115 ms, most of which is round trips and cycle overhead on both.

**Slower, and why.** The worst cases, slowest first:

1. **`natural_join` and `exact_join`: 18x to 40x.** (Fixed by task 9.1; see "After task
   9.1" above. What follows describes the code before it.) `ops/join/natural.rs` (`NaturalJoin::start`)
   reads every row's key into a `Vec<Cell>` (`ops/join/keys.rs`: one heap allocation per row,
   with an enum per cell), keeps the keys in a `BTreeMap<Vec<Cell>, Slot>` in which every slot
   has a `BTreeSet` of its left rows and one of its right rows, clones the key once more per
   left row, and puts each left row's match into a `WritableRowRedirection`
   (`oxhaven-column/src/redirect.rs`), which is a `BTreeMap<RowKey, RowKey>` with a second
   map for previous values (`Tracked`). So a 10M-row static join is about 10M allocations, 20M
   ordered-tree insertions of variable-length keys and 10M more into the redirection. Java hashes
   the typed key chunk into an open-addressed table generated per key type and writes the
   redirection into a flat array, and does not keep the left rows per key for a static join.
   The per-left-row state is needed for a refreshing join only.
2. **`select_distinct`: 8x to 31x.** `ops/distinct/mod.rs` turns each row's key into an owned
   `Vec<u8>` (Arrow's row format) and looks it up in a `BTreeMap<Vec<u8>, usize>`, and records
   the group in a `BTreeMap` of touched groups, for every row, also when the table is static.
   Two ordered-tree operations and one allocation per row against Java's hash probe on a typed
   chunk.
3. **`sort`: 4x to 10x, and growing faster than the row count** (`sort_int` 0.6 s at 1M rows,
   11.5 s at 10M). `ops/sort/sorted.rs` (`sorted_entries`) encodes each row's key into its own
   `Box<[u8]>` (`ops/sort/keys.rs`), sorts a vector of (row key, box) entries with a stable
   comparison sort over the boxed bytes, so each comparison follows two pointers to memory that
   is not in cache, and then writes the permutation into a `WritableRowRedirection`, a
   `BTreeMap`, one insertion per row. Java sorts a typed chunk of values with the row keys
   alongside (a timsort per primitive type) and keeps the permutation in a flat array. The
   superlinear growth is what pointer chasing through 10M boxes and a 10M-entry tree do once
   they no longer fit in cache.
4. **`last_by`, `group_by`, and aggregations with 1M keys: 3x to 10x.** The aggregation
   engine (`ops/agg/aggregator.rs`) is chunked like Java's, but each chunk's keys are encoded with
   Arrow's `RowConverter` and looked up in a `HashMap<Box<[u8]>, usize>` with the standard
   library's SipHash (`ops/agg/keys.rs`), a new key costs an allocation, and the operators are
   called once per run of equal slots through a trait object, which costs most when runs are a
   row or two long, as with 1M keys. Every result column is a `WritableSource`
   (`oxhaven-column/src/writable.rs`), whose cells are again entries of a `BTreeMap` keyed by
   row key, so 1M groups is 1M tree insertions per result column. `last_by` keeps the last value
   of each of its 6 value columns per group as a `Scalar` (`ops/agg/first_last.rs`), which for
   a string is a new `Arc<str>` per run. `group_by` does more than Java at creation: it gathers
   every value of every group into one Arrow list array (`ops/agg/group.rs`, `recompute` and
   `results`), where Java's grouped columns are vectors read through each group's row set when
   someone reads them. That is a difference of what "complete" means for this one case, in
   Java's favour; it is also what Oxhaven's later reads of the column do not pay.

**Larger at 10M rows than at 1M.** `update_arith`, `update_cond` and `where_range` are within
about 1.5x of Java at 1M rows but 3x to 8x at 10M. Two reasons, both expected: Java parallelises
`where` over 2^16-row segments and `update` above 2^22 rows on its 2 threads, which pays only at
10M; and Oxhaven evaluates a formula or condition over the whole selection in one call
(`ops/filter/filters.rs`, the `Resolved::Condition` arm), so each intermediate array is 10M
values long and is written to and read back from memory, where Java's kernels work in chunks of
4,096 that stay in cache. The 10M-row `build` (3x) is the same formulas plus the string column.

**`ungroup` and `tail_half` at 10M rows** are 4x to 6x but a few milliseconds on either server;
at that size round-trip noise is a large part of the number.

## Where the comparison is unfair

In Java's favour:

- **Parallelism.** Java uses more than one core for some static operations: a `where` over more
  than 2^16 rows is split into segments filtered in parallel
  (`QueryTable.parallelWhereRowsPerSegment`), and `select`/`update` over more than 2^22 rows
  are split across threads (`QueryTable.minimumParallelSelectRows`). The oracle starts Java with
  `-XX:ActiveProcessorCount=2`, so it has 2 threads for this. Every Oxhaven operation runs on
  one thread. This is part of why `update_*` and `where_range` grow less than linearly from 1M to
  10M rows on Java and linearly on Oxhaven.
- **Tuning.** Java's operators have years of performance work: type-specialised kernels,
  open-addressed hash tables generated per key type, typed sort kernels, flat
  redirections. Oxhaven's are first ports written for correctness and have never been
  profiled.

In Oxhaven's favour:

- **JIT warm-up is excluded.** Two untimed repetitions per case at 1M rows (one at 10M), on a
  JVM that by the second run has executed every case several times, so Java's numbers are its
  steady state. Java's first, cold run of an operation is slower than what the table shows.
- **Memory.** Java keeps strings as objects and pays for garbage collection (5 GB heap here);
  the timing of a collection falls into whichever case triggers it, which is part of Java's
  wider min-max spread. Oxhaven's Arrow arrays are smaller. Memory was not measured.
- **Release build.** Oxhaven was built with `--release`, so `debug_assert!`s are off. The
  self-consistency checker of the engine tests is not in the server either way.

Neutral: both servers run on one machine with the client, over loopback, with the same client
library; the round trip (a few hundred microseconds) is in every number, which matters only for
the cases under about 10 ms (`head_half`, `tail_half`, `ungroup`).

## What was not measured

- Memory: neither the servers' peak nor their resident size per row.
- Ticking throughput under load: many cycles per second, large or wide updates, many
  downstream tables, modifies and shifts (the one ticking case only appends), and Barrage
  subscriptions to ticking tables.
- Tables read from storage (Parquet, CSV) and Kafka ingestion.
- `update_by`, range joins, rollups and trees, partitioned tables, formulas calling Python, and
  the Python console in general.
- Concurrency: several clients or several operations at once.
- Cold JVM start, and sizes above 10M rows.

## Follow-ups

Ordered by value for the effort; estimates are for one agent and include tests against the
oracle corpus and the self-consistency checker, which every change to these paths needs.

1. **Flat storage for dense row keys in `oxhaven-column`** (3 to 5 days). *Half done by task
   9.1: `WritableRowRedirection` has it; `WritableSource`, which the aggregation results
   use, does not.* Give
   `WritableRowRedirection` and `WritableSource` a representation backed by a `Vec` for keys
   `0..n`, with previous values tracked only once the table refreshes, instead of a
   `BTreeMap<RowKey, _>` per cell. Sort results, join results over a flat left table, and
   aggregation and `select_distinct` results all have such keys. This one change is under every
   slow case above except `select_distinct`'s own lookup, and it also speeds up every later read
   through a redirection (snapshots, chained operations), which this benchmark does not time.
   Expected: the redirection part of joins and sorts falls from about a tree insertion to an
   array store per row.
2. **Hash the join keys, and keep per-row join state only when refreshing** (3 to 4 days).
   *Done by task 9.1 for `natural_join` and `exact_join`. The same key table
   (`ops/join/key_table.rs`) is there for `join` (`ops/crossjoin`), `aj` (`ops/asof`),
   `multi_join`, `range_join` and `where_in`, which still read a `Vec<Cell>` per row; `join`
   is 1.5x to 2.9x and `aj` 2x to 3x of Java's earlier times.*
   Encode the keys of a chunk with `RowConverter` into one buffer, as aggregation already does,
   look them up in a hash table with a fixed-seed fast hasher (PORTING.md rule 7 allows fixed
   seeds), and build the left rows per slot only for a refreshing left table. With item 1, the
   static `natural_join` and `exact_join` should come within a small factor of Java; today they
   are the worst cases at 18x to 40x.
3. **Sort without a box per row** (2 days). *Done by task 9.3; see "After task 9.3".* Encode the keys once into Arrow `Rows` (one buffer),
   sort row positions with a stable sort comparing borrowed rows, and add a typed path for one
   primitive column (sort `(value, position)` pairs; stability then comes free). With item 1
   this removes both the per-row allocation and the per-row tree insertion. Expected to bring
   `sort_int` from 9x to 10x down to within 2x to 3x of Java.
4. **Grouping: one hash lookup per row, no ordered trees** (2 to 3 days). `select_distinct` should
   use the aggregation engine's `KeyTable` (or the same encoded-row hash table) instead of a
   `BTreeMap<Vec<u8>, _>` and a touched-group map per row; `KeyTable` itself should hash
   borrowed bytes with a fixed-seed fast hasher instead of SipHash and allocate keys in one
   arena. Covers `select_distinct` (8x to 31x) and the high-cardinality aggregations and
   `last_by` (3x to 10x).

After these, the next step is parallelism: Java's 2-thread `where` and `update` are what make the
10M-row filters and formulas 3x to 8x. Splitting a static `where` or `update` into segments over
a thread pool is a separate task, and needs a decision on how the engine schedules work.

## After task 9.2 (2026-11-17): aggregations and `select_distinct`

Task 9.2 (`tasks/9.2-fast-aggregations.md`) replaced the per-row trees of the aggregation
engine: group keys go into an open-addressed hash table with flat key storage
(`ops/group_keys.rs`), result columns are vectors indexed by group (`ops/agg/slot_source.rs`),
`select_distinct` uses the same key table, `first`/`last` of a static or add-only table keep a
row key per group, and `group_by` builds each group's row set once. Results are unchanged.

**How it was measured.** Oxhaven only: the Java numbers are those of run 1 / run 2 above (Java
was not run again). Oxhaven at `2be3c714` (the code of the task's last change), built and run as
above (`cargo build --release --locked -p oxhaven-server`, default options), the cases below
with `bench/bench.py run --cases 'select_distinct_*,last_by,sum_by_*,avg_by_*,count_by_*,agg_by_multi,group_by'`,
twice, 12:23 to 12:26. The load average was 6.3 to 7.4 on the 4 cores during both runs (other
agents were building), higher than in runs 1 and 2 above. Every case's check (row count, column
sums, position-weighted sum, first and last three rows) equals Java's check recorded in
`bench/results/2026-11-17-run1.json`. Raw results: `bench/results/2026-11-17-task-9.2-run*.json`.

Median milliseconds, run 1 / run 2 of each. "ratio" is Oxhaven after 9.2 over Java, run by run.

### 1,000,000 rows

| case | java | oxhaven before | oxhaven after 9.2 | ratio |
| --- | ---: | ---: | ---: | ---: |
| `select_distinct_one` | 16.6 / 13.8 | 323 / 276 | 7.3 / 6.8 | 0.44 / 0.49 |
| `select_distinct_two` | 30.4 / 29.5 | 285 / 249 | 24.4 / 16.6 | 0.80 / 0.56 |
| `last_by` | 20.8 / 23.0 | 207 / 193 | 111 / 61.0 | 5.3 / 2.7 |
| `sum_by_low` | 20.1 / 18.9 | 38.2 / 37.2 | 22.1 / 23.7 | 1.1 / 1.3 |
| `avg_by_low` | 21.2 / 20.1 | 33.5 / 36.8 | 30.6 / 24.5 | 1.4 / 1.2 |
| `count_by_low` | 13.6 / 17.7 | 27.6 / 38.2 | 20.1 / 20.0 | 1.5 / 1.1 |
| `sum_by_med` | 47.9 / 50.5 | 60.6 / 88.5 | 44.3 / 57.6 | 0.92 / 1.1 |
| `avg_by_med` | 53.6 / 60.7 | 81.2 / 91.5 | 67.6 / 86.4 | 1.3 / 1.4 |
| `count_by_med` | 20.6 / 18.3 | 45.8 / 67.3 | 21.0 / 26.1 | 1.0 / 1.4 |
| `sum_by_high` | 183 / 194 | 1126 / 1080 | 261 / 229 | 1.4 / 1.2 |
| `avg_by_high` | 194 / 199 | 1018 / 1161 | 293 / 202 | 1.5 / 1.0 |
| `count_by_high` | 106 / 133 | 807 / 927 | 167 / 148 | 1.6 / 1.1 |
| `agg_by_multi` | 76.2 / 103 | 124 / 130 | 164 / 118 | 2.2 / 1.1 |
| `group_by` | 109 / 79.6 | 366 / 314 | 133 / 98.5 | 1.2 / 1.2 |

### 10,000,000 rows

| case | java | oxhaven before | oxhaven after 9.2 | ratio |
| --- | ---: | ---: | ---: | ---: |
| `select_distinct_one` | 103 / 100 | 3201 / 2767 | 53.5 / 79.2 | 0.52 / 0.79 |
| `select_distinct_two` | 321 / 249 | 2680 / 2385 | 229 / 247 | 0.71 / 0.99 |
| `last_by` | 258 / 221 | 2120 / 1928 | 523 / 594 | 2.0 / 2.7 |
| `sum_by_low` | 166 / 166 | 332 / 288 | 335 / 364 | 2.0 / 2.2 |
| `avg_by_low` | 205 / 158 | 325 / 332 | 315 / 283 | 1.5 / 1.8 |
| `count_by_low` | 144 / 103 | 329 / 226 | 213 / 196 | 1.5 / 1.9 |
| `sum_by_med` | 589 / 379 | 817 / 550 | 517 / 407 | 0.88 / 1.1 |
| `avg_by_med` | 574 / 480 | 892 / 692 | 469 / 567 | 0.82 / 1.2 |
| `count_by_med` | 265 / 150 | 469 / 351 | 232 / 383 | 0.88 / 2.6 |
| `sum_by_high` | 912 / 871 | 3496 / 2813 | 925 / 1123 | 1.0 / 1.3 |
| `avg_by_high` | 1392 / 1021 | 3360 / 2761 | 988 / 1077 | 0.71 / 1.1 |
| `count_by_high` | 498 / 437 | 2691 / 2448 | 524 / 607 | 1.1 / 1.4 |
| `agg_by_multi` | 882 / 743 | 1353 / 1409 | 974 / 1032 | 1.1 / 1.4 |
| `group_by` | 1209 / 934 | 8797 / 6709 | 1220 / 1235 | 1.0 / 1.3 |

**Reading.** `select_distinct` went from 8x-31x slower than Java to faster than Java at both
sizes; the high-cardinality aggregations from 3x-8x to 1x-1.6x; `group_by` from 3x-7x to
1x-1.3x. The low- and medium-cardinality aggregations were within 2x before and are about
the same now, within the noise of a loaded machine. `last_by` improved 2x-4x but is still 2x-5x
slower than Java; its two runs at 1M rows differ by almost 2x, so the load affected it. With
these loads, ratios under about 2x are noise, as above.

The same cases inside the engine, without the server (`cargo run --release -p oxhaven-engine
--example aggregation-cases`, 1M rows, median of 5), before (main at `03f8dbb8`) and after:
`select_distinct_one` 310 → 4.7 ms, `select_distinct_two` 297 → 19.8 ms, `last_by` 166 → 50 ms,
`sum_by_high` 1271 → 261 ms, `count_by_high` 1014 → 170 ms, `group_by` 344 → 155 ms,
`sum_by_low` 31 → 20 ms.

**What is left.** Measured with temporary phase timers in the engine example (1M rows, after
the change): `last_by` spends about 38 ms of its 47 in the operators, one call through a trait
object per group per chunk for each of its six columns; passing a chunk's runs to an operator
in one call would remove most of that, for every aggregation. The 1M-group aggregations spend
90-190 ms finding slots, nearly all for inserting a million new keys into a table that no
longer fits in cache (Java's whole `count_by_high` takes about the same); and 25-60 ms
writing results. `group_by` still gathers every value into Arrow lists at creation, which
Java does not; that is now about 20 ms of its 155 at 1M rows. Not measured here: ticking
throughput (the engine example's 19 append-only cycles of 50,000 rows into `select_distinct`,
`last_by` and `sum_by` went from 4.6 s to 2.4 s; the two aggregators' own cycles were about half of what remains,
and the rest, the input table's writes and `select_distinct`, was not broken down).

## After task 9.4 (2026-11-17): filters and formulas

Task 9.4 (`tasks/9.4-parallel-filter-update.md`) measured where `where` and `update` spend their
time and then changed the formula evaluator and the match and range filters. Results are
unchanged.

**How it was measured.** Inside the engine only, without a server and without Java:
`cargo run --release -p oxhaven-engine --example filter-update-cases -- <rows> <reps> 1,2,4`,
the benchmark's cases on a table built as the benchmark builds its source, median of 5
repetitions at 1M rows and of 3 at 10M. "Before" is `main` at `eceb7e55`, load average 3.0;
"after" is the branch at `6152b690`, load average 3.4 falling to 2.3 (another agent was
building; the machine has 4 cores). The end-to-end benchmark (`bench/bench.py` against a
release server and Java) was **not** run for this task, so the Java column below repeats the
first measurement's medians and is an indication only: the server adds a round trip to every
case. Raw output: `bench/results/2026-11-17-task-9.4-engine-*.txt`.

Median milliseconds. "1 / 2 / 4" are the thread counts (`OXHAVEN_THREADS`).

| case | rows | before (1 thread) | after, 1 / 2 / 4 threads | before / after at 4 | java, first measurement |
| --- | ---: | ---: | ---: | ---: | ---: |
| `where_range` | 1M | 30.9 | 11.1 / 8.8 / 7.6 | 4.1 | 32.0 / 15.0 |
| `where_string` | 1M | 6.1 | 4.5 / 3.1 / 2.2 | 2.8 | 16.6 / 18.3 |
| `where_conj` | 1M | 17.6 | 8.1 / 8.9 / 9.3 | 1.9 | 17.9 / 14.3 |
| `update_arith` | 1M | 73.8 | 24.6 / 10.1 / 8.9 | 8.3 | 43.7 / 31.8 |
| `update_cond` | 1M | 48.0 | 6.1 / 4.8 / 4.2 | 11 | 17.7 / 26.5 |
| `update_string` | 1M | 201 | 112 / 74.9 / 53.7 | 3.7 | 100 / 52.8 |
| `build` | 1M | 357 | 157 / 117 / 75.2 | 4.8 | 173 / 139 |
| `where_range` | 10M | 420 | 216 / 134 / 114 | 3.7 | 52.1 / 48.5 |
| `where_string` | 10M | 64.4 | 50.3 / 37.5 / 19.1 | 3.4 | 121 / 161 |
| `where_conj` | 10M | 258 | 118 / 95.4 / 94.7 | 2.7 | 92.9 / 80.3 |
| `update_arith` | 10M | 960 | 402 / 254 / 202 | 4.8 | 179 / 355 |
| `update_cond` | 10M | 442 | 189 / 112 / 97.4 | 4.5 | 71.9 / 89.7 |
| `update_string` | 10M | 1987 | 1303 / 742 / 393 | 5.1 | 1094 / 1460 |
| `build` | 10M | 4943 | 2147 / 1492 / 1026 | 4.8 | 1738 / 2577 |

**What was slow.** Not, in the first place, the single thread, as the first reading above
guessed. A pass over the `int` column's memory costs 0.4 to 0.9 ns per row on this machine;
evaluating `A * 2 + 1` cost 13 ns per row at 1M rows and 25 at 10M, `A > 50000 ? B : -B` 18 and
46, and the row set of a filter that keeps 40% of the rows 8 to 11. Every operator built its
result from an iterator of `Option`s, copied operands that already had the right type, and at
10M rows worked on intermediates of 40 to 80 MB; the filters widened each cell to an
`Option<i64>` and added kept rows to the row set one key at a time.

**What changed.** Typed loops in the evaluator; formulas made only of operators run in pieces
of 2^16 rows (the size of a segment of Java's parallel `where`) that stay in cache, on as many
threads as `OXHAVEN_THREADS` says (default: the available cores), and are evaluated again in
one pass if a piece fails, so that the failure is the one reported before; match and range
filters have typed kernels and all single-column filters run in the same pieces. Formulas that
call any function keep the old path. On one thread the evaluation alone went, at 10M rows, from
25.1 to 5.0 ns per row for `A * 2 + 1`, from 42.4 to 14.4 for `B * 1.5 - A`, from 46.1 to 16.5
for the conditional and from 32.1 to 5.1 for the range condition.

**What did not get faster, and the next bottlenecks.** `where_conj` gains nothing from threads:
its second and third filters read their columns at scattered rows (4.7 ns per kept row, one
thread) and every filter's row set is built on one thread. `where_range` at 10M rows is still
about 114 ms on 4 threads against Java's 50: the row set of 4M scattered rows (3M ranges) takes
about 120 ms when built one key at a time, and how much of the 114 ms it still is was not
broken down. Formula results with 8-byte values cost three times as much per row at 10M rows as
at 1M (14 against 5 ns for `B * 1.5 - A` on one thread): the pieces are joined by copying them
into one new array. `update_string` and the string column of `build` format one string per row.
Threads scale less than linearly here because the columns are read, the pieces joined and the
row sets built on one thread, and the machine was shared.

## After task 9.3 (2026-11-17): sort, and reads through a row redirection

Task 9.3 (`tasks/9.3-fast-sort.md`) changed how `sort` orders its rows and how a column is
read through a row redirection. This block measures that change only.

**What was compared.** Two Oxhaven servers on the machine above, run by the same runner with
`--cases 'sort_*,natural_join_small,aj,head_half'`, interleaved case by case: "before" is
`main` at `11c29b75`, "after" is the branch of task 9.3 merged with `main` at the time
(task 9.2 included, which touches none of these cases), both built with
`cargo build --release --locked -p oxhaven-server`. Two complete runs, 14:02 to 14:09, with
another agent's builds on the machine (one-minute load 1.3 to 4.7). Every case gave the same
result on both servers in both runs. Java was not run again: the "java (earlier)" column
repeats the medians of the first measurement, taken under another load, so the last column is
an indication and not a measurement of one run. The raw results and the table with min-max and
load per case are in `bench/results/2026-11-17-task-9.3*`.

Median milliseconds of each run, run 1 / run 2.

| case | rows | before | after | before / after | java (earlier) | after / java (earlier) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `sort_int` | 1M | 478 / 489 | 29.5 / 26.0 | 16 / 19 | 116 / 153 | 0.17 to 0.25 |
| `sort_two` | 1M | 443 / 493 | 139 / 126 | 3.2 / 3.9 | 232 / 251 | 0.50 to 0.60 |
| `sort_string` | 1M | 333 / 361 | 114 / 138 | 2.9 / 2.6 | 280 / 317 | 0.36 to 0.49 |
| `natural_join_small` | 1M | 11.4 / 9.4 | 11.6 / 9.9 | 1.0 / 0.9 | | |
| `aj` | 1M | 470 / 482 | 446 / 425 | 1.1 / 1.1 | | |
| `head_half` | 1M | 0.89 / 3.9 | 1.0 / 0.86 | | | |
| `sort_int` | 10M | 10078 / 9740 | 570 / 540 | 18 / 18 | 1175 / 1255 | 0.43 to 0.49 |
| `sort_two` | 10M | 9256 / 9555 | 1728 / 1672 | 5.4 / 5.7 | 2779 / 2810 | 0.60 to 0.62 |
| `sort_string` | 10M | 6398 / 5782 | 1770 / 1615 | 3.6 / 3.6 | 2546 / 2372 | 0.63 to 0.75 |
| `natural_join_small` | 10M | 144 / 158 | 160 / 148 | 0.9 / 1.1 | | |
| `aj` | 10M | 5511 / 5779 | 5411 / 5233 | 1.0 / 1.1 | | |
| `head_half` | 10M | 1.1 / 1.1 | 0.91 / 1.0 | | | |

The join and `head_half` cases did not change beyond the noise.

**What changed.**

- The sort keys of a set of rows are kept in a few flat buffers, never in an allocation per
  row (`ops/sort/keys.rs`). A sort by one column of a fixed-width type (the integers,
  `char`, `boolean`, the floating point types, and the date and time types) maps each value
  to an unsigned integer whose order is Java's (one NaN, the greatest; `-0.0` equal to
  `0.0`; the absolute value when asked), keeps nulls apart, and sorts (value, position) pairs
  with a radix sort by bytes, which skips the bytes every value shares; a stable radix sort
  keeps ties in the parent's order. A sort by one string column keeps the strings' bytes
  in one buffer, with the two lead bytes of U+E000 to U+FFFF moved above those of the
  characters beyond U+FFFF so that byte order is UTF-16 order. Every other sort keeps Arrow's
  row format in one buffer, as before but without copying each row out. Strings and rows are
  sorted as (first 15 bytes and length, position) values, which order exactly whenever a key
  is at most 15 bytes long, and read the whole key only to break a tie of longer ones.
- The result's redirection is made in one piece (`WritableRowRedirection::from_rows`).
- An incremental sort keeps its keys in the same buffers, appended in sorted order each
  cycle, instead of a box per row.
- `RedirectedSource` reads a row set through a redirection a range at a time: for the flat
  array, a copy of the slice for each range of the row set, under one read lock, and the
  mappings changed in the cycle laid over it for previous values. `FlatSource` gathers by a
  plain vector of positions when no key is null.

**Where the time went, and where it goes now.** Measured with
`cargo run --release -p oxhaven-engine --example sort-phases -- 10000000`, which also runs
the parts of the old method with Arrow directly, one run of each build:

| part, 10M rows, sort by the int column | before | after |
| --- | ---: | ---: |
| the row format of the key column, in one buffer | 38 ms | (no row format) |
| copy each row into a box | 371 ms | none |
| stable comparison sort of the boxes | 6081 ms | none |
| the whole sort | 7921 ms | 524 to 560 ms |
| read a column of every row back through the redirection | 2722 ms | 314 to 324 ms |

So the box per row was 5% of the time and the comparison sort itself, which follows a
pointer per comparison, 77%. Sorting positions with a comparison over the one buffer of the
row format instead of the boxes was slower still (9704 ms), which is why the general path
sorts values that carry the start of their key. One redirection write per row cost 236 ms
against 61 ms in one piece. The other shapes, whole sorts before and after at 10M rows: two
columns 6480 ms and 1582 to 1697 ms, a string 4468 ms and 1419 to 1653 ms. At 1M rows the
int sort went from 424 ms to 49 to 60 ms.

**What did not get faster.** An incremental sort of a refreshing table: ten cycles that each
append 1,000 rows to a table of 10M rows took 23642 ms before and 17514 ms after (2056 ms and
1619 ms at 1M rows). The keys are no longer boxed, but each cycle is still linear in the
size of the table: the result is positional, so an insertion shifts every row after it,
and `remap` writes each shifted position into the redirection, which records each write in
an ordered map of previous mappings (`Mappings::prev` in `oxhaven-column/src/redirect.rs`),
10M insertions per cycle here. Reading a column of that result back takes 9940 ms (10402 ms
before), most of it, by reading the code, the input table's column (`WritableSource`), which
is read a cell at a time; that part was not timed apart.

**The next bottlenecks.** For a static sort by two columns or by a string, the comparison
sort of 24-byte values (1.4 to 1.7 s at 10M rows); a radix sort over the row format's bytes
would remove it. For an incremental sort, the previous mappings of the redirection: a
redirection rewritten in bulk could keep its array as of the start of the cycle once (one
copy) instead of one map entry per write, and the result could leave gaps in its key space
as Java's does, so that an insertion moves few rows; the second changes the row keys and
shifts the sort reports.
