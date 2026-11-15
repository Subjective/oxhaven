# Journal

Times are UTC.

## 2026-11-14

- **05:38** First plan proposed a narrow weekend scope (filter, sort, basic aggregation) at 5 to 8 agents. Owner pushed back that this underestimated agents.
- **07:51** Owner chose full scope with a wide fan-out, no spend cap (subscription). Plan rewritten: 70 tasks, serial M0, then six parallel tracks.
- **07:53** Reference pinned to Deephaven 42.9 (released wheel and image) instead of building the owner's fork at its head. Reason: one `pip install` versus a long Gradle build per environment. Owner told; no objection yet.
- **07:56** Incident: first pin file named the tag `v42.9.0`; the real tag is `v42.9`. Found when the fetch failed. Corrected in the next commit.
- **07:57** Finding: no Docker daemon in the agent environment (client only). Oracle task told to use the pip-embedded Java server.
- **07:57** M0 fan-out: three agents (oracle and harness, parity rules, protocol crate) in separate worktrees; orchestrator wrote the contracts.
- **08:00** Owner asked whether a reviewer role is needed and warned against review churn. Decision: no routine review. Implementers own their task through green CI. A second agent audits only unsafe code, contract changes and oracle changes, must bring a failing test or seed, and gets one round. PORTING.md updated.
- **08:03** Incident: `git add -A` in the main checkout staged the agents' worktree directories (`.claude/worktrees`) as embedded repositories, twice. Removed before merge; `.claude/` is now ignored. Lesson: agents and orchestrator must add paths explicitly.
- **08:06** Owner approved merging on green and freezing contracts and parity rules on the orchestrator's or an architect agent's judgment, for his review afterwards.
- **08:07** Architect agent started on the contracts before they are frozen. The contracts pull request stays unmerged until it reports.
- **08:08** Parity rules delivered. Notable findings that change engine design: Java's null sentinels stay observable (an integer result that lands on the sentinel reads as null); `/` on integers yields a double; three different comparison semantics depending on filter shape; ticking float sums depend on update history; aggregation group slots must be stable.
- **08:09** Owner asked for task sizes to be adjusted as the run shows what agents can handle, leaning larger, and for this run log. Sizing policy from here: start each operator family as one task, record a size verdict per run, merge tasks whose runs come back small and quick, split a task only after a run shows context exhaustion or a parked gate, and send related follow-ups to the same agent while its context is useful.
- **08:10** Parity rules merged (PR 3) and protocol crate delivered (PR 4). Source map and task files merged (PR 2).
- **08:11** Owner asked whether to split across threads. Plan: one orchestrator thread through the first wave of about 10 agents, then one thread per track, each on its own machine, coordinating only through the repo. Reason: agents started from one thread share a 4-core machine, and their reports fill the orchestrator's context.
- **08:12** Owner asked whether the `Port of ...` comments stay. Decision: keep them during the port, because a failing seed must lead straight to the Java method that defines the behavior. After hardening, task 7.10 moves them out of the code into a generated source index and keeps a comment only where the Java behavior is surprising.
- **08:13** Owner said he does not want a strict 1:1 port. PORTING.md now states that Java is the behavior reference and not a structural template: no mirrored hierarchies or per-type copies, idiomatic Rust inside, but the incremental case analysis of each operator is kept.
- **08:14** Architect review of the contracts returned. Applied before the freeze: tables and listeners are dependencies with `satisfied(step)` (a merged listener could not otherwise wait for its parents); column reads and `on_update` return errors; the graph exposes its shared lock so snapshot and subscription are atomic; an input-table write trait; a compiled `Formula` seam so operators do not depend on the expression crate; unordered reads by key for joins; shift data normalizes itself; listener ownership documented (result owns listener, listener owns parents, parents hold listeners weakly) in place of Java's liveness counting.
- **08:24** Contracts merged (PR 1) and frozen. Changes from here go through contract-change pull requests.
- **08:26** Sizing: first wave merges small neighbours into one agent each, following the owner's guidance to lean larger: 1.2 with 1.3 (row sets), 1.5 with 1.6 (update helpers and graph), 1.7 with 0.12 (checker and reference implementations). Six agents, sharing one Cargo target directory because the machine has 4 cores and 23 GB of free disk. The oracle agent is still running; gates that need it are reported as not run until it lands.
- **08:27** Oracle delivered (PR 5): Java server from the released wheel, harness, 37 scenarios with 327 recorded tables. Its command line is `python -m harness diff|record|replay|show` run from `oracle/`, not the `oracle/run.py` the task files assumed; task gates to be aligned. No per-operation filter or fuzzer yet (task 0.8).
- **08:30** First wave started: six agents on 1.2+1.3, 1.4, 1.5+1.6, 1.7+0.12, 2.1, 3.1, each in its own worktree with a shared build directory.
- **08:29** Oracle merged (PR 5) after a `.gitignore` conflict with main. Its tests are not run in CI (they need a JVM); the pass is the implementing agent's observation.
- **08:31** Context reuse: the oracle agent was resumed for 0.8 (fuzzer with shrinking, per-operation filter, error comparison, client smoke scripts, task gate alignment) instead of briefing a new agent.
- **08:41** Column sources merged (PR 6). Row-set work delivered (PR 7).
- **08:47** First contract bug found by ported tests: `sub_set_by_key_range` panicked for negative bounds. Fixed by the orchestrator as a contract change (PR 8). The differential approach working as intended: the Java test, not a reviewer, found it.
- **08:47** Open question recorded: Java keys previous values on the step only, so writes made while idle are treated differently from the contract's "previous equals current while idle". Two agents noticed independently. No observable difference identified yet; revisit when base tables (1.8) land.
- **08:52** Incident: the shared cargo target directory gave one agent wrong test results, because cargo reused a `oxhaven-contracts` artifact built from another worktree where an agent was running mutation checks. Cause: my instruction to share one directory to save disk. Fix: one target directory per agent under `/home/user/targets`, deleted when the branch is pushed; running agents told to rerun their final checks. Passes observed in the shared directory before this are confirmed only by CI.
- **08:53** Row sets (PR 7) and the contract fix (PR 8) merged. Test kit (PR 9) and update graph (PR 10) delivered.
- **08:53** Contract gap found independently by two agents: a listener cannot learn that a delivery to it is queued, so `satisfied` cannot be exact from the contracts alone (Java tracks `lastEnqueuedStep`). The graph task solved it by convention inside `Subscribers` (a table is satisfied once its deliveries have run). Accepted for now instead of a contract change; every table must go through `Subscribers`.
- **08:58** Test kit (PR 9) and update graph (PR 10) merged; main verified locally after both (clippy clean, all workspace tests pass). Foundations M1 complete except base tables (1.8), given to the graph agent because it holds the listener design in context.
- **09:02** Minimal expressions merged (PR 11). The same agent continues with the full formula language (5.2 and 5.3 as one task). Formula corpus (5.1) started in parallel as an independent check on it.
- **09:19** Fuzzer and gate alignment delivered (PR 12). Gap noted: the harness generates only the first 15 operations, so gates for 4.5, 4.7 to 4.10 and parts of others cannot pass until the oracle is extended. That extension becomes its own task ahead of M4. Finding to raise with the owner: Java serves inconsistent contents from a ticking table that failed mid-cycle; PARITY.md is silent on it.
- **09:22** Fuzzer (PR 12) and base tables (PR 13) merged. M1 foundations complete, 89 minutes after implementation started. Context reuse worked: the graph agent built base tables in 24 minutes with fewer tokens than its first task.
- **09:25** First operators (M3) start before the table service exists: engine side only, checked with the consistency checker and against the recorded Java corpus read directly in Rust tests. gRPC handlers and the live oracle gate follow when 2.1 and 2.2 land. Sort is combined with merge and flatten. Operator modules pre-declared on main so branches do not collide in shared files.
- **09:25** Capacity: 14 GB of free disk and 4 cores hold about seven concurrent agents with private build directories. This is the limit for this thread; the M4 fan-out needs per-track threads on their own machines.
- **09:28** Session service delivered (PR 14) after 57 minutes, the longest run and the first judged too large by its agent (I had added the server skeleton and ticket router to the task). It completed without a split, so the lesson is about duration, not failure: service tasks from here are scoped to one service each.
- **09:28** Owner decision pending: `NewSession` is refused because the reference would hand out an unauthenticated superuser session. Not in PARITY.md's allowed differences; flagged for sign-off.
- **09:28** Operator wave started: where; sort with head, tail, slice, merge and flatten; select and update family; basic aggregation.
- **09:31** Session service merged (PR 14). Services split so no task repeats its size: the session agent continues with the table service shell, config constants and the periodic cycle driver; a new agent takes Flight (schema, DoGet, DoPut) with the input-table service. They share one schema module, owned by the table-service task and pushed early; this is the first deliberate cross-agent dependency inside a wave and is a drift risk to watch. Eight agents now running.
- **09:33** Owner set the acceptance criterion: open the web IDE served by Oxhaven, connect, and see a table. Made an explicit end-to-end test (task 6.0): a headless-browser test built and proven against Java first, plus a recorded list of exactly what the IDE calls. The minimal IDE path (static IDE files, gRPC-web, application fields or scope tickets, Barrage viewports, tables defined at startup without a console) is pulled ahead of the Python console work.

### Checkpoint 1, 09:40 (after wave one, 107 minutes in)

Written after each wave from here on: pace, bottlenecks, what changed.

- **Pace.** 14 pull requests merged. 11 agent runs finished: 13 to 57 minutes each, 155k to 356k tokens each, no CI failure on any agent pull request. Median run about 24 minutes.
- **Bottleneck 1, the machine.** 4 cores and about 18 GB of free disk cap this thread near nine agents. Load has stayed under 3, so disk for build directories is the tighter limit.
- **Bottleneck 2, the dependency chain.** Operators cannot run the live oracle gate until the table service and Flight exist. That chain (session, table service, Flight) is the critical path, and the session task was the slowest run.
- **Bottleneck 3, the orchestrator.** Each delivery costs a few minutes of serial work here: open the pull request, wait for CI, merge, log. Agents have not waited on this so far, but it will not scale to 40 agents.
- **Not a bottleneck.** CI takes one to two minutes. Rate limits have not been hit.
- **Changed because of this.** Merged small neighbours into one task five times. Resumed four agents for related follow-ups; base tables cost fewer tokens that way than the agent's first task. Split the service work after the 57-minute session run. Gave each agent a private build directory after the shared one produced wrong results. Pre-declared operator modules so branches do not collide. Started operators before the server exists, checked against the recorded Java corpus. Pulled the IDE path forward.
- **Next.** Move to one thread per track before the M4 fan-out. Run the oracle replay in CI once the server answers it, since today its results are observed by agents only.
- **10:00** Select family (PR 15) and the formula language (PR 16) delivered. Two sizing signals: select reported filling more than one context, and the formula task ran 94 minutes. Both finished and matched Java, so the tasks were at the upper edge, not over it. Rule from here: an operator family with more than about four operations, or with both stored and computed modes, is split in two.
- **10:00** Method finding worth keeping: the formula agent recorded 1,374 cases from a live Java server instead of relying on the source, and reported that the source reading had several behaviors wrong. Future briefs tell agents to record from Java whenever a server is cheap to start.
- **10:06** Formula corpus delivered (PR 17): 6,309 cases recorded from Java. It contradicts PARITY.md in nine places (written from source reading in M0), for example casts to small integer types and range filters with a NaN literal. The where agent was sent the filter findings mid-task; the corpus agent was resumed to correct PARITY.md's statements of fact, with no change to allowed differences. Lesson: rules written from reading Java source are hypotheses until recorded from a running server.
- **10:06** Select family merged (PR 15). The formula language pull request (16) was updated with main before merging, because the select tests pin which formulas the compiler refuses and the new compiler accepts more: two green pull requests can still break each other.
- **10:06** Disk: reclaimed 11 GB of stale build output (old shared directory, default target directory in the main checkout, two finished agents' leftovers).

## 10:15 PR 16 red after select merged, fixed by updating a pin

The select corpus test pins which recorded results it compares and which it does not run. Once the
formula branch was updated with main, the new compiler accepted string concatenation, so
`update_null_propagation.u` ran and matched Java, and the pinned lists were out of date. Moved the
entry from "not run" to "compared" on the formula branch. This was the predicted interaction of two
parallel branches; the pin did its job. Cost: one CI round, about ten minutes.

PR 17 (formula corpus) merged. PR 18 opened for the PARITY.md corrections (facts only).

## 10:12 PRs 16 and 18 merged; expr agent resumed for 5.4

Formula language and the PARITY.md corrections are on main. The expr agent is resumed in its own
context for the tagged corpus test and the function library (5.4), with an explicit exit after the
corpus test if its context is nearly full. Column arrays (5.5) stay separate.

## 10:25 Flight branch waits for the table service; oracle agent resumed

The Flight and input-table branch is pushed but depends on the table service branch for `schema.rs`
and for the config service its input-table gate needs. It is held until 2.2 merges, then takes main
and gets its PR. Lesson: two service tasks sharing one file should have had that file merged first
as its own small PR. Next time a shared file is split out before the fan-out.

The oracle agent is resumed to add recordings for operations the corpus lacks (slice, flatten,
reverse, absolute sort, blink) and harness support for the remaining batch operations, because two
operator agents reported verifying against the Java source alone for those. Agents now get a
private scratch directory after one overwrote another's script in the shared scratchpad.

## 10:22 PR 19 merged; M4 started with two fresh agents

Sort and merge are on main. Two operator slots were free, so M4 starts now rather than waiting for
the M3 gate: natural/exact/left join (4.2), and snapshot plus select_distinct (4.9 and half of 4.8).
Both are fresh agents because the finished operator agents ended near 250k tokens. They may record
their own Java fixtures, since the oracle extension is still in progress. Eight agents are running;
load average is about 7 on 4 cores, so no more are added until one finishes.

## 10:27 Wave 2 landing: where and the table service merged; handlers and replay start

PRs 20 (table service, config, cycle driver) and 21 (where) merged; PR 22 (basic aggregation) is in
CI. All four wave-2 operator and service runs finished within 50 to 60 minutes of each other and
all judged themselves too large or only just right, with 87k to 339k tokens each. None parked.

Next: the table service agent (98k tokens used, knows the registry) is resumed to expose the merged
operators over gRPC and run the first oracle replay against Oxhaven. This is the first check of the
engine through the wire, so mismatches go back to operator owners rather than being patched in
handlers. The Flight agent is resumed to bring its branch onto main and wire CreateInputTable.

Guard added to the handler task: an `unported!` reachable from a client must be rejected with a
status before it reaches the engine (found in the aggregation report).

## 10:35 IDE acceptance test exists; the IDE path is re-planned from its findings

The scout's test passes against Java and its fault matrix replaces my guesses about the IDE path.
Corrections to the plan:

- Task 2.6 assumed gRPC-web. Over plain http the browser uses one multiplexed websocket for every
  call, so the websocket transport is on the critical path and gRPC-web is not.
- Login is GetConfigurationConstants with an authorization header; no Flight handshake.
- ApplyPreviewColumns (4.10) is fatal for every route, not optional.
- No console is needed: GetConsoleTypes may return an empty list.

Three pieces now run in parallel toward the acceptance test: browser transport and static files
(scout resumed, it holds the traces), application fields with startup tables plus the console,
storage and preview calls (fresh agent, task 6.1), and Barrage DoExchange, which starts when the
Flight branch is on main because it reuses the Flight encoder. Static web files are not checked in;
the server takes a web root extracted from the reference wheel (licensing stays an owner question).

Lesson for the retrospective: scouting the acceptance test against the reference first was cheap
(101k tokens) and removed a wrong task (gRPC-web first) before anyone built it.

## 10:40 Main verified after stacked merges

PRs 20, 21 and 22 were each green against an older main and merged within two minutes of each
other. Checked the combined main locally: fmt, clippy and all workspace tests pass. PR 23's CI ran
on the combined tree as well. PR 23 (IDE acceptance test) merged.

## 10:58 First end-to-end replay against Oxhaven

With the handlers (PR 25), the recorded Java corpus replays against a running Oxhaven server for
the first time. Of 26 scenarios that use only exposed operations, 17 are clean. 400 differential
fuzz seeds against live Java found one new difference. Row data matched everywhere; every
difference is an attribute or metadata rule, or a missing function:

1. static where result lacks AddOnly/AppendOnly;
2. input-table column metadata is lost on derived tables;
3. isNaN/isInf missing from the function library;
4. select of a non-flat static table is not marked flat.

What this says about the gates: the engine corpus tests compared rows and the attributes each
agent chose to compare, and three of four causes are attributes that those tests did not cover.
The wire-level replay is the stronger gate and should have existed earlier; from now on an
operator is not done until its handler exists and replay is clean for its scenarios. Putting
replay into CI is the next infrastructure task (needs a Java-free replay of recorded results,
which `harness replay` already is).

Fixes 1, 2 and 4 go to the where agent (87k tokens, lowest context among operator agents), 3 and
row variables to the expr agent already working on the function library. `fuzz` stops at the first
difference, which hides later ones once a known difference exists: the oracle agent should add a
keep-going mode with grouping of known causes (the handler agent wrote a driver for this).

## 11:06 PR 25 merged; replay gate for CI started

Operator handlers are on main. A fresh agent builds the CI gate: start Oxhaven, replay the whole
corpus and the client smokes, compare with a checked-in baseline that ratchets in both directions
(a clean scenario that breaks fails; a differing scenario that becomes clean must be promoted in
the same pull request). Ten agents are running.

## 11:12 Browser transport pushed (PR 26); disk watch

The web IDE now loads from Oxhaven and logs in over the websocket. Remaining for the acceptance
test: application fields and console stubs (6.1, running), ApplyPreviewColumns (6.1), Barrage
(2.4, running).

Disk: 11 GB free with ten agents building (each target directory 1 to 3 GB; each worktree is
0.7 GB since the formula corpus was checked in). Removed four clean worktrees of retired agents.
If free space drops below 5 GB, the next agent to finish is not replaced until space recovers.

## 11:20 Corpus doubled; integration is its own task

The oracle extension is pushed but cannot merge alone: engine corpus tests pin the scenario count
and the lists of compared results, so new scenarios move pins in four operator test suites. A fresh
agent merges main into the oracle branch, classifies every new recorded result per operator
(matches, cannot run, differs) and reports the differences. Differences become a pinned list per
test that fails when it changes in either direction, not a silent skip.

Design note for the retrospective: pinned lists in every operator test make corpus growth cost an
integration task each time. A single shared classification file read by all corpus tests would cut
that; worth doing once the operator set settles.

New Java facts from the recordings that PARITY.md does not state (left_join unimplemented, range
join restrictions, head_by/tail_by refusals, sum of a single -0.0 is 0.0 which bears on A2) are
collected for a second PARITY corrections pass after integration.

## 11:36 Machine is the bottleneck: load 16 on 4 cores, 8.6 GB disk free

Eight agents are building and testing at once. Load average is 16 to 18 on 4 cores and one agent
hit "No space left on device". Agent tokens are not the limit; CPU and disk on this one container
are. No new agent is started until load is under 8 and disk over 10 GB; the next freed slots are
left empty. New agents get CARGO_INCREMENTAL=0. Queue, in order: full aggregations (4.1), cross
join (4.3), as-of join (4.4), ungroup (4.7), shared-ticket resolver, second PARITY pass.

For the retrospective: a second container, or a shared compile cache (sccache) set up in M0,
would have roughly doubled throughput in this phase. Worth raising with Josh.

## 11:38 A downstream operator found a divergence in merged sort

The select_distinct agent recorded Java for select_distinct of a sorted ticking table and found
that ops::sort emits "modified" where Java removes and re-adds a row whose new key sorts before its
old one. Sorted rows are identical, so the sort corpus test (which compares contents) passed; only
an order-sensitive consumer shows it. A sort unit test asserted the non-Java shape.

Decision: the finding agent fixes sort on its branch with a sort-only fixture recorded from Java,
corrects the unit test to the recorded behavior, and removes its ignored test. No ignored tests.

For the retrospective: corpus tests compare table contents per step, not update shapes. Update
shape errors surface only through downstream consumers or Barrage. A recorded update-shape check
(Barrage deltas from Java are exactly that) would close this hole; the Barrage fixtures may give
it for free.

## 11:48 Merge order for the pieces that touch the replay baseline

Four branches interact: the function library (PR 28, changes filter corpus pins), the attribute
fixes (PR 29, changes filter corpus comparisons), the 83-scenario corpus integration, and the CI
gate whose baseline lists every scenario. Order: 28, then 29 updated with main, then the corpus
integration, then the gate with its baseline regenerated on the result. The gate lands last so its
first baseline is the strictest true one; after that every pull request that changes replay
outcomes must edit its baseline lines, and the rules job allows only stricter edits without the
orchestrator's label.

Started full aggregations (4.1) in a freed slot after load fell to 8 and disk recovered to 16 GB
(removed worktrees of six finished agents).

## 18:35 Usage limit stopped the run for about six and a half hours

At about 11:50 the account hit its usage limit on the primary model. Five agents stopped mid-task
(joins, update_by, the sort fix on the snapshot branch, corpus integration, full aggregations) and
the orchestrator did not get another turn until 18:29, after a container restart. The stated reset
is 17 November 08:00 UTC.

State found on return: PR 29 was green and is merged. The Barrage branch is PR 30. Committed work
survived in every worktree, and uncommitted edits survived in four of them. The full-aggregation
agent had committed nothing and its worktree was gone, so that run is lost.

Decision: continue on the next model down for agents, since resuming the stopped agents would send
requests to the limited model. Three replacement agents were started in the existing worktrees
(corpus integration, sort fix, update_by), each briefed from the branch state, not from the old
agent's context. The orchestrator finishes the join branch itself.

For the retrospective: agents should commit work in progress at least every 30 minutes so a stop
costs minutes, not a run; and the limit should have been anticipated when eight to ten agents
were running at once.

## 18:42 Acceptance criterion met on main

Barrage merged as PR 30. On main at f8b121a, `oxhaven-server --demo-tables --web-root <dir>`
with `node oracle/ide/ide-test.js` gave `PASSED: panels, iframe, jsapi`: the IDE loads from
Oxhaven, logs in, opens `ide_static` with matching cells and `ide_ticking` advances. The console
route is not run because there is no Python or Groovy console. Join engine work is PR 31.

## 19:55 State after the restart wave, and the contract decision on list types

Merged since the restart: PRs 29 to 37 and 39 (Barrage, joins with handlers and attribute fix,
snapshot and distinct, the 83-scenario corpus, formula constants, update_by, cross join). Open:
38 (full aggregations, rerunning checks on current main), 40 (ungroup). The replay gate branch
is ready with a baseline at 47 clean of 83; it lands after PR 38 because that changes replay
outcomes, and will need one more regeneration.

Josh reset the usage limit at 19:05 and asked for agents on the primary model again; agents
started after that use it. Replacement agents briefed from branch state finished interrupted
work in 9 to 60 minutes each, which suggests a fresh agent with a precise brief and a branch is
a cheap way to recover; the cost was the six and a half hours nobody was awake to restart.

Two agents independently reported the same blocker: no array-valued column type in the
contracts. Using the standing authority to change contracts, an architect agent is adding vector
and array types across the workspace as one change (contracts plus what must change to compile),
for Josh's review afterwards. Operators stay out of that change.

Sizing so far: operator tasks that bundle the engine operator, its handler and a contract change
are consistently too large. Engine operator alone, handler alone and contract change alone each
fit one context. Applied from here on.

## 20:41 Replay gate is on main

PR 41 merged after its first Actions run passed (oracle-replay job about 4.5 minutes including
the build; rust job about 8 minutes now that the engine suite is large). Baseline: 48 of 83
scenarios clean, 26 of 27 smoke checks. From here every pull request that changes what the
server answers must move its baseline lines, so the count of clean scenarios is the run's
progress measure and cannot silently go down.

Watch: the engine test suite took 365 s locally under load. If it keeps growing, the random
tests should take their seed counts from an environment variable with a smaller default in CI.

## 20:58 List types and range join in review

PR 43 adds vector and array column types to the contracts (my decision under the standing
approval; for Josh's review). PR 44 adds range join behind a seam that PR 43 removes the need
for. PR 42 (as-of join) is waiting on its rust job.

Proposed PARITY.md text for list columns, from the architect agent. Not applied: it needs
Josh's sign-off, like every change to what counts as equal.

> Array and vector columns: a null cell and an empty cell are different values. Elements
> compare in order under the rules of the element type (null equals only null; any NaN equals
> any NaN). Whether a column is a vector or an array is part of its type and must match
> `deephaven:type`. On the wire the offsets start at 0 and a null cell contributes no elements.
> `deephaven:isSortable` is absent for an array whose element type has no registered comparator.

Range join finding: Java's results for `->` depend on left table size and order (the 2048-row
read and the equality shortcut), and the shortcut treats -0.0 and 0.0 as equal while the binary
search does not. Reproduced, not corrected.

## 21:25 List types on main

PR 43 merged after a second Actions run on top of as-of join and range join (both match on
column types, so the first green run did not cover the combination). PRs 42, 44 and 45 merged.
Baseline is 51 of 83 clean. Running: cross join handler, as-of join handler, shared tickets,
head_by and tail_by. Next, now unblocked: group_by with the group aggregation, ungroup reading
real array columns, range join result tables.

## 22:28 As-of join: Java's ticking result differs from its own static result

The as-of handler made `harness fuzz --ops aj` possible. 3000 seeds against live Java 42.9:
2985 clean, 15 differ, all after a ticking step. In every case Oxhaven equals a static
recompute of the same tables and Java's incremental result does not.

- NaN stamps, both sides the same ticking table, rule `>`: seeds 39, 257, 1319, 2619 (1774
  probably). Java joins a NaN left row to a newly added NaN right row; statically it does not.
- Chains of 3 to 4 `aj` whose last left table depends on its right table: seeds 2, 465, 540,
  675, 1371, 1427, 1735, 2110, 2669, 2873. After the right table changes Java keeps the match
  it had (no row after an add, a deleted row's values after a delete).

Reproduce with `oracle/bin/harness fuzz --a java --b oxhaven --ops aj --seeds 1 --seed <n>`.

Owner question for Josh: reproduce Java's incremental answers, or keep the answer that agrees
with Java's static path and record it in PARITY.md as an allowed difference. Nothing is changed
until he decides; the engine keeps the static-consistent answer, and the fuzz for `aj` is not
part of the CI gate. The second family may also be an artifact of how the fuzzer builds a left
table from the right one within one cycle; worth one look before deciding.

Also seen: a `Batch` operation with an unset table reference ends Java's stream with CANCELLED
where Oxhaven answers INVALID_ARGUMENT (generic batch path; follow-up).

## 22:40 Follow-ups found by the handler work

- Engine: `CrossJoinHelper#zeroKeyColumnsJoin` returns a static table when one side is static
  and empty; ours is refreshing and never changes. Small fix in `ops/crossjoin`.
- Joins: a match or addition that is empty or ends in a space reaches a Java client as
  CANCELLED (the message ends in a space and the transport rejects the header); we send
  INVALID_ARGUMENT with the message. Candidate for PARITY.md, owner decision.
- Session: an export id defined twice has Java's "Details Logged w/ID" text and our own text.
- Watch: one of ten local gate runs hung 900 s in the client smoke "input table from an
  initial table" with an idle server. Not reproduced. If CI shows it, it is a real hang.

## 23:10 Input-table smoke stall seen by two agents

The cross join agent saw one gate run of ten hang in "inputtable: input table from an initial
table"; the group_by agent saw three hangs in "inputtable: rejected changes" on main at
5e50fcb, also with a server built from that main alone. Client waits for response headers of
`input_table_service.add`, server idle. Not seen in Actions so far. Two independent sightings
make this a real intermittent hang in the server, not a flake. An agent is assigned to find it.

## 23:40 The stall reached Actions

PR 51's oracle-replay job hung in the client smokes after the tables smoke and hit the 300 s
timeout (run 43651067945). No baseline line was involved; the next head passed with no code
change. Third independent sighting, first in CI. The debugging agent has the CI server log.
Until it is found, a red oracle-replay job with "did not finish within 300 seconds" and no
PROMOTE/TIGHTEN/REGRESSED line is this stall; it gets one comment on the pull request and the
merge waits for a green run on the head, never a merge over red.

## 00:20 State

Merged through PR 54. Baseline: 65 of 83 scenarios clean, 27 of 27 smokes. The rust job in
Actions now takes 16 minutes (8 three hours ago); the engine suite is the cost. Follow-up
queued: take random-test seed counts from an environment variable with a smaller default in CI
and the full count in a nightly job, without dropping any test.

Running: stall investigation, head_by handler with the cross join static fix, list columns on
the wire, remaining aggregations, remaining update_by operators.

## 2026-11-15 00:35 UTC

PR 55 opened (HeadBy and TailBy handlers, cross join zero-key fix). Its agent hit the
gate hang once and captured a stack: the hang was in the harness client, with
pydeephaven's `Table.__del__` making an RPC from the gRPC channel thread. Passed to the
stall investigation as a lead to verify; if it holds, the fix belongs in the harness.

## 2026-11-15 00:50 UTC

Stall explained (PR 56). It is a deadlock in the Python client, not the server:
pydeephaven's `Table.__del__` makes a blocking RPC, failed calls leave tables in
reference cycles, and the collector can run that finalizer inside gRPC's channel code.
It reproduces against Java too. The harness now disables automatic collection and
collects on its own thread between checks; the gate also dumps all thread stacks before
its time limit. Before: 21 of 21 loop processes hung. After: 0 hangs in 490 runs.
The defect is upstream in pydeephaven 42.9; worth reporting to Deephaven (owner's call).

## 2026-11-15 01:15 UTC

PRs 55 (HeadBy/TailBy handlers) and 56 (harness stall fix) merged; main is at 70 of 83.
PR 57 opened with three of the seven remaining update_by operators; the agent ran out of
context, so the rest (rolling group, count-where, rolling formula, validateFormulas) is a
second task. Sizing note: numeric operators with recorded fixtures cost about 100k tokens
each. Owner question added: EmStd after an update is seeded with the true variance here,
Java uses the square of its previous result; results differ in the last bits and are
compared under PARITY A1. Launched: multi_join (4.12) and three small parity fixes (4.13).

## 2026-11-15 01:45 UTC

PR 57 merged. PRs 58 (multi_join, 72/83 on its head) and 59 (aggregations, 71/83 on its
head) open. Two scenarios (`agg_by_sets`, `ticking_agg_by_full`) now wait only on formula
aggregation, which needs vector values in oxhaven-expr; that and update_by's rolling
group/formula/count-where make "vectors in expressions" the next enabling task.
Follow-ups: `min` keeps 0.0 where Java keeps -0.0 (agg fuzz seeds 337, 351, 785); stale
`why` texts in the baseline for agg_by_sets, update_by_time_and_ema, update_by_rolling_tick.
Owner review items added by PR 59: distinct null-to-[] reported modified; first/last
modified decided at cycle end rather than per chunk.

## 2026-11-15 03:50 UTC

Second usage-limit stop. All five running agents (list wire, parity fixes, CI test time,
expression vectors, update_by group/count-where) failed on the Fable limit at about
02:05 UTC and the machine restarted; nothing ran for about 1h45m. The owner reset the
limit at 03:46 and all five were resumed in their existing contexts. Uncommitted work in
the worktrees survived; servers did not. PRs 58 (multi_join) and 59 (aggregations) had
merged before the stop: main is at 73 of 83 scenarios. Disk is at 8.5 GB free.
Token use so far, summed from tasks.csv: about 14.2M subagent tokens over 68 runs.

## 2026-11-15 04:17 UTC

PR 60 (list columns on the wire, ApplyPreviewColumns) merged. PR 61 opened (parity fixes).
Finding that closes an earlier owner question: Java's CANCELLED for a join match ending in
a space, and for a batch with an unset table reference, is one mechanism. A status whose
message ends in 0x20 is an illegal HTTP/2 field value, so Java resets the stream. We now
do the same in the transport, so no allowed difference is needed.
Contract gap for owner review (from PR 60): no column type for preview columns and no
table attributes for column descriptions or the uploaded schema; the server keeps them in
side registries that table operations do not carry.
Possible bug to check: head_by over a table whose only column is the key fails internally.

## 2026-11-15 04:25 UTC

Owner asked why main shows failed runs. Cause: `concurrency.cancel-in-progress` on the
main ref; each run-log push cancels the run of the merge commit before it. No PR was
merged on a red head. One real failure on main: run 235 (23:39 UTC, the harness hang,
fixed by PR 56). Fix assigned to the CI test-time task: cancel only pull request runs and
skip push runs for runlog-only commits. Until then run-log commits are batched.
Also found: squash merges carry a `Co-authored-by: Claude` trailer (GitHub adds it from
the branch commits' author) and commits use the environment's "Claude" git identity,
against the owner's no-attribution rule. From PR 61 on the squash message is written
explicitly. Author identity is with the owner to decide.

## 2026-11-15 04:33 UTC

Owner's decision on commit identity: commits are authored by him (Josh, GitHub noreply
address) and carry a `Co-authored-by: Claude <noreply@anthropic.com>` trailer. The repo's
local git identity is set accordingly, which worktrees inherit; squash merges get the
trailer in an explicit message. He asked whether existing history can be rewritten; no
rewrite or force push is to happen without his explicit go-ahead.
PRs 62 (rolling group) and 63 (CI seed budget, main runs no longer cancelled) opened.

## 2026-11-15 04:36 UTC

Owner asked whether main's history can be rewritten after all implementation is done,
keeping order. Yes: recorded as a final task. Scope: change only the author of the older
commits to the owner and add the Claude co-author trailer where missing; keep commit
order, dates, messages and content. It needs a force push of main, so it waits for the
owner's explicit confirmation at that time and for no branches to be open.
Launched: console and Python session design spike (6.1), second parity fixes (4.15).
History rewrite task, added steps: push a backup branch of the old main first; after the
rewrite verify that the final tree hash, and the tree of every commit in order, equal the
originals before force pushing; keep the backup branch until the owner says to drop it.
Merged pull requests cannot be changed by this: their pages keep the original commits.

## 2026-11-15 04:36 UTC (later)

Owner decided to leave existing history as it is. The history rewrite task is dropped.
Going forward stays: owner as author, Claude co-author trailer.

## 2026-11-15 04:38 UTC

Owner settled the identity question: keep things as they were. Branch and run-log commits
are made under the Claude identity, and squash merges use GitHub's default message, which
shows the owner and Claude as co-authors. The repo-local identity override is removed.
Commits between 04:33 and 04:37 (run-log entries and the squash of PR 61) were made under
the owner's identity and stay as they are. No history rewrite.

## 2026-11-15 06:05 UTC

PRs 61 to 64 merged; main is at 74 of 83 scenarios. The `rust` job now takes about 6.5
minutes (was 13 to 16).
Architecture decision for the console (PR 65, for owner review): CPython embedded in the
server through PyO3 behind cargo feature `python`; Oxhaven's own `deephaven` package with
the reference's names and signatures (nothing copied; the reference package is under the
Deephaven Community License); table methods go through the existing table-service
handlers; scope is the interpreter's globals. A slice works end to end in the IDE and
with pydeephaven `run_script`. Task files 6.2 to 6.18 and 7.1 to 7.11 written. Largest
risk: Python functions in query strings (GIL versus update cycles).

## 2026-11-15 06:11 UTC

PR 65 (console design, embedded Python behind the `python` feature, M6 and M7 task files) merged on green, including the first Actions run of the `python-console` job. Machine at load 24 with 6.9 GB disk free: the fuzz-coverage agent was told to run one family at a time, and no new agents start until load and disk recover.

## 2026-11-15 06:25 UTC

PR 66 opened (column statistics, quick filters, blink input tables). Gate reaches 78 of 83 on that branch. It also corrects a rule merged in PR 61: Java memoizes a view on a copy only for bare column names, not `view(X = K)`. For owner review: three quick-filter sub-cases and vector column statistics are refused as not ported; blink Barrage subscriptions still refused.

## 2026-11-15 06:35 UTC

PR 66 merged on green. Main is at 78 of 83 replay scenarios clean. Remaining: formula aggregation (2) and update_by count-where and rolling formula (3), both in flight.

## 2026-11-15 07:18 UTC

PR 67 (formula aggregation) opened; after updating it with main it failed to compile: PR 66 and it both changed `ops/filter/filters.rs` and the textual merge was clean but a match lost an arm. Author agent resumed to reconcile and rerun the gate. Lesson: two tasks edited the same file in parallel; a branch update is not proof of a compatible merge, CI on the merged head is.

PR 68 (Python script oracle, task 6.3) opened. The agent tried to make `check-baseline-change.sh` ignore sections the base lacks and the permission system refused the edit; it left the script alone and reported it. The script exits 1 on added entries, so the outcome on CI decides whether that needs an owner decision.

## 2026-11-15 07:21 UTC

PR 68 merged on green. The replay gate now builds with the Python console and carries two new sections: console smokes (4 clean) and recorded scripts (2 of 21 clean, 17 missing API names, 2 differing). Branches in flight must merge main before their final gate run.

## 2026-11-15 07:40 UTC

PR 69 opened (second parity fixes). Fuzz against Java is now clean in every family except two known classes, both owner questions: tables that fail mid-cycle (Java keeps serving the last snapshot; 65 of 3000 default seeds, 463 of 3000 join seeds) and one as-of join incremental difference (seed 1297). Java surprise recorded: seed 1526, Java throws ArrayIndexOutOfBounds in UnionRedirection while building the duplicate-right-key message.

## 2026-11-15 07:32 UTC

PR 67 merged on green after the author fixed the merge break (one match arm). Main is at 80 of 83 replay scenarios clean; the remaining three are the update_by count-where and rolling formula scenarios, in flight.

## 2026-11-15 07:38 UTC

PR 69 (second parity fixes) merged on green, on a head that included PR 67.

## 2026-11-15 08:20 UTC

Task 6.4 (Python table core) came back with the gate red by design: about a quarter of the listed Python methods need engine operators no task owned (head_pct family, rename/move columns, restrict_sort_to, where of an OR, merge_sorted of static tables, attributes). The agent refused to change protected baseline lines on its own and set the task to blocked, which is the right behaviour. Decision (mine, for owner review): record the four `table_*` lines as `differs` with the real failure kinds under the `oracle-change` label, and open task 4.16 for the operators, whose gate is promoting those lines to clean. Reason: the lines said `missing` because the Python names did not exist; they now exist and refuse honestly, and the baseline should say what is true. Size verdict for 6.4: the Python surface fit, the planning missed engine dependencies.

## 2026-11-15 08:24 UTC

PRs 70 (Python environment) and 72 (query scope) merged on green. PR 71 (Python table core) conflicts with both in `console/python/mod.rs`; its author is merging main and rerunning the gate. Three console tasks ran in parallel over the same module file: acceptable cost (one conflict round), but the next Python API tasks get distinct files per task.

## 2026-11-15 08:45 UTC

PR 73 opened (block row set). Measured: the contract RowSet costs O(ranges) for size, get, find and single-key adds (adding one key to a 2M-range set takes 64 ms); the block row set is 100 to 30000 times faster on those and uses 1 MB instead of 32 MB, and is slower on range walks and unaligned shifts. It is not the engine row set yet: swapping it changes `RowSet::ranges` in the contracts (about 35 callers). Decision: do the swap as its own contract-change task (7.7b) once the engine branches in flight (update_by count-where, table shape operators) have merged, to avoid a workspace-wide conflict. Owner review item: the contract change and the performance trade-offs in docs/ROWSET.md.

## 2026-11-15 08:54 UTC

PR 74 merged on green: every replay scenario is clean (83 of 83), every client smoke clean (31 of 31), and every `UpdateBySpec` operator is ported. What the gate does not show: the `unported!` refusals listed in the pull requests, the Python script scenarios (3 of 21 clean, in progress) and the two open owner questions from the fuzzer (failed tables keep serving in Java; one as-of join incremental difference).

## 2026-11-15 08:58 UTC

PR 73 (block row set) merged on green. Its checked head had PR 71 but not PR 74; merged anyway because the change is a self-contained module nothing uses and PR 74 touches no row set or contract code. The main push run is the check on the combined tree.

## 2026-11-15 09:40 UTC

PR 75 (Python aggregations, joins, update_by) and PR 76 (Parquet read, first slice) opened. Owner decision recorded for the summary: Snappy, Zstandard and Brotli decoding need BSD-3-Clause crates (`snap`, `zstd`, `brotli`), outside the Apache-2.0/MIT rule; Snappy is the default codec Java writes, so Deephaven-written Parquet files are refused until Josh decides. Missing engine pieces found by the Python tasks, for a follow-up task: partition aggregation, aggBy initial groups, count_where with no filter, keyed transpose, range_join with no aggregation, multi_join with no key.

## 2026-11-15 09:42 UTC

PR 75 merged. Its head showed one red check, `python-console`, from the run that the label event cancelled seconds after the pull request opened; the full run that replaced it on the same commit passed every job including `python-console`. The aggregate job added in PR 70 reports failure when its matrix jobs are cancelled, so every labelled pull request shows a stale red mark. To fix in the next CI change: the aggregate job should not run (or should report cancelled) when the run is cancelled.

## 2026-11-15 09:44 UTC: Parquet read merged (PR 76); CI fix for the stale check open (PR 77)

PR 76 merged on a green run whose head contained PR 75. Python scripts are now 16 of 34 clean
(nine `parquet_read_*` scenarios added, five clean). Still for owner review: `docs/STORAGE.md`,
and whether Snappy, Zstandard and Brotli may be enabled (their crates are BSD-3-Clause, and
Snappy is what Java writes by default).

PR 77 changes the `python-console` aggregate job from `always()` to `!cancelled()`, so the run
that a label event cancels no longer leaves a failed check on the commit.

## 2026-11-15 18:52 UTC: Usage limit stop, 09:46 to 18:52 UTC

All four running agents (0.15 fuzz coverage, 6.14 console logs and cancel, 4.16 table shape
operators, 6.8 Python dtypes) stopped at about 09:46 UTC on the model usage limit, and the run
was idle for nine hours until the owner reset it. The machine restarted in between, so servers
and builds were lost; the worktrees and pushed branches were intact (one uncommitted file in
4.16, one in 6.14, six unpushed commits in 6.14). All four were resumed in their existing
context rather than restarted. Cost of the stop: nine hours of wall time, no lost work found.

## 2026-11-15 19:02 UTC: Owner approved BSD-3-Clause for the Parquet codecs

The owner answered the open licence question: the Snappy, Zstandard and Brotli codec crates
(BSD-3-Clause) may be used. The exception is for these Parquet codecs only; the Apache-2.0/MIT
rule otherwise stands. The author of the Parquet read task was resumed with the follow-up
(task 7.1b, branch task/7.1b-parquet-codecs).

Correction, 19:05 UTC: the 7.1 author could not be resumed, because an agent whose worktree
has been removed cannot be resumed. Task 7.1b went to a fresh agent. Lesson for the run: keep
an author's worktree until its likely follow-ups are done, disk allowing.

## 2026-11-15 19:20 UTC: Console logs and cancel (PR 79) held for an owner decision

Task 6.15 assumed Java interrupts a command whose call is cancelled. The agent probed Java 42.9
and found it never does: the command runs to its end after cancel and after session close.
The task's gate still demanded a stop, and the agent followed the gate, so PR 79 stops such a
command (SIGUSR2 to the interpreter thread, KeyboardInterrupt). That is a difference from Java
not listed in PARITY.md, and allowed differences need the owner's sign-off, so the pull
request is open but not to be merged until he chooses: keep the stop and record it, put it
behind a flag that defaults to Java's behaviour, or remove it. Orchestrator error: the task
file stated Java's behaviour without checking it. Also noted: the console transcript carries
traceback line numbers of table.py, which go stale with every Python API change; the probe
should scrub them (follow-up).

## 2026-11-15 19:50 UTC: Table shape operators up as PR 80; Python dtypes merged (PR 78)

PR 78 merged green; its cancelled first run left no failed check, so the PR 77 fix works.
PR 80 (task 4.16) promotes the four script lines that task 6.4 had recorded as differing.
For owner review, differences it reports in failure paths: NullPointerException text in a
sorted merge, a schema that Java fails to build for a restriction naming a non-column, the
text for a non-int index in move_columns, and which column SortRowRedirection names.
Tasks 6.9 and 6.10 (conversion, input tables) started as one run.

## 2026-11-15 19:40 UTC: Widened fuzzer up as PR 81; findings become task 4.17

The fuzzer now draws every ported operation. With its restrictions in place, 12 of 13
families are clean over 1000 seeds and one seed differs (aj losing a left row). Before the
restrictions the first wide generator had 69, 103 and 12 differing seeds in aggregations,
groups and update_by, so the earlier "83 of 83 scenarios clean" overstated parity: the
recorded scenarios did not reach array-column aggregations, attribute copies or in-cycle
failures. Seven open engine defects are written up as tasks/4.17-fuzz-findings.md with the
minimal scenarios beside it. For owner review: A5 (Java's head_by cast changes a column's
type; reproduce or not), the serving of tables that failed in a cycle (A7, open since
before), and five Java defects (J1 to J5) that Oxhaven does not reproduce.
