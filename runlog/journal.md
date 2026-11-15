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
