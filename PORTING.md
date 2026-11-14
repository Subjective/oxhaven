# Porting rules

Oxhaven is a Rust implementation of the Deephaven Community Core server. It keeps the gRPC, Arrow
Flight and Barrage protocols compatible and reimplements the engine behind them. These rules apply
to every change, whoever or whatever writes it.

Read this file, `PARITY.md`, the `oxhaven-contracts` crate and your task file in `tasks/` before
writing code. Read nothing else unless your task file points you to it.

## The reference

- The reference is the Java implementation pinned in `oracle/REFERENCE.md`. Behavior questions are
  answered by that source or by running the oracle, never by memory or by what seems reasonable.
- The pin does not move during a run. Re-pinning is its own task.
- Do not copy Java source text into this repository. Cite it.

## What to port and what to redesign

Port with the logic intact:

- Row-set operations, shift handling, and previous-value tracking.
- The incremental algorithms of each operator: what is added, removed, modified and shifted
  downstream for a given upstream update. Follow the Java control flow for these.
- Null, NaN, overflow, and ordering behavior of every operation and expression.

Redesign freely, behind the contracts:

- Memory layout. Columns are Arrow arrays; nulls are validity bitmaps, not sentinel values.
- Per-type code. One generic implementation replaces Java's replicated per-primitive classes.
- Anything that exists for the JVM: chunk pools, reusable contexts, soft and weak references,
  class loading, javac formula compilation, Dagger wiring, the property system.

When the two conflict, observable behavior wins: the result must match under `PARITY.md`.

## Rules for every change

1. **Cite the source.** Each ported function carries a doc comment naming the Java class and
   method it implements, for example `/// Port of io.deephaven.engine.rowset.RowSet#invert`.
   New designs with no Java counterpart say so: `/// No Java counterpart: <why it exists>`.
2. **Never guess.** If behavior you depend on is not ported yet, call
   `unported!("<java.Class#method>")` and list it in your pull request. Do not stub a return
   value, do not approximate, do not leave a silent default.
3. **Never weaken a check.** Do not delete, skip, loosen, or special-case a test, a fuzz seed, a
   tolerance, or a comparison to make something pass. If you believe a check is wrong, stop and
   say so in the pull request; the orchestrator decides.
4. **Stay in your lane.** Change only the crate or module your task owns. `oracle/`, `PARITY.md`
   and `crates/oxhaven-contracts` are protected: CI rejects changes to them without the
   `oracle-change` label, which only the orchestrator applies. If a contract is wrong or
   missing something, open a contract-change pull request that touches nothing else and explain
   the need.
5. **Simple before fast.** Write the plain correct version first. Optimize only in a task that
   names a benchmark.
6. **`unsafe` needs a reason.** Each `unsafe` block has a `// SAFETY:` comment stating the
   invariant that makes it sound. Prefer safe code that is slower.
7. **Determinism.** The same input gives the same output on every run: fixed hash seeds, and
   parallel reductions that combine in a fixed order.
8. **Errors, not panics, for bad input.** Anything a client can send must produce a gRPC status,
   not a crash. Panics are for broken internal invariants and `unported!`.
9. **Tests can fail.** For each new test, confirm it fails when the behavior is broken. A test
   that cannot fail is not evidence.

## Verification every pull request must pass

1. `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo test --workspace`.
2. The differential smoke run in `oracle/` for the operations your change touches.
3. The self-consistency checker, which is on in all engine tests.
4. Blind review by a second agent that sees only the diff, the contracts and this file.

Report what you ran and what you saw. Say which checks you did not run. Never report a pass you
did not observe.

## Git

- One task, one branch, one pull request. Branch from current `main`.
- Never use `git stash`, `git reset --hard`, `git push --force` on a shared branch, or
  `git checkout -- .`. Never touch a branch or worktree you did not create.
- Do not merge your own pull request.
- Two fix rounds per task. If the gate is still red after the second, write down the failing
  seed or test and what you tried, mark the task blocked in its task file, and stop.

## Commit messages

Use scope-prefixed commits, not Conventional Commits:

`<scope>: <imperative description>`

- Lead with the affected crate or area, using the names in this repository: `rowset`, `column`,
  `engine`, `engine/sort`, `expr`, `proto`, `server`, `server/barrage`, `contracts`, `testkit`,
  `oracle`, `ci`, `tasks`, `workspace`.
- Describe the actual change, not its category. Prefer
  `rowset: return empty set when subtracting a superset` to `fix(rowset): fix bug`.
- Short, specific, imperative, no trailing period, about 72 characters or fewer.
- For changes spanning areas, choose the most meaningful common scope. Never `misc` or `chore`.
- Add a body only for why the change was needed, non-obvious tradeoffs, compatibility
  implications, or constraints. The diff already shows how.
- Keep commits logically coherent. No unrelated changes. Do not claim results you did not verify.
- No agent attribution, generated-by notices, or `Co-authored-by` lines.

## Pull requests

A description is a concise handoff written from the final diff, not a log of what you did.

- Title: same scope-first style as a commit subject, describing the outcome.
- Description: the problem or motivation and the resulting behavior. One short paragraph is
  enough for a straightforward change.
- Include implementation detail only for a non-obvious design decision or something that affects
  downstream behavior. For substantial changes, note invariants, compatibility concerns and
  unresolved risks.
- List every `unported!` call you added.
- Report verification only when useful, separating observed passes and failures from checks not
  run.
- No file-by-file summaries, no `Summary` / `Changes` / `Test Plan` boilerplate, no promotional
  language, no speculation presented as fact.
