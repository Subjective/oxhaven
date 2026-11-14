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
