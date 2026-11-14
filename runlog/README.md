# Run log

A record of the port for later analysis: what each task cost, how it ended, and what went wrong.
The orchestrator appends to these files as the run proceeds.

- `tasks.csv`: one row per agent run. A task that is retried or split has several rows.
- `journal.md`: decisions, incidents, drift, re-splits and task-sizing adjustments, in time order.

Columns of `tasks.csv`:

| Column | Meaning |
| --- | --- |
| `task` | Task id from `tasks/` |
| `run` | 1 for the first attempt, 2 and up for fix rounds or re-runs |
| `who` | `agent` or `orchestrator` |
| `started_utc` | When the run started |
| `minutes` | Wall-clock duration |
| `tokens` | Tokens the agent used, as reported by the harness; blank for orchestrator work |
| `tool_uses` | Tool calls the agent made |
| `outcome` | `merged`, `pr-open`, `blocked`, `superseded`, `review-only` |
| `gate_failures` | Number of times its gate or CI failed before the outcome |
| `pr` | Pull request number |
| `size_verdict` | `too-small`, `right`, `too-large`, judged after the fact |
| `notes` | Cause of failures, findings, anything unexpected |
