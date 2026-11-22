# Always-on agents on Temporal: a reference implementation

This is a runnable companion to *The durable execution behind always-on agents*.
It follows the article's briefing assistant, which prepares a weekly report on
schedule, delivers it, and remembers the user's feedback for next time. Each
pattern the article describes is implemented as working Temporal code, with
tests and recovery drills you can run on a laptop.

The agent runs on Claude (`claude-opus-5-5`) by default, so you need a Claude
API key or an `ant auth login` sign-in to run it. It can also run on Claude
through Amazon Bedrock, or on OpenAI models (see [Model providers](#model-providers)).

## Article to code

| Article section | Pattern | Code |
|---|---|---|
| Submit work the same way, now or later | A Standalone Activity with a start delay. Now and later share one path, with no timer table or due-work poller. | [`scheduling.py`](src/always_on_agents/scheduling.py) `schedule_briefing`, [`activities/agent.py`](src/always_on_agents/activities/agent.py) `run_briefing_turn` |
| | The agent's timer tool: `set_timer` submits a wake-up job through the same helper path; when it fires, it signals the session, and the agent acts on its note. | `schedule_wakeup`, `AgentActivities.wake_session`, `SessionWorkflow._set_timer` |
| | A recurring Schedule that starts a Workflow per run. Delivery is its own Activity with its own retries. The assistant sets, moves, and cancels it from chat. | `set_weekly_briefing_schedule`, tools `set_weekly_briefing` / `get_weekly_briefing` / `cancel_weekly_briefing`, [`workflows/briefing.py`](src/always_on_agents/workflows/briefing.py) `BriefingTurnWorkflow` |
| Choose how much work a retry will repeat | **Coarse:** one Activity wraps a whole turn, and the agent runtime keeps its own loop. | `run_briefing_turn` + [`agent/runtime.py`](src/always_on_agents/agent/runtime.py) |
| | **Fine-grained:** Workflow code runs the loop, and every model and tool call is an Activity. | [`workflows/agent_loop.py`](src/always_on_agents/workflows/agent_loop.py) `WorkflowAgentLoop` |
| | **A Workflow implementing a tool:** it returns a job id, and the agent collects the result later. | `ResearchWorkflow`, tools `start_research` / `get_research_result` in [`agent/tools.py`](src/always_on_agents/agent/tools.py) |
| | Separate task queues for interactive, scheduled, and background work. | [`config.py`](src/always_on_agents/config.py), [`worker.py`](src/always_on_agents/worker.py) |
| Manage the workspace between turns | A hot-pool controller: ready VMs, claims, health checks, orphan cleanup, rate-limited creation, Continue-As-New. | [`workflows/workspace.py`](src/always_on_agents/workflows/workspace.py) `WorkspacePoolWorkflow` |
| | Save-and-release: wait until idle, save files, record it, then release the VM. Restore when work returns. | `WorkspaceLeaseWorkflow` |
| Update memory after the conversation ends | Batch by count or time. Preparation saves a proposal under a stable batch id; publication commits it with its coverage in one transaction. | [`workflows/memory.py`](src/always_on_agents/workflows/memory.py), [`activities/memory.py`](src/always_on_agents/activities/memory.py), [`store.py`](src/always_on_agents/store.py) `publish_batch` |
| | A fixed Workflow ID per user with the Fail conflict policy, plus a nightly Schedule with Skip overlap. | [`app.py`](src/always_on_agents/app.py) `consolidate_now`, `ensure_nightly_memory_schedule` |
| Define retries and cleanup alongside the work | When retries run out, ask an operator and wait. An approval with a deadline expires and triggers cleanup. | `MemoryConsolidationWorkflow._publish`, `SessionWorkflow._approval_gate` |
| Coordinate sessions and approvals | One Workflow per session. Accepting a message is separate from finishing it; messages start, join, or queue. Approval is tied to exact parameters and rechecked when the action runs. Clients reconnect by event offset. The session's agent can set timers, schedule briefings, run research, and its conversations feed memory. | [`workflows/session.py`](src/always_on_agents/workflows/session.py) |
| Start small and test recovery | Fault injection, an operation log, recovery drills, and tests. | [Recovery drills](#recovery-drills), [`tests/`](tests) |

The session Workflow is a small, self-contained version of the patterns in the
experimental [Temporal Agent Kit](https://github.com/temporal-community/temporal-agent-kit).
It does not depend on the Kit.

## Layout

```
src/always_on_agents/
  scheduling.py        schedule_briefing (Standalone Activity + start delay), Schedules
  app.py               application entry points: record feedback, open workspace, session calls
  worker.py            one worker process for the interactive, scheduled, and background queues
  cli.py               `agent` command for demos and drills
  domain.py            dataclasses passed between clients, Workflows, and Activities
  llm.py               model clients: Claude (Claude API or Bedrock), Temporal-friendly error handling
  llm_openai.py        OpenAI models (Responses API), translated to and from the Claude message format
  store.py             the application's own systems, simulated: SQLite memory store, reports,
                       outbox, fake VM provider and snapshots, operation log, fault injection
  agent/               prompts and tools, the in-process agent loop, message helpers
  activities/          agent, research, memory, workspace Activities, plus background heartbeats
  workflows/           briefing turn and research tool, memory, workspace pool and lease, session
  web/                 FastAPI server and a no-build HTML/JS frontend (`agent-web`)
tests/                 tests against a local dev server
AGENTS.md              rules for coding agents and the commands that check a change (CLAUDE.md imports it)
llms.txt               an index of the patterns, for LLM retrieval
```

## Reading the code

The code carries two sets of comments, written for different readers.

- **For people:** module docstrings teach each pattern: the problem, how
  Temporal solves it, what to notice in the code, and which drill shows it
  happening. Short inline comments explain the reason behind a decision. A
  good reading order is `workflows/agent_loop.py`, `scheduling.py`,
  `workflows/memory.py`, `workflows/session.py`, then `workflows/workspace.py`.
- **For coding agents:** [`AGENTS.md`](AGENTS.md) holds the repo-wide rules
  and the test and lint commands. Inline, a comment starting
  `agent(invariant)`, `agent(trade-off)`, or `agent(constraint)` marks a rule
  that an edit could break without a nearby test failing: an ordering that
  prevents lost work, an ID that makes a retry safe, a string another file
  matches. These notes are deliberately few, and never repeat what the code
  already says. `grep -rn "agent(" src tests` lists them.
  [`llms.txt`](llms.txt) indexes the patterns for retrieval.

## Quickstart

You need Python 3.12+, [uv](https://docs.astral.sh/uv/), and the Temporal CLI
1.7 or later. Its dev server has Standalone Activities enabled.

```bash
temporal server start-dev
```

```bash
uv sync
```

Copy the example settings, then put your Claude API key in `.env` (or skip
the key and run `ant auth login`). To use Bedrock or OpenAI instead, see
[Model providers](#model-providers).

```bash
cp .env.example .env
```

```bash
uv run agent-worker
```

Then, in another terminal:

```bash
uv run agent schedule alice --in 30s
```

```bash
uv run agent job briefing-alice-<due> --wait
```

```bash
uv run agent turn alice
```

```bash
uv run agent feedback alice "Next time, lead with the decisions that need my attention." --batch-size 1
```

```bash
uv run agent turn alice
```

The second turn's report leads with "Decisions that need your attention".
Check the report and the outbox with `uv run agent report --user alice` and
`uv run agent outbox alice`. The Temporal UI at http://localhost:8233 shows
every Workflow, Activity, and Schedule.

### Web frontend

With the dev server and a worker running:

```bash
uv run agent-web
```

Then open http://127.0.0.1:8000. If your Temporal UI isn't at
http://localhost:8233, set `TEMPORAL_UI_URL` so the page's links point to it.
The server is a thin FastAPI layer over the same helpers the CLI uses, and the
page is plain HTML and JavaScript with no build step. It's a local development
tool: it binds to `127.0.0.1` and has no authentication.

- **Assistant:** chat with a session Workflow. Ask it to do something later
  ("send me hello in 30 seconds") and it sets a timer: a Standalone Activity
  with a start delay that wakes the session when it fires. Ask for a briefing
  now or later and it submits one through the same scheduling helper. Ask for
  one every week ("every Monday at 8am") and it sets up a Temporal Schedule,
  which it can also move or cancel. What you tell it feeds memory
  consolidation. Each message shows whether it started a turn, joined the
  running one, or was queued. When the agent wants
  to share a report, an approval card shows the exact parameters, the digest,
  and a countdown, with Approve and Deny buttons. The chat streams over
  Server-Sent Events, and each event's id is the session's event number.
  **Disconnect** and **Reconnect** show the stream picking up from the last
  event you saw, the same way a browser resumes with `Last-Event-ID`.
- **Briefings** (read-only): the briefings the assistant scheduled (each a
  Standalone Activity, with a countdown while it waits), the weekly Schedule,
  recent briefing Workflows, reports, and the outbox. To get a briefing, ask
  the assistant, once or every week.
- **Memory** (read-only): the curated notes, and the consolidation Workflow
  moving through batching, preparing, and publishing. What you tell the
  assistant is its source material.
- **Workspace:** start the VM pool, open a workspace, write files, and watch
  save-and-release, then restore when you open it again.
- **Operations:** inject faults, see operator alerts, and read the operation
  log, with repeated attempts highlighted. When memory publication runs out of
  retries, its alert has a **Retry publication** button that sends
  `retry_publication` to the waiting Workflow.

### Model providers

`AGENT_MODEL_PROVIDER` picks where the model runs. Set it in `.env` and
restart the worker and the web server.

| Provider | Credentials | Model setting (default) |
|---|---|---|
| `anthropic` (default) | `ANTHROPIC_API_KEY` or an `ant auth login` profile | `AGENT_CLAUDE_MODEL` (`claude-opus-5-5`) |
| `bedrock` | `AWS_REGION`, plus `AWS_BEARER_TOKEN_BEDROCK` or standard AWS credentials (environment, profile, SSO). Run `uv sync --extra bedrock` first. | `AGENT_BEDROCK_MODEL` (`anthropic.claude-opus-5-5`) |
| `openai` | `OPENAI_API_KEY` | `AGENT_OPENAI_MODEL` (`gpt-6.2-sol`) |

The worker checks the provider before it starts and exits with a clear
message if something is missing. For the Claude API and OpenAI it does a
free model lookup. Bedrock has no free lookup, so the worker checks the AWS
libraries, the region and the credentials locally. The worker and CLI load a
`.env` file from the current directory (copy [`.env.example`](.env.example)).
Variables already set in your shell win, and blank values are ignored.

**The same everywhere:**

- Every client runs with `max_retries=0`, so Temporal owns retries.
- Errors are classified as retryable (429, 5xx, connection) or not (400,
  401, 403, 404, and OpenAI's `insufficient_quota`). `retry-after` becomes
  the next retry delay.
- Requests set `effort` explicitly, and memory proposals use structured
  output.

**Different per provider:**

- **Claude API:** opts into server-side refusal fallbacks (`fallbacks: "default"`).
- **Bedrock:** uses Anthropic's Bedrock client (`AsyncAnthropicBedrockMantle`).
  Server-side fallbacks aren't offered there, so the SDK's client-side
  fallback middleware retries a refused request on Claude Opus 4.8. The
  Bedrock adapter has no automated tests.
- **OpenAI:** uses the Responses API with `store=False`. The conversation
  stays in the Claude message format in Workflow state, and
  [`llm_openai.py`](src/always_on_agents/llm_openai.py) translates each
  request and reply. Encrypted reasoning items are carried in the
  conversation, so the model keeps its reasoning across tool calls. There is
  no refusal fallback: a refusal fails the turn without a retry, as it does
  on Claude when every fallback declines.

Because the format in Workflow state doesn't change, you can switch provider
with sessions still open. Each adapter skips the blocks only the other
provider can read.

### Temporal Cloud

The worker and CLI connect through Temporal's standard environment config.
Set `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and `TEMPORAL_API_KEY`, or use a
TOML profile. No code changes are needed.

## Recovery drills

The article ends with a checklist: restart the worker before a wake-up,
interrupt a turn after an expensive call, disconnect while approval is
pending, then check what woke, what repeated, and what reached the outside
world. These drills do exactly that. Each one makes real Claude calls, a few
cents' worth.

Two tools make the results visible:

- `uv run agent fault POINT [COUNT] [--mode error|crash]` makes the next COUNT
  calls at a fault point fail (a retryable error) or kill the worker process
  on the spot. The points are `model_call`, `after_model_call`,
  `write_report`, `deliver`, `publish_memory`, `provision_vm`, and
  `snapshot_workspace`.
- `uv run agent ops [--op OP] [--key PREFIX]` is the operation log: every
  model call, tool call, delivery, and provider call, with its attempt number.

### 1. Restart the worker before a scheduled wake-up

```bash
uv run agent schedule alice --in 60s --due monday
```

Stop the worker (Ctrl-C) and wait past the minute. Then check the job:

```bash
uv run agent job briefing-alice-monday
```

It shows `RUNNING (SCHEDULED)`: Temporal kept the job through the wait. Start
the worker again, and the job runs once, at attempt 1. The application kept no
timer table, and nothing had to recover the wake-up.

### 2. Interrupt a turn after an expensive model call

```bash
uv run agent fault after_model_call 1 --mode crash
```

```bash
uv run agent turn bob --id turn-bob-1
```

The worker dies right after the first model call finishes, before Temporal
records the result. Restart the worker. About 30 seconds later, when the
heartbeat timeout notices the dead worker, the turn resumes and finishes. Then
look at the log:

```bash
uv run agent ops --key turn-bob-1
```

Only `turn-bob-1#0` appears twice (attempt 1 and attempt 2), because its
result was never recorded. Every later model call, tool call, and the delivery
ran once.

### 3. Coarse vs. fine-grained: what a retry repeats

```bash
uv run agent fault write_report 1
```

```bash
uv run agent schedule carol --due coarse
```

The coarse Standalone Activity retries the whole turn, 10 seconds later.
`agent ops --key briefing-carol-coarse` shows every model call from the first
attempt again under attempt 2. Now the same failure against the Workflow loop:

```bash
uv run agent fault write_report 1
```

```bash
uv run agent turn carol --id turn-carol-fine
```

`agent ops --key turn-carol-fine` shows each model call once and two
`write_report` attempts. In both cases, `agent outbox carol` shows each
briefing delivered once.

### 4. The memory database is down during publication

```bash
uv run agent fault publish_memory 3
```

```bash
uv run agent feedback dana "Lead with the decisions that need my attention." --batch-size 1
```

Then check the result:

```bash
uv run agent ops --op memory:publish_attempt
```

```bash
uv run agent memory dana
```

You'll see four publication attempts but only one memory `model_call`:
publication retried from the saved proposal. To see the escalation path, set
`agent fault publish_memory 100` instead. After eight attempts (about 90
seconds), `agent alerts` shows an operator alert and `agent memory dana` shows
`waiting_for_operator`. Later sessions keep reading the previous revision.
Clear the fault (`agent fault --clear`) and run
`agent retry-publication dana`; the saved proposal publishes without another
model call.

While one consolidation is running, `agent consolidate dana` is rejected by the
Fail conflict policy.

### 5. Disconnect while approval is pending, then reconnect

A report has to exist first, so run `agent turn erin` once.

```bash
uv run agent session start s1 erin
```

```bash
uv run agent session send s1 "Please share my report with frank@example.com"
```

```bash
uv run agent session follow s1
```

`follow` prints `approval_requested` with the exact parameters and a digest.
Press Ctrl-C to disconnect, and restart the worker too if you like. Then
reconnect from the last event you saw and approve:

```bash
uv run agent session follow s1 --since 4 --once
```

```bash
uv run agent session approve s1
```

The assistant replies that the report was shared, and `agent outbox erin`
shows exactly one `share:` message. Approving again reports no pending
approvals. An approval only ever authorizes the digest that was shown, and
the tool rechecks the digest and the deadline when it runs. With
`--approval-timeout 30s` on `session start`, the approval expires, cleanup
sends a "nothing was sent" notice, and the agent tells the user.

While a turn is running, `session send` reports `joined` (in auto mode) or
`queued` (with `--mode queue`). Accepting a message is separate from finishing
it.

### 6. Workspace pool and save-and-release

```bash
uv run agent pool start --target 2 --tick 5
```

```bash
uv run agent workspace open ws1 --idle 20s
```

```bash
uv run agent workspace write ws1 notes.md "draft: decisions first"
```

After 20 idle seconds, `agent workspace status ws1` shows `released`.
`agent ops --op workspace:snapshot` shows that the save was recorded before the
VM went back to the pool. Open `ws1` again, and `agent ops --op
workspace:restore` shows the files restored from that snapshot.

To exercise cleanup:

- `agent vm leak` creates a VM the pool never recorded. The controller
  destroys it on its next tick.
- `agent vm break <vm_id>` makes a VM fail its next health check; the
  controller replaces it.
- `temporal workflow terminate -w workspace-ws1` leaves a claimed VM without
  an owner; the controller reclaims it.

## Design notes

**What makes each external effect safe to repeat.** Temporal can run an
Activity more than once, so every side effect carries a key:

| Effect | Key |
|---|---|
| Report write | turn id (the Workflow ID or Standalone Activity ID) |
| Briefing delivery | `{turn_id}:delivery` |
| Sharing a report | the approval id: one approval, at most one send |
| Research job | `research-{turn_id}-{tool_use_id}` with `USE_EXISTING` |
| VM creation | a request id generated by the pool; the provider deduplicates on it |
| Workspace save | `{workspace_id}-{run_id}` |
| Memory batch | `{workflow_id}/{run_id}/batch-{n}`: a retry reloads the same snapshot and saved proposal |
| Memory publication | one transaction that also checks the base revision. Publishing twice returns the first result; a stale batch is never published. |

**Heartbeats.** Model calls can take minutes, so their start-to-close timeouts
are long. Activities that wait on slow calls heartbeat every few seconds with a
30-second heartbeat timeout, so a dead worker is noticed quickly (drill 2).

**Determinism.** Workflow code makes no I/O calls and never reads the clock or
random numbers directly. Model output is an Activity result, so replay reuses
the recorded decision. Tests replay the recorded history of each Workflow type
against the current code.

**Payload growth.** The fine-grained loop sends the whole conversation to each
`call_model` Activity, and sessions keep it in Workflow state. That is fine at
this scale. For long sessions, store transcripts outside Temporal and pass
references. Sessions, the pool, and memory Workflows already use
Continue-As-New to keep each run's history bounded.

**Cost.** A scheduled wake-up as a Standalone Activity is one billable Action
on Temporal Cloud, before retries. A Workflow that only starts one Activity is
two.

**What's simulated.** `store.py` stands in for the application's database,
compute provider, and object storage. Temporal coordinates those operations;
the application still makes the provider calls. A waiting Workflow does not
suspend a VM or stop its charges. Pool size, idle timeout, and snapshot policy
are application decisions.

## Tests

```bash
uv run pytest
```

The tests run in about a minute. They use the installed `temporal` CLI as a
local dev server (or download one). So they're free and repeatable, they
substitute a deterministic test double for Claude
([`tests/fake_model.py`](tests/fake_model.py)); the application never uses it.
The Claude and OpenAI clients are tested against a mock HTTP transport,
including a whole agent turn through the OpenAI adapter. The tests
cover:

- scheduling and duplicate rejection, and a delayed job outliving a worker
- coarse vs. fine-grained retry counts, and a worker restart mid-turn
- the workflow-as-tool
- memory batching, publication recovery, operator escalation, and stale batches
- the pool's health checks, orphan cleanup, rate limiting, and Continue-As-New
- save and restore
- approvals (restart, wrong digest, denial, expiry) and join/queue
- the chat's timers (including one that fires after the session ended), scheduled briefings, and memory
- the web API, including the session stream resuming from `Last-Event-ID`
- replay of each Workflow type
