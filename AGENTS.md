# AGENTS.md

A Temporal reference implementation (Python 3.12, `temporalio` 1.36.1) of an
always-on briefing agent that runs on Claude by default, or on Claude via
Amazon Bedrock or OpenAI models (`AGENT_MODEL_PROVIDER`). `README.md` maps each article
section to the code. This file lists the rules an edit must keep.

## Check every change

```bash
uv run pytest -q
uvx ruff@0.14.5 format src tests && uvx ruff@0.14.5 check src tests
node --check src/always_on_agents/web/static/app.js
```

- **pytest** starts a local Temporal dev server and takes about a minute.
- **Tests never call a model:** they inject `tests/fake_model.py`, and the
  client tests use mock HTTP transports, so no API key is needed.
- **Replay check:** a test that ends with `assert_replays(handle)` replays the
  history it just produced against the current code.
  - It catches non-determinism.
  - It does not prove that Workflows already running on older code can replay.
    For that, use `workflow.patched()`.

## Rules

- **The app always runs on real models**, from the configured provider: `anthropic`, `bedrock` or `openai`.
  - Never add a scripted, offline or mock model path under `src/`. The owner removed one on purpose.
  - The worker refuses to start without access to the configured provider, by design.
  - The only test double is `tests/fake_model.py`, passed in through `build_workers(model=...)`.
- **The conversation format is Claude Messages everywhere**: Workflow state, `ModelRequest`, `ModelResponse`.
  - Provider differences stay inside the model clients (`llm.py`, `llm_openai.py`).
  - A new provider is a new client with `complete(ModelRequest) -> ModelResponse` that translates at the edge.
  - Never put provider-specific branches in Workflows or the agent loop.
- **Never read, print or commit `.env`.** It holds the API key.
- **Workflow code (`workflows/`) is deterministic.**
  - No I/O.
  - No `datetime.now()`, `random` or `uuid`. Use `workflow.now()`, and build IDs from `workflow.info()`, `tool_use_id` or counters kept in Workflow state.
  - Put side effects in Activities.
  - Update validators only read state; they never change it.
  - Import application modules inside `workflow.unsafe.imports_passed_through()`.
- **Every external effect is keyed by an ID derived from durable data** (turn, tool_use, approval, batch or request ID), and `store.py` deduplicates on that key.
  - A random or time-based key makes a retry repeat the effect.
  - The keys are listed in `README.md` under "Design notes".
- **`domain.py` dataclasses are Temporal payloads, serialized to JSON by field name.**
  - Add new fields with defaults.
  - Don't rename or remove fields that running Workflows or Schedules carry.
- **Continue-As-New carries Workflow state in the input:** `SessionState`, `PoolState`, `ConsolidationInput`.
  - New state that isn't added there is lost at the next Continue-As-New.
  - Call `continue_as_new` only after `workflow.all_handlers_finished`.
- **`AppStore` is blocking SQLite.** Async Activities call it through `asyncio.to_thread`. A direct call blocks the event loop and stalls heartbeats.
- **Retries belong to Temporal.**
  - Every SDK client uses `max_retries=0`.
  - Classify errors in `llm.py` as retryable or non-retryable `ApplicationError` instead of adding retry loops.
- **Register new Activities and Workflows** in `worker.build_workers` / `WORKFLOWS`.
- **Keep the structure flat.** This is a reference implementation meant to be read top to bottom:
  - Extend the existing modules rather than adding layers, base classes or registries.
  - The UI has no build step and no framework.
  - The Briefings and Memory views stay read-only: the user acts through the assistant.

## Comment conventions

- **Docstrings and plain comments are for human readers.** They explain the
  pattern and why it exists. Don't rewrite them as rules.
- **`# agent(<kind>): ...` (`// agent(...)` in JS) marks a rule that an edit
  could break without any test failing nearby:**
  - `invariant`: must stay true, or retries duplicate effects, work is lost, or a Workflow deadlocks.
  - `trade-off`: a deliberate choice. Don't "fix" it unless the reason no longer holds.
  - `constraint`: imposed by Temporal, the Claude API or another file. Changing it needs the matching change named in the note.
- **Keep each note true when you edit the code beside it,** and delete it when the rule no longer applies.
- **Don't add notes that restate what the code does.**
- **Find every note** with `grep -rn "agent(" src tests`.
