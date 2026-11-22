"""Runtime settings shared by the worker, the CLI, and the tests.

Everything is read from environment variables so the same code runs against a
local dev server or Temporal Cloud (connection settings come from Temporal's
standard env/TOML profile loader, see ``worker.connect``). The worker and CLI
first load a ``.env`` file from the current directory, if there is one.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def load_dotenv(path: Path | str = ".env") -> None:
    """Load KEY=VALUE lines into the environment.

    Variables already set in the environment win. Blank values are skipped:
    an empty ``ANTHROPIC_API_KEY=`` would otherwise shadow an ``ant auth login``
    profile, which the Anthropic SDK only consults when no key is set.
    """
    path = Path(path)
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.removeprefix("export ").split("=", 1)
        key, value = key.strip(), value.strip().strip("'\"")
        if key and value and key not in os.environ:
            os.environ[key] = value


PROVIDERS = ("anthropic", "bedrock", "openai")


@dataclass(frozen=True)
class Settings:
    # Separate task queues let interactive turns, scheduled automations, and
    # background maintenance scale independently, so a backlog of scheduled or
    # background work never delays an interactive reply.
    interactive_queue: str = "agent-interactive"
    scheduled_queue: str = "agent-scheduled"
    background_queue: str = "agent-background"

    # Long-lived controller for the hot pool of sandbox VMs.
    pool_workflow_id: str = "workspace-pool"

    # Where the "application database" (SQLite) and fake VM/snapshot files live.
    data_dir: Path = field(default_factory=lambda: Path(".data"))

    # Which service the agent's model runs on: "anthropic" (the Claude API),
    # "bedrock" (Claude on Amazon Bedrock), or "openai". Each has its own model
    # setting, so switching providers is one variable.
    model_provider: str = "anthropic"
    claude_model: str = "claude-opus-5-5"
    bedrock_model: str = "anthropic.claude-opus-5-5"
    openai_model: str = "gpt-6.2-sol"

    def __post_init__(self) -> None:
        if self.model_provider not in PROVIDERS:
            raise ValueError(f"AGENT_MODEL_PROVIDER must be one of {', '.join(PROVIDERS)}, not {self.model_provider!r}")

    @property
    def model_id(self) -> str:
        """The model the configured provider runs."""
        return {"anthropic": self.claude_model, "bedrock": self.bedrock_model, "openai": self.openai_model}[
            self.model_provider
        ]

    @property
    def db_path(self) -> Path:
        return self.data_dir / "agent.db"

    @property
    def all_queues(self) -> tuple[str, str, str]:
        return (self.interactive_queue, self.scheduled_queue, self.background_queue)

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            interactive_queue=os.environ.get("AGENT_INTERACTIVE_QUEUE", cls.interactive_queue),
            scheduled_queue=os.environ.get("AGENT_SCHEDULED_QUEUE", cls.scheduled_queue),
            background_queue=os.environ.get("AGENT_BACKGROUND_QUEUE", cls.background_queue),
            pool_workflow_id=os.environ.get("AGENT_POOL_WORKFLOW_ID", cls.pool_workflow_id),
            data_dir=Path(os.environ.get("AGENT_DATA_DIR", ".data")),
            model_provider=os.environ.get("AGENT_MODEL_PROVIDER", cls.model_provider).strip().lower(),
            claude_model=os.environ.get("AGENT_CLAUDE_MODEL", cls.claude_model),
            bedrock_model=os.environ.get("AGENT_BEDROCK_MODEL", cls.bedrock_model),
            openai_model=os.environ.get("AGENT_OPENAI_MODEL", cls.openai_model),
        )
