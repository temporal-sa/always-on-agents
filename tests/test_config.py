import pytest

from always_on_agents.config import Settings, load_dotenv
from always_on_agents.llm import ClaudeModel, provider_model
from always_on_agents.llm_openai import OpenAIModel


def test_load_dotenv_skips_blanks_and_respects_existing(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text(
        "# comment\nANTHROPIC_API_KEY=\nexport AGENT_CLAUDE_MODEL='claude-opus-5-5'\nTEMPORAL_ADDRESS=from-file\n"
    )
    for key in ("ANTHROPIC_API_KEY", "AGENT_CLAUDE_MODEL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("TEMPORAL_ADDRESS", "from-shell")

    load_dotenv(env)

    import os

    assert "ANTHROPIC_API_KEY" not in os.environ  # blank value must not shadow an ant profile
    assert os.environ["AGENT_CLAUDE_MODEL"] == "claude-opus-5-5"
    assert os.environ["TEMPORAL_ADDRESS"] == "from-shell"


def test_provider_selects_its_own_model(monkeypatch):
    monkeypatch.setenv("AGENT_MODEL_PROVIDER", " OpenAI ")
    monkeypatch.setenv("AGENT_OPENAI_MODEL", "gpt-test")
    settings = Settings.from_env()
    assert (settings.model_provider, settings.model_id) == ("openai", "gpt-test")
    assert Settings().model_id == "claude-opus-5-5"


def test_unknown_provider_is_rejected():
    with pytest.raises(ValueError, match="AGENT_MODEL_PROVIDER"):
        Settings(model_provider="gemini")


def test_each_provider_gets_its_client(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    assert isinstance(provider_model(Settings(model_provider="openai")), OpenAIModel)
    assert isinstance(provider_model(Settings()), ClaudeModel)
