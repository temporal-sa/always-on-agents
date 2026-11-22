from always_on_agents.config import load_dotenv


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
