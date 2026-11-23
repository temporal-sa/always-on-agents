"""A deterministic test double for the model, used only by the test suite.

It makes the same kinds of decisions a real model makes in this app (which
tools to call, what to write), so tests are free, fast, and repeatable. The
application itself always runs on real models; nothing under ``src/`` can
select this.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from always_on_agents.domain import ModelRequest, ModelResponse


def _text(text: str) -> ModelResponse:
    return ModelResponse(stop_reason="end_turn", content=[{"type": "text", "text": text}], model="fake")


def _tool_calls(messages: list[dict[str, Any]], calls: list[tuple[str, dict[str, Any]]]) -> ModelResponse:
    turn = len(messages)
    content = [
        {"type": "tool_use", "id": f"toolu_{turn:02d}_{i}_{name}", "name": name, "input": args}
        for i, (name, args) in enumerate(calls)
    ]
    return ModelResponse(stop_reason="tool_use", content=content, model="fake")


class _History:
    """Tool calls and results visible in a conversation, optionally since a message index."""

    def __init__(self, messages: list[dict[str, Any]], start: int = 0) -> None:
        self.calls: dict[str, tuple[str, dict[str, Any]]] = {}
        self.results: dict[str, dict[str, Any]] = {}
        for message in messages[start:]:
            if not isinstance(message.get("content"), list):
                continue
            for block in message["content"]:
                if block.get("type") == "tool_use":
                    self.calls[block["id"]] = (block["name"], block["input"])
                elif block.get("type") == "tool_result":
                    self.results[block["tool_use_id"]] = block

    def called(self, name: str) -> bool:
        return any(n == name for n, _ in self.calls.values())

    def result(self, name: str) -> dict[str, Any] | None:
        for tool_use_id, (n, _) in reversed(list(self.calls.items())):
            if n == name and tool_use_id in self.results:
                return self.results[tool_use_id]
        return None

    def result_json(self, name: str) -> Any:
        block = self.result(name)
        if block is None or block.get("is_error"):
            return None
        try:
            return json.loads(block["content"])
        except (TypeError, json.JSONDecodeError):
            return None


def _turn_start(messages: list[dict[str, Any]]) -> tuple[int, str, list[str]]:
    """Where the current turn began, its opening text, and any text that joined it later.

    A turn opens with a plain-string user message; messages that join a running
    turn arrive as text blocks next to tool results.
    """
    start = max((i for i, m in enumerate(messages) if m["role"] == "user" and isinstance(m["content"], str)), default=0)
    joined = [
        b["text"]
        for m in messages[start + 1 :]
        if m["role"] == "user" and isinstance(m["content"], list)
        for b in m["content"]
        if b.get("type") == "text"
    ]
    opening = messages[start]["content"] if messages and isinstance(messages[start]["content"], str) else ""
    return start, opening, joined


class FakeModel:
    """Deterministic decisions that mirror what a tool-using model would do."""

    def __init__(self, latency_seconds: float = 0.0) -> None:
        self._latency = latency_seconds

    async def complete(self, request: ModelRequest) -> ModelResponse:
        if self._latency:
            await asyncio.sleep(self._latency)
        handler = {
            "briefing": self._briefing,
            "session": self._session,
            "research": self._research,
            "memory": self._memory,
        }[request.purpose]
        return handler(request.messages)

    def _briefing(self, messages: list[dict[str, Any]]) -> ModelResponse:
        h = _History(messages)
        if not h.called("read_memory"):
            return _tool_calls(
                messages,
                [
                    ("read_memory", {}),
                    ("fetch_documents", {}),
                    ("start_research", {"question": "What changed in competitor pricing this week?"}),
                ],
            )
        if h.called("start_research") and not h.called("get_research_result"):
            job = h.result_json("start_research") or {}
            return _tool_calls(messages, [("get_research_result", {"job_id": job.get("job_id", "")})])
        if not h.called("write_report"):
            markdown = _compose_briefing(
                h.result_json("read_memory") or {},
                h.result_json("fetch_documents") or [],
                h.result_json("get_research_result") or {},
            )
            return _tool_calls(messages, [("write_report", {"title": "Weekly briefing", "markdown": markdown})])
        report = h.result_json("write_report") or {}
        return _text(f"Your weekly briefing is ready (report {report.get('report_id', '?')}).")

    def _session(self, messages: list[dict[str, Any]]) -> ModelResponse:
        start, text, joined = _turn_start(messages)
        h = _History(messages, start)
        also = f" Also noted: {' '.join(joined)}" if joined else ""

        # A timer fired: do what the note says.
        if text.startswith("[Automated wake-up]"):
            note = text.split("Your note to yourself:", 1)[-1].split("\n", 1)[0].strip()
            quoted = re.search(r'"([^"]+)"', note)
            return _text(quoted.group(1) if quoted else f"Reminder: {note}")

        # The recurring weekly briefing.
        weekly = _weekly_request(text)
        if weekly and not any(h.called(n) for n in WEEKLY_TOOLS):
            name, args = weekly
            return _tool_calls(messages, [(name, args)])
        for name in WEEKLY_TOOLS:
            if h.called(name):
                block = h.result(name) or {}
                if block.get("is_error"):
                    return _text(f"I couldn't do that: {block.get('content', '')}")
                if name == "cancel_weekly_briefing":
                    return _text("Your weekly briefing is cancelled.")
                info = (h.result_json(name) or {}).get("weekly_briefing")
                return _text(
                    f"Your weekly briefing runs {info['when']} ({info['time_zone']})."
                    if info
                    else "You have no weekly briefing."
                )

        delay = _delay_seconds(text)
        if "briefing" in text.lower() and not h.called("schedule_briefing"):
            return _tool_calls(messages, [("schedule_briefing", {"delay_seconds": delay or 0})])
        if h.called("schedule_briefing"):
            scheduled = h.result_json("schedule_briefing") or {}
            return _text(f"Your briefing {scheduled.get('briefing_id', '')} is on its way.")
        if delay and not h.called("set_timer"):
            return _tool_calls(
                messages, [("set_timer", {"delay_seconds": delay, "note": f"Do this for the user: {text}"})]
            )
        if h.called("set_timer"):
            return _text(f"OK, I'll do that in {delay} seconds.")

        share = re.search(r"share\b.*?\bwith\s+(\S+@\S+)", text, re.IGNORECASE)
        if share and not h.called("share_report"):
            recipient = share.group(1).rstrip(".,;")
            return _tool_calls(
                messages, [("share_report", {"recipient": recipient, "note": "From my weekly briefing."})]
            )
        if h.called("share_report"):
            block = h.result("share_report") or {}
            if block.get("is_error"):
                return _text(f"I did not share the report: {block.get('content', '')}{also}")
            return _text(f"Done. I shared your latest report.{also}")
        return _text(f"Noted: {text.strip()}{also}")

    def _research(self, messages: list[dict[str, Any]]) -> ModelResponse:
        payload = json.loads(messages[-1]["content"])
        names = ", ".join(s["title"] for s in payload["sources"])
        snippets = " ".join(s["snippet"] for s in payload["sources"])
        return _text(f"{snippets} (Sources: {names}.)")

    def _memory(self, messages: list[dict[str, Any]]) -> ModelResponse:
        payload = json.loads(messages[-1]["content"])
        notes = [dict(n) for n in payload["current_notes"]]
        seen = {n["text"].lower() for n in notes}
        for interaction in payload["interactions"]:
            text = interaction["text"].strip()
            note = f"User preference: {text}"
            if note.lower() not in seen:
                notes.append({"text": note, "sources": [interaction["interaction_id"]]})
                seen.add(note.lower())
        return _text(json.dumps({"notes": notes}))


def _compose_briefing(memory: dict[str, Any], docs: list[dict[str, Any]], research: dict[str, Any]) -> str:
    notes = " ".join(n["text"].lower() for n in memory.get("notes", []))
    decisions_first = "decision" in notes and any(w in notes for w in ("lead", "first", "top"))
    decisions = "\n".join(f"- **{d['title']}**: {d['body']}" for d in docs if d["kind"] == "decision")
    updates = "\n".join(f"- **{d['title']}**: {d['body']}" for d in docs if d["kind"] == "update")
    sections = [
        "## Decisions that need your attention\n" + decisions,
        "## Updates\n" + updates,
    ]
    if not decisions_first:
        sections.reverse()
    if research.get("summary"):
        sections.append("## Research\n" + research["summary"])
    return "# Weekly briefing\n\n" + "\n\n".join(sections) + "\n"


WEEKLY_TOOLS = ("set_weekly_briefing", "get_weekly_briefing", "cancel_weekly_briefing")
_WEEKDAY = re.compile(r"\bevery\s+(mon|tues|wednes|thurs|fri|satur|sun)day\b", re.IGNORECASE)
_AT = re.compile(r"\bat\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", re.IGNORECASE)
_ZONE = re.compile(r"\b([A-Z][a-z]+/[A-Za-z_]+)\b")


def _weekly_request(text: str) -> tuple[str, dict[str, Any]] | None:
    """'every Monday at 8am America/New_York' -> set; 'cancel my weekly ...' -> cancel."""
    lower = text.lower()
    if "weekly" in lower and ("cancel" in lower or "stop" in lower):
        return "cancel_weekly_briefing", {}
    if "weekly" in lower and "when" in lower:
        return "get_weekly_briefing", {}
    day = _WEEKDAY.search(text)
    if not day:
        return None
    hour, minute = 7, 0
    if at := _AT.search(text):
        hour, minute = int(at.group(1)), int(at.group(2) or 0)
        meridiem = (at.group(3) or "").lower()
        if meridiem == "pm" and hour < 12:
            hour += 12
        elif meridiem == "am" and hour == 12:
            hour = 0
    zone = _ZONE.search(text)
    return "set_weekly_briefing", {
        "day_of_week": day.group(1)[:3].lower(),
        "time": f"{hour:02d}:{minute:02d}",
        "time_zone": zone.group(1) if zone else "UTC",
    }


def _delay_seconds(text: str) -> int:
    """'in 30 seconds' -> 30, 'in 2 minutes' -> 120, otherwise 0."""
    match = re.search(r"\bin\s+(\d+)\s*(seconds?|secs?|s|minutes?|mins?|m)\b", text, re.IGNORECASE)
    if not match:
        return 0
    value = int(match.group(1))
    return value * 60 if match.group(2).lower().startswith("m") else value
