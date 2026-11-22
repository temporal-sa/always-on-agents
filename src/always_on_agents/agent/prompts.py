"""System prompts, tool definitions, and output schemas for the briefing assistant."""

from __future__ import annotations

from typing import Any

BRIEFING_SYSTEM = """\
You are a briefing assistant that prepares a weekly briefing for one user.

Work in this order:
1. Call read_memory to learn the user's standing preferences, and fetch_documents
   to get this week's material. You can call both at once.
2. If one open question would make the briefing better, call start_research once,
   then get_research_result with the job_id it returns.
3. Call write_report exactly once with the finished briefing in Markdown. Follow
   the user's remembered preferences about structure and emphasis.
4. Reply with one short sentence telling the user the briefing is ready.
"""

SESSION_SYSTEM = """\
You are the user's always-on assistant in an ongoing conversation. You keep
working after a reply: you can schedule work for later and wake yourself up.

What you can do:
- read_memory and fetch_documents: the user's standing preferences and this week's material.
- schedule_briefing: have the weekly briefing prepared once, now or after a delay. It runs
  in the background and is delivered to the user by email.
- set_weekly_briefing, get_weekly_briefing, cancel_weekly_briefing: the user's recurring
  briefing, prepared and emailed every week until cancelled. Use these when the user wants
  a briefing on a regular day and time, and schedule_briefing for a one-off. If the user
  doesn't give a time zone, use UTC and say so.
- start_research, then get_research_result: run a background research job.
- set_timer: wake yourself up later in this conversation. Use it whenever the user asks
  you to do or say something later ("in 30 seconds", "in 10 minutes", "remind me").
  Write the note as an instruction to your future self. When the timer fires you get a
  message starting with [Automated wake-up]; do what your note says, and your reply is
  shown to the user. Never say you cannot act later: use set_timer.
- write_report: save a report.
- share_report: send the user's latest report to someone. The system asks the user to
  approve it first. If the approval is declined or expires, say so plainly and do not
  retry unless asked.

Keep replies short.
"""

RESEARCH_SYSTEM = """\
You are a research subagent. Answer the question in at most three sentences using
only the sources provided, and name the sources you relied on.
"""

MEMORY_SYSTEM = """\
You curate long-term memory notes for a briefing assistant. You receive the current
notes and a batch of new interactions with the user.

Return the complete revised list of notes:
- Keep durable preferences, corrections, and facts about how the user wants to work.
- Revise or drop notes that the new interactions contradict or make outdated.
- Merge notes that say the same thing.
- Ignore small talk and one-off requests that will not matter next time.
- Every note must cite its sources: interaction ids from this batch, or the sources
  of the existing note it came from.
"""

MEMORY_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "notes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["text", "sources"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["notes"],
    "additionalProperties": False,
}


def _tool(name: str, description: str, properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "input_schema": {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        },
    }


READ_MEMORY = _tool(
    "read_memory",
    "Read the user's curated memory notes (preferences and corrections from earlier conversations).",
    {},
    [],
)
FETCH_DOCUMENTS = _tool(
    "fetch_documents",
    "Fetch this week's documents for the user: pending decisions, project updates, and metrics.",
    {},
    [],
)
START_RESEARCH = _tool(
    "start_research",
    "Start a background research job for one question. Returns a job_id immediately; "
    "call get_research_result to collect the answer.",
    {"question": {"type": "string", "description": "The question to research."}},
    ["question"],
)
GET_RESEARCH_RESULT = _tool(
    "get_research_result",
    "Get the result of a research job started with start_research. Waits briefly; "
    "returns status 'running' if the job is not finished yet.",
    {"job_id": {"type": "string"}},
    ["job_id"],
)
WRITE_REPORT = _tool(
    "write_report",
    "Save the finished briefing. Calling it again in the same turn replaces the earlier version.",
    {"title": {"type": "string"}, "markdown": {"type": "string"}},
    ["title", "markdown"],
)
SHARE_REPORT = _tool(
    "share_report",
    "Share the user's latest report with another person. Requires the user's approval.",
    {
        "recipient": {"type": "string", "description": "Email address of the recipient."},
        "note": {"type": "string", "description": "Optional short note to include."},
    },
    ["recipient"],
)

SCHEDULE_BRIEFING = _tool(
    "schedule_briefing",
    "Prepare the user's weekly briefing in the background, now or after a delay, and email it to them. "
    "Returns the briefing id right away.",
    {"delay_seconds": {"type": "integer", "description": "0 for now, otherwise seconds from now (max 30 days)."}},
    ["delay_seconds"],
)
SET_TIMER = _tool(
    "set_timer",
    "Wake yourself up later in this conversation. When it fires you receive an [Automated wake-up] message "
    "with your note, and your reply is shown to the user.",
    {
        "delay_seconds": {"type": "integer", "description": "Seconds from now, 1 to 2592000 (30 days)."},
        "note": {"type": "string", "description": "What to do when you wake up, written to your future self."},
    },
    ["delay_seconds", "note"],
)

SET_WEEKLY_BRIEFING = _tool(
    "set_weekly_briefing",
    "Create the user's recurring weekly briefing, or move it to a new day and time. It is prepared and emailed "
    "every week until cancelled. Returns when it runs and the next run times.",
    {
        "day_of_week": {"type": "string", "enum": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
        "time": {"type": "string", "description": "24-hour time of day in that time zone, HH:MM, e.g. 07:30."},
        "time_zone": {"type": "string", "description": "IANA time zone, e.g. America/Los_Angeles, or UTC."},
    },
    ["day_of_week", "time", "time_zone"],
)
GET_WEEKLY_BRIEFING = _tool(
    "get_weekly_briefing",
    "Show the user's recurring weekly briefing: when it runs and the next run times, or that there is none.",
    {},
    [],
)
CANCEL_WEEKLY_BRIEFING = _tool(
    "cancel_weekly_briefing",
    "Stop the user's recurring weekly briefing. Past briefings and reports are kept.",
    {},
    [],
)

BRIEFING_TOOLS = [READ_MEMORY, FETCH_DOCUMENTS, START_RESEARCH, GET_RESEARCH_RESULT, WRITE_REPORT]
SESSION_TOOLS = [
    READ_MEMORY,
    FETCH_DOCUMENTS,
    SCHEDULE_BRIEFING,
    SET_WEEKLY_BRIEFING,
    GET_WEEKLY_BRIEFING,
    CANCEL_WEEKLY_BRIEFING,
    START_RESEARCH,
    GET_RESEARCH_RESULT,
    SET_TIMER,
    WRITE_REPORT,
    SHARE_REPORT,
]
MAX_DELAY_SECONDS = 30 * 24 * 3600

# Tools whose effects reach other people. The session asks for approval before
# running them.
APPROVAL_REQUIRED = {"share_report"}

# Per-tool Activity timeouts (seconds). get_research_result may wait on a job.
# agent(constraint): must exceed RESEARCH_WAIT_SECONDS (agent/tools.py), or the wait times out
# the Activity instead of returning "running".
TOOL_TIMEOUTS = {"get_research_result": 120}
DEFAULT_TOOL_TIMEOUT = 30
