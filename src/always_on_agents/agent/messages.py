"""Pure helpers for building Claude Messages API conversations.

Used both by the in-process agent loop (inside an Activity) and by the
Workflow-side loop, so they must stay deterministic and free of I/O.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from temporalio.exceptions import ApplicationError

from ..domain import ModelResponse, ToolResult

# Blocks that must not be echoed back if they appear before a mid-output
# fallback boundary (see the Claude API refusal-fallback docs).
_PRE_FALLBACK_DROP = {"thinking", "redacted_thinking", "tool_use"}


def tool_uses(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [b for b in content if b.get("type") == "tool_use"]


def final_text(content: list[dict[str, Any]]) -> str:
    return "\n".join(b["text"] for b in content if b.get("type") == "text").strip()


def replayable(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return assistant content in the form it should be sent back to the API.

    Thinking blocks are passed back unchanged. The only edit is the documented
    rule for a mid-output fallback: drop thinking/tool_use blocks that precede
    the final ``fallback`` block, and keep the fallback block where it was.
    """
    last_fallback = max((i for i, b in enumerate(content) if b.get("type") == "fallback"), default=-1)
    if last_fallback <= 0:
        return list(content)
    head = [b for b in content[:last_fallback] if b.get("type") not in _PRE_FALLBACK_DROP]
    return head + content[last_fallback:]


def check_stop_reason(response: ModelResponse) -> None:
    """Turn terminal model outcomes into non-retryable failures.

    A refusal survives the server-side fallback chain only if every model
    declined, and a max_tokens stop can leave a tool call half-written.
    Retrying the identical request would not help in either case.
    """
    if response.stop_reason == "refusal":
        raise ApplicationError("The model declined this request.", type="ModelRefusal", non_retryable=True)
    if response.stop_reason == "max_tokens":
        raise ApplicationError("The model hit max_tokens mid-response.", type="ModelTruncated", non_retryable=True)


def tool_result_block(result: ToolResult) -> dict[str, Any]:
    block: dict[str, Any] = {"type": "tool_result", "tool_use_id": result.tool_use_id, "content": result.content}
    if result.is_error:
        block["is_error"] = True
    return block


def approval_digest(tool_name: str, tool_input: dict[str, Any]) -> str:
    """Fingerprint of exactly what an approval authorizes: one tool, one set of parameters."""
    canonical = json.dumps({"tool": tool_name, "input": tool_input}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]
