"""Activities for the research subagent that backs the ``start_research`` tool."""

from __future__ import annotations

import json

from temporalio import activity

from ..agent.messages import check_stop_reason, final_text
from ..agent.prompts import RESEARCH_SYSTEM
from ..agent.sample_data import research_sources
from ..domain import ModelRequest, ResearchInput
from ..llm import ModelClient
from ..store import AppStore
from .heartbeat import heartbeating


class ResearchActivities:
    def __init__(self, store: AppStore, model: ModelClient) -> None:
        self._store = store
        self._model = model

    @activity.defn
    def gather_sources(self, request: ResearchInput) -> list[dict[str, str]]:
        self._store.log_op("research:gather", activity.info().workflow_id or "", activity.info().attempt)
        return research_sources(request.question)

    @activity.defn
    async def summarize_sources(self, request: ResearchInput, sources: list[dict[str, str]]) -> str:
        async with heartbeating():
            response = await self._model.complete(
                ModelRequest(
                    purpose="research",
                    system=RESEARCH_SYSTEM,
                    messages=[
                        {"role": "user", "content": json.dumps({"question": request.question, "sources": sources})}
                    ],
                    effort="low",
                    log_key=activity.info().workflow_id or "",
                )
            )
        check_stop_reason(response)
        return final_text(response.content)
