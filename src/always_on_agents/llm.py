"""The model client: Claude, through the Messages API.

It is called only from Activities. Retries belong to Temporal, so the
Anthropic client is created with ``max_retries=0`` and errors are classified
into retryable and non-retryable ``ApplicationError``s.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, Protocol

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import Settings
from .domain import ModelRequest, ModelResponse
from .store import AppStore

FALLBACK_BETA = "server-side-fallback-2026-07-01"


class ModelClient(Protocol):
    async def complete(self, request: ModelRequest) -> ModelResponse: ...


def build_model(settings: Settings, store: AppStore, client: ModelClient | None = None) -> ModelClient:
    """The model every Activity uses: Claude, with each call recorded in the operation log."""
    return LoggedModel(client or ClaudeModel(settings.claude_model), store)


async def verify_claude_access(model_id: str) -> str:
    """Fail fast at worker start: check credentials and the model with a free lookup."""
    import anthropic

    try:
        model = await anthropic.AsyncAnthropic(max_retries=2).models.retrieve(model_id)
    except (TypeError, anthropic.AuthenticationError, anthropic.PermissionDeniedError) as err:
        raise SystemExit(
            "No working Claude credentials. Set ANTHROPIC_API_KEY in .env (see .env.example) "
            "or run `ant auth login`, then start the worker again."
        ) from err
    except anthropic.NotFoundError as err:
        raise SystemExit(f"Model {model_id!r} is not available to these credentials.") from err
    return model.display_name


class LoggedModel:
    """Records every model call in the operation log so drills can count repeats."""

    def __init__(self, inner: ModelClient, store: AppStore) -> None:
        self._inner = inner
        self._store = store

    async def complete(self, request: ModelRequest) -> ModelResponse:
        attempt = activity.info().attempt if activity.in_activity() else 1
        await asyncio.to_thread(self._store.log_op, "model_call", request.log_key, attempt, request.purpose)
        return await self._inner.complete(request)


# ---------------------------------------------------------------------------
# Claude
# ---------------------------------------------------------------------------


class ClaudeModel:
    def __init__(self, model_id: str, client: Any = None) -> None:
        import anthropic

        self._anthropic = anthropic
        # Credentials resolve from ANTHROPIC_API_KEY or an `ant auth login` profile.
        self._client = client or anthropic.AsyncAnthropic(max_retries=0)
        self._model = model_id

    async def complete(self, request: ModelRequest) -> ModelResponse:
        a = self._anthropic
        output_config: dict[str, Any] = {"effort": request.effort}
        if request.output_schema:
            output_config["format"] = {"type": "json_schema", "schema": request.output_schema}
        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": request.max_tokens,
            "system": request.system,
            "messages": request.messages,
            "output_config": output_config,
            # If a safety classifier declines, the API re-runs the request on
            # Anthropic's recommended fallback model inside the same call.
            "betas": [FALLBACK_BETA],
            "fallbacks": "default",
        }
        if request.tools:
            kwargs["tools"] = request.tools
        try:
            response = await self._client.beta.messages.create(**kwargs)
        except (a.AuthenticationError, a.PermissionDeniedError, a.NotFoundError, a.BadRequestError) as err:
            raise ApplicationError(
                f"Claude API rejected the request: {err}", type=type(err).__name__, non_retryable=True
            ) from err
        except a.RateLimitError as err:
            retry_after = err.response.headers.get("retry-after")
            delay = timedelta(seconds=float(retry_after)) if retry_after else None
            raise ApplicationError(f"Rate limited: {err}", type="RateLimitError", next_retry_delay=delay) from err
        except a.APIStatusError as err:
            retryable = err.status_code >= 500 or err.status_code in (408, 409)
            raise ApplicationError(
                f"Claude API error {err.status_code}: {err}", type="APIStatusError", non_retryable=not retryable
            ) from err
        except a.APIConnectionError as err:  # includes timeouts
            raise ApplicationError(f"Could not reach the Claude API: {err}", type="APIConnectionError") from err
        return ModelResponse(
            stop_reason=response.stop_reason or "end_turn",
            content=[block.to_dict() for block in response.content],
            model=response.model,
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )
