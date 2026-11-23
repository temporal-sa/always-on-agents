"""The model clients: Claude (Claude API or Amazon Bedrock) and OpenAI.

``AGENT_MODEL_PROVIDER`` picks one, and ``build_model`` returns it. Every
client has the same one-method shape, ``complete(ModelRequest) ->
ModelResponse``, so Activities and Workflows never know which provider
answered. The conversation is kept in the Claude Messages format throughout,
and the OpenAI adapter (``llm_openai.py``) translates at the edge.

Every client is called only from Activities. Retries belong to Temporal, so
each SDK client is created with ``max_retries=0`` and errors are classified
into retryable and non-retryable ``ApplicationError``s.

Why not let the SDK retry too? Two retry layers multiply. With the SDK's
default of two retries, ten Temporal attempts could make thirty calls, and
Temporal's history would show only ten. With one layer, the Activity's retry
policy is the whole story. A rate limit's ``retry-after`` becomes
``next_retry_delay``, so Temporal waits exactly as long as the API asked.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from typing import Any, Protocol

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import Settings
from .domain import ModelRequest, ModelResponse
from .store import AppStore

FALLBACK_BETA = "server-side-fallback-2026-07-01"
# Server-side fallbacks aren't offered on Bedrock, so the SDK's client-side
# middleware retries a refused request on this model instead.
BEDROCK_FALLBACK_MODEL = "anthropic.claude-opus-4-8"
# Bedrock API keys; without one, the Bedrock client signs requests with AWS credentials.
BEDROCK_API_KEY_VARS = ("AWS_BEARER_TOKEN_BEDROCK", "ANTHROPIC_AWS_API_KEY")

# agent(constraint): llm_openai.py writes OpenAI reasoning items into the conversation as
# blocks of this type, and ClaudeModel drops them. Change both together.
OPENAI_REASONING = "openai_reasoning"


class ModelClient(Protocol):
    async def complete(self, request: ModelRequest) -> ModelResponse: ...


def build_model(settings: Settings, store: AppStore, client: ModelClient | None = None) -> ModelClient:
    """The model every Activity uses, with each call recorded in the operation log."""
    # agent(invariant): the app always runs on real models, from the configured provider. ``client`` exists
    # only for tests/fake_model.py. Don't add a scripted, offline, or mock model; the owner
    # removed one on purpose.
    return LoggedModel(client or provider_model(settings), store)


def provider_model(settings: Settings) -> ModelClient:
    if settings.model_provider == "bedrock":
        return ClaudeModel.on_bedrock(settings.bedrock_model)
    if settings.model_provider == "openai":
        from .llm_openai import OpenAIModel  # local import: llm_openai imports this module

        return OpenAIModel(settings.openai_model)
    return ClaudeModel(settings.claude_model)


async def verify_model_access(settings: Settings) -> str:
    """Fail fast at worker start: check credentials and the model without paying for a call.

    Returns a description of the model for the startup log.
    """
    if settings.model_provider == "openai":
        return await _verify_openai(settings.openai_model)
    if settings.model_provider == "bedrock":
        return _verify_bedrock(settings.bedrock_model)
    return await _verify_claude_api(settings.claude_model)


async def _verify_claude_api(model_id: str) -> str:
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


def _verify_bedrock(model_id: str) -> str:
    # The Bedrock endpoint has no free model lookup, so this checks what can be
    # checked locally: the AWS libraries, a region, and some credentials.
    try:
        import boto3
    except ImportError as err:
        raise SystemExit("Bedrock needs the AWS libraries: run `uv sync --extra bedrock`.") from err
    has_api_key = any(os.environ.get(v) for v in BEDROCK_API_KEY_VARS)
    if not has_api_key and boto3.Session().get_credentials() is None:
        raise SystemExit("No AWS credentials. Set AWS_BEARER_TOKEN_BEDROCK, or sign in with the AWS CLI.")
    try:
        client = ClaudeModel.on_bedrock(model_id)
    except Exception as err:  # the SDK raises AnthropicError when no region is set
        raise SystemExit(f"Can't use Amazon Bedrock: {err}") from err
    return f"{model_id} on Amazon Bedrock ({client.endpoint})"


async def _verify_openai(model_id: str) -> str:
    import openai

    try:
        client = openai.AsyncOpenAI(max_retries=2)
    except openai.OpenAIError as err:  # raised when no API key is configured
        raise SystemExit("No OpenAI API key. Set OPENAI_API_KEY in .env (see .env.example).") from err
    try:
        model = await client.models.retrieve(model_id)
    except (openai.AuthenticationError, openai.PermissionDeniedError) as err:
        raise SystemExit("OpenAI rejected these credentials. Check OPENAI_API_KEY in .env.") from err
    except openai.NotFoundError as err:
        raise SystemExit(f"Model {model_id!r} is not available to these OpenAI credentials.") from err
    return f"{model.id} on OpenAI"


def api_error(sdk: Any, err: Exception, service: str) -> ApplicationError:
    """Classify an SDK error for Temporal: retry what can succeed later, fail the rest.

    The Anthropic and OpenAI SDKs share one exception hierarchy, so this serves
    both; ``sdk`` is the ``anthropic`` or ``openai`` module.
    """
    name = type(err).__name__
    if isinstance(err, (sdk.AuthenticationError, sdk.PermissionDeniedError, sdk.NotFoundError, sdk.BadRequestError)):
        return ApplicationError(f"{service} rejected the request: {err}", type=name, non_retryable=True)
    if isinstance(err, sdk.RateLimitError):
        if getattr(err, "code", None) == "insufficient_quota":
            # OpenAI's out-of-credit error is a 429 too, but waiting won't fix it.
            return ApplicationError(f"{service} quota exhausted: {err}", type="InsufficientQuota", non_retryable=True)
        retry_after = err.response.headers.get("retry-after")
        delay = timedelta(seconds=float(retry_after)) if retry_after else None
        return ApplicationError(f"Rate limited: {err}", type="RateLimitError", next_retry_delay=delay)
    if isinstance(err, sdk.APIStatusError):
        retryable = err.status_code >= 500 or err.status_code in (408, 409)
        return ApplicationError(
            f"{service} error {err.status_code}: {err}", type="APIStatusError", non_retryable=not retryable
        )
    # Connection errors and timeouts: the next attempt may well get through.
    return ApplicationError(f"Could not reach the {service}: {err}", type=name)


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
# Claude, on the Claude API or Amazon Bedrock
# ---------------------------------------------------------------------------


class ClaudeModel:
    def __init__(self, model_id: str, client: Any = None, *, server_fallbacks: bool = True) -> None:
        import anthropic

        self._anthropic = anthropic
        # Credentials resolve from ANTHROPIC_API_KEY or an `ant auth login` profile.
        self._client = client or anthropic.AsyncAnthropic(max_retries=0)
        self._model = model_id
        self._server_fallbacks = server_fallbacks

    @classmethod
    def on_bedrock(cls, model_id: str) -> ClaudeModel:
        """Claude on Amazon Bedrock, through Anthropic's Bedrock (Mantle) client.

        Same Messages API, so everything else in this class is shared. The
        region comes from AWS_REGION, and credentials from AWS_BEARER_TOKEN_BEDROCK
        or the standard AWS chain (environment, profile, SSO).
        """
        import anthropic

        client = anthropic.AsyncAnthropicBedrockMantle(
            max_retries=0,
            middleware=[anthropic.BetaRefusalFallbackMiddleware([{"model": BEDROCK_FALLBACK_MODEL}])],
        )
        return cls(model_id, client, server_fallbacks=False)

    @property
    def endpoint(self) -> str:
        return str(self._client.base_url)

    async def complete(self, request: ModelRequest) -> ModelResponse:
        a = self._anthropic
        output_config: dict[str, Any] = {"effort": request.effort}
        if request.output_schema:
            output_config["format"] = {"type": "json_schema", "schema": request.output_schema}
        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": request.max_tokens,
            "system": request.system,
            "messages": _claude_messages(request.messages),
            "output_config": output_config,
        }
        if self._server_fallbacks:
            # If a safety classifier declines, the API re-runs the request on
            # Anthropic's recommended fallback model inside the same call.
            kwargs["betas"] = [FALLBACK_BETA]
            kwargs["fallbacks"] = "default"
        if request.tools:
            kwargs["tools"] = request.tools
        try:
            response = await self._client.beta.messages.create(**kwargs)
        except a.APIError as err:
            raise api_error(a, err, "Claude API") from err
        return ModelResponse(
            stop_reason=response.stop_reason or "end_turn",
            content=[block.to_dict() for block in response.content],
            model=response.model,
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )


def _claude_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The conversation without OpenAI reasoning blocks, which Claude can't read.

    They are there only if the provider changed partway through a conversation.
    """
    kept = []
    for message in messages:
        content = message["content"]
        if isinstance(content, list):
            content = [b for b in content if b.get("type") != OPENAI_REASONING]
            if not content:
                continue
        kept.append({**message, "content": content})
    return kept
