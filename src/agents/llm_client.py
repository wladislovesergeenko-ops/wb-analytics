import os
from openai import OpenAI

from .llm_wrapper import OpenAIWrapper


def get_llm_client(provider: str):
    """Return a wrapped LLM client implementing chat_completion(...).

    This abstracts the underlying SDK and provides retry/backoff behavior.
    """
    provider = (provider or "openai").lower()

    if provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is missing")
        raw = OpenAI(api_key=api_key)
        return OpenAIWrapper(raw)

    if provider == "openrouter":
        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            raise RuntimeError("OPENROUTER_API_KEY is missing")

        base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
        headers = {}
        http_referer = os.getenv("OPENROUTER_HTTP_REFERER")
        app_title = os.getenv("OPENROUTER_APP_TITLE")
        if http_referer:
            headers["HTTP-Referer"] = http_referer
        if app_title:
            headers["X-Title"] = app_title

        raw = OpenAI(api_key=api_key, base_url=base_url, default_headers=headers or None)
        return OpenAIWrapper(raw)

    raise ValueError(f"Unknown provider: {provider}. Use 'openai' or 'openrouter'.")
