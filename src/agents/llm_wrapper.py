from typing import Any, Dict, List
import time
import logging

logger = logging.getLogger(__name__)


class OpenAIWrapper:
    """Простейшая обёртка вокруг OpenAI SDK-объекта, реализующая retry и унифицированный метод chat_completion.

    wrapper.chat_completion возвращает словарь: {"content": str, "raw": <raw_response>}
    """

    def __init__(self, client, max_retries: int = 3, backoff_factor: float = 1.0):
        self.client = client
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def chat_completion(self, model: str, messages: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        attempt = 0
        while True:
            try:
                # Используем тот же вызов, что и в вашем коде
                resp = self.client.chat.completions.create(model=model, messages=messages, **kwargs)
                # Попробуем получить содержимое в совместимом виде
                content = None
                try:
                    content = resp.choices[0].message.content
                except Exception:
                    # fallback: попытаться прочитать text или raw
                    content = getattr(resp, "text", None) or str(resp)
                return {"content": content, "raw": resp}
            except Exception as exc:
                attempt += 1
                logger.warning("LLM call failed (attempt %d/%d): %s", attempt, self.max_retries, exc)
                if attempt >= self.max_retries:
                    logger.exception("LLM call permanently failed")
                    raise
                sleep_for = self.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_for)
