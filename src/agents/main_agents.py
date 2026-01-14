import os
import logging
from typing import Any, Dict, List

from dotenv import load_dotenv

from .llm_client import get_llm_client


def env_int(name: str, default: int = 0) -> int:
    v = os.getenv(name, str(default)).strip()
    try:
        return int(v)
    except Exception:
        return default


logger = logging.getLogger(__name__)


def main():
    # 1) ENV
    load_dotenv()
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    provider = os.getenv("LLM_PROVIDER", "openai").lower()

    # 2) (optional) Supabase client
    # Пока не нужен для ping. Добавим, когда начнем SQL-агентов.

    # 3) FLAGS
    RUN_DAILY_SQL_REPORT = env_int("RUN_DAILY_SQL_REPORT", 0)
    RUN_LLM_SUMMARY = env_int("RUN_LLM_SUMMARY", 0)

    # 4) PARAMS
    model = os.getenv("LLM_MODEL", "gpt-4o-mini")

    totals = None
    rows: List[Dict[str, Any]] = []

    # 5) RUN STEPS
    if RUN_DAILY_SQL_REPORT == 1:
        from .supabase_client import get_supabase
        from .daily_sql_agent import run_daily_sql_report

        supabase = get_supabase()
        totals, rows = run_daily_sql_report(
            supabase,
            limit=int(os.getenv("DAILY_REPORT_LIMIT", "20")),
        )
        logger.info("=== TOTALS (python) ===")
        logger.info(
            "top_n=%d revenue=%.0f spend=%.0f drr%%=%.2f clicks=%d orders=%d",
            totals["top_n"],
            totals["total_revenue"],
            totals["total_spend"],
            totals["drr_percent"],
            totals["total_clicks"],
            totals["total_orders"],
        )
    if RUN_LLM_SUMMARY == 1 and rows:
        from .daily_sql_agent import rows_to_brief

        provider = os.getenv("LLM_PROVIDER", "openai")
        model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        client = get_llm_client(provider)

        brief = rows_to_brief(rows, top_n=int(os.getenv("DAILY_REPORT_TOP_N", "10")))

        prompt = f"""
        Ты performance-маркетолог WB. У тебя есть ТОП-10 позиций с выручкой/расходом/DRR/кликами/заказами.

        ВАЖНО:
        - Do NOT repeat totals. Do NOT invent numbers. Do NOT recompute sums.
        - Используй только строки из TOP ITEMS.
        - DRR%: меньше = лучше.
        - Сделай выводы на основе tag каждого ряда и кратко упоминай DRR и rev.

        Возвращай строго JSON в формате:
        {
          "insights": [".."],
          "alerts": [{"nmid": ..., "reason": "..."}],
          "actions": {"PAUSE": [...], "CUT": [...], "SCALE": [...], "WATCH": [...]}
        }

        TOP ITEMS:
        {brief}
        """.strip()

        try:
            resp = client.chat_completion(model=model, messages=[{"role": "user", "content": prompt}], temperature=0.2)
        except Exception as exc:
            logger.exception("LLM summary failed: %s", exc)
            return

        logger.info("=== LLM SUMMARY ===")
        # resp expected to be a dict-like result from wrapper
        content = resp.get("content") or resp.get("text") or str(resp)
        logger.info("%s", content)


if __name__ == "__main__":
    main()
