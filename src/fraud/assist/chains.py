"""LangChain chains for the analyst-assist layer (invoked ON DEMAND, off hot path).

* case_summary_chain -> plain-English rationale from txn + SHAP + history
* rag_summary_chain  -> grounded, cited summary using retrieved similar cases

Both are provider-agnostic (fake / openai / anthropic / fine-tuned) via assist.llm.
"""

from __future__ import annotations

import re
from typing import Any

from langchain_core.output_parsers import StrOutputParser

from fraud.assist.llm import describe_provider, get_llm
from fraud.assist.prompts import CASE_SUMMARY_PROMPT, RAG_PROMPT
from fraud.assist.retriever import format_context, similar_cases
from fraud.assist.tracing import configure_tracing
from fraud.logging_config import get_logger
from fraud.schemas import CaseSummary

log = get_logger("chains")


def _fmt_shap(shap_top: list[tuple[str, float]]) -> str:
    if not shap_top:
        return "none available"
    return ", ".join(f"{name}={val:+.2f}" for name, val in shap_top)


def _fmt_txn(doc: dict[str, Any]) -> str:
    return (
        f"txn_id={doc.get('_id')}, user={doc.get('user_id')}, "
        f"amount={doc.get('amount')} {doc.get('currency', '')}, "
        f"merchant={doc.get('merchant')}, risk_score={doc.get('risk_score')}, "
        f"band={doc.get('risk_band')}, ring={doc.get('graph', {}).get('ring_id')}"
    )


def _fmt_history(history: list[dict[str, Any]]) -> str:
    if not history:
        return "no prior transactions"
    return "; ".join(
        f"{h.get('amount')} at {h.get('merchant')} (score {h.get('risk_score')})"
        for h in history[:5]
    )


def _extract_cited_ids(text: str) -> list[str]:
    return sorted(set(re.findall(r"\[([A-Z0-9_]+)\]", text)))


def case_summary(
    txn_doc: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> CaseSummary:
    """Case-summary chain: rationale grounded in SHAP + user history."""
    configure_tracing()
    llm = get_llm()
    chain = CASE_SUMMARY_PROMPT | llm | StrOutputParser()
    text = chain.invoke(
        {
            "txn": _fmt_txn(txn_doc),
            "shap": _fmt_shap(txn_doc.get("shap_top", [])),
            "history": _fmt_history(history or []),
        }
    )
    return CaseSummary(
        txn_id=txn_doc.get("_id", "?"),
        summary=text.strip(),
        cited_case_ids=_extract_cited_ids(text),
        grounded=True,
        model=describe_provider(),
    )


def rag_summary(
    txn_doc: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> CaseSummary:
    """RAG chain: retrieve similar resolved cases + policy, ground with citations."""
    configure_tracing()
    query = _fmt_txn(txn_doc) + " | " + _fmt_shap(txn_doc.get("shap_top", []))
    cases = similar_cases(query, k=3)
    context = format_context(cases)

    llm = get_llm()
    chain = RAG_PROMPT | llm | StrOutputParser()
    text = chain.invoke(
        {
            "txn": _fmt_txn(txn_doc),
            "shap": _fmt_shap(txn_doc.get("shap_top", [])),
            "context": context,
        }
    )
    cited = _extract_cited_ids(text) or [c.get("_id") for c in cases]
    return CaseSummary(
        txn_id=txn_doc.get("_id", "?"),
        summary=text.strip(),
        cited_case_ids=[c for c in cited if c],
        grounded=bool(cases),
        model=describe_provider(),
    )
