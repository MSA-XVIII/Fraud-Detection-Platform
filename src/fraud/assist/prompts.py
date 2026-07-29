"""Grounded, cited prompt templates for the analyst-assist chains.

Prompts force grounding: the model may only assert what is supported by the
provided SHAP features / retrieved cases, must cite retrieved case ids, and must
say when evidence is insufficient. Summaries stay under ~120 words.
"""

from __future__ import annotations

from langchain_core.prompts import ChatPromptTemplate, PromptTemplate

SYSTEM_RULES = (
    "You are a fraud analyst assistant. You may ONLY assert facts supported by the "
    "provided SHAP features, user history, and retrieved cases. Cite retrieved case "
    "ids in square brackets like [CASE_017]. If evidence is insufficient, say so "
    "explicitly. Keep the summary under 120 words. Do not invent data."
)

CASE_SUMMARY_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("system", SYSTEM_RULES),
        (
            "human",
            "Flagged transaction:\n{txn}\n\n"
            "Top SHAP feature attributions:\n{shap}\n\n"
            "Recent user history:\n{history}\n\n"
            "Write a concise, plain-English rationale for why this was flagged.",
        ),
    ]
)

RAG_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("system", SYSTEM_RULES),
        (
            "human",
            "Flagged transaction:\n{txn}\n\n"
            "Top SHAP feature attributions:\n{shap}\n\n"
            "Retrieved similar resolved cases and policy snippets:\n{context}\n\n"
            "Summarise the risk, grounding every claim in the SHAP features or the "
            "retrieved cases, and cite the case ids you rely on.",
        ),
    ]
)

# Plain (non-chat) template used by local HuggingFace fine-tuned pipelines.
CASE_SUMMARY_TEXT = PromptTemplate.from_template(
    "### Fraud analyst summary\n"
    "Transaction: {txn}\n"
    "SHAP: {shap}\n"
    "History: {history}\n"
    "Rationale:"
)
