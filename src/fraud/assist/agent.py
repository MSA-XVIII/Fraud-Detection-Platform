"""LangChain investigation agent — orchestrates graph + history + retrieval tools.

The analyst asks e.g. "is this part of a ring?" and the agent uses tools:
    query_graph(user_id)     -> Neo4j Aura ring / shared-entity info
    get_user_history(user_id)-> recent txns from Mongo
    similar_cases(text)      -> vector search over resolved cases

With a real tool-calling model (openai / anthropic / openai-ft) it uses a proper
LangChain tool-calling agent. Offline (fake / local models that can't tool-call)
it falls back to a deterministic tool orchestration so demos always work.
"""

from __future__ import annotations

from typing import Any

from langchain_core.tools import tool

from config.settings import settings
from fraud.assist.llm import describe_provider, get_llm
from fraud.assist.retriever import format_context, similar_cases
from fraud.assist.tracing import configure_tracing
from fraud.graph.loader import query_graph
from fraud.logging_config import get_logger
from fraud.serving import mongo

log = get_logger("agent")


@tool
def query_graph_tool(user_id: str) -> str:
    """Return ring / shared-device info for a user from the Neo4j graph."""
    info = query_graph(user_id)
    return (
        f"ring_id={info.ring_id}, component_size={info.component_size}, "
        f"shared_device_flags={info.shared_device_flags}"
    )


@tool
def get_user_history_tool(user_id: str) -> str:
    """Return the user's recent transactions from the serving store."""
    hist = mongo.get_user_history(user_id, limit=5)
    if not hist:
        return "no prior transactions"
    return "; ".join(
        f"{h.get('amount')} at {h.get('merchant')} (score {h.get('risk_score')})" for h in hist
    )


@tool
def similar_cases_tool(query: str) -> str:
    """Return similar resolved fraud cases for the given transaction description."""
    return format_context(similar_cases(query, k=3))


TOOLS = [query_graph_tool, get_user_history_tool, similar_cases_tool]

_TOOL_CALLING_PROVIDERS = {"openai", "anthropic"}


def _supports_tool_calling() -> bool:
    if settings.llm_provider in _TOOL_CALLING_PROVIDERS and settings.llm_enabled:
        return True
    # OpenAI-hosted fine-tuned models support tool calling; local LoRA does not.
    if settings.finetuned_enabled and settings.finetuned_model_id.startswith("ft:"):
        return True
    return False


def investigate(user_id: str, question: str, txn_doc: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the investigation agent for a user + analyst question."""
    configure_tracing()
    if _supports_tool_calling():
        return _run_tool_calling_agent(user_id, question)
    return _run_deterministic(user_id, question, txn_doc)


def _run_tool_calling_agent(user_id: str, question: str) -> dict[str, Any]:
    from langchain.agents import AgentExecutor, create_tool_calling_agent
    from langchain_core.prompts import ChatPromptTemplate

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You are a fraud investigation agent. Use the tools to gather graph, "
                "history and precedent evidence. Ground every claim in tool output and "
                "cite case ids in [brackets]. Be concise.",
            ),
            ("human", "User under review: {user_id}\nQuestion: {question}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )
    agent = create_tool_calling_agent(get_llm(), TOOLS, prompt)
    executor = AgentExecutor(agent=agent, tools=TOOLS, verbose=False, max_iterations=6)
    result = executor.invoke({"user_id": user_id, "question": question})
    return {
        "answer": result.get("output", ""),
        "mode": "tool_calling_agent",
        "model": describe_provider(),
    }


def _run_deterministic(user_id: str, question: str, txn_doc: dict[str, Any] | None) -> dict[str, Any]:
    """Offline-safe orchestration: call every tool, compose a grounded answer."""
    graph = query_graph_tool.invoke({"user_id": user_id})
    history = get_user_history_tool.invoke({"user_id": user_id})
    query = question
    if txn_doc:
        query = f"{txn_doc.get('merchant')} amount {txn_doc.get('amount')} {question}"
    cases = similar_cases_tool.invoke({"query": query})

    info = query_graph(user_id)
    in_ring = info.shared_device_flags > 0 or (info.component_size or 0) > 2
    verdict = (
        f"User {user_id} appears to be part of ring {info.ring_id} "
        f"(component size {info.component_size}, {info.shared_device_flags} shared-device "
        f"links to other flagged users)."
        if in_ring
        else f"No strong ring signal for user {user_id}."
    )
    answer = (
        f"{verdict}\n\nGraph: {graph}\nRecent history: {history}\n"
        f"Precedent cases:\n{cases}"
    )
    return {"answer": answer, "mode": "deterministic", "model": describe_provider()}
