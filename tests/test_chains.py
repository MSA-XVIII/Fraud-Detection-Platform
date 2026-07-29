"""Tests for the LangChain analyst chains with a mocked LLM (no network, no Mongo)."""

import pytest

from fraud.schemas import CaseSummary

TXN = {
    "_id": "t1",
    "user_id": "u1",
    "amount": 12000.0,
    "currency": "INR",
    "merchant": "Steam",
    "risk_score": 0.9,
    "risk_band": "high",
    "graph": {"ring_id": "R_2", "shared_device_flags": 3},
    "shap_top": [["amt_z", 0.41], ["txn_1m", 0.28]],
}


@pytest.fixture
def fake_llm():
    from langchain_community.llms import FakeListLLM

    return FakeListLLM(
        responses=["Amount 8x baseline; new device; matches [CASE_017]. Recommend hold."]
    )


def test_case_summary_is_grounded(monkeypatch, fake_llm):
    import fraud.assist.chains as chains

    monkeypatch.setattr(chains, "get_llm", lambda *a, **k: fake_llm)
    result = chains.case_summary(TXN, history=[{"amount": 500, "merchant": "Amazon", "risk_score": 0.1}])
    assert isinstance(result, CaseSummary)
    assert result.txn_id == "t1"
    assert "CASE_017" in result.cited_case_ids


def test_rag_summary_uses_retrieved_context(monkeypatch, fake_llm):
    import fraud.assist.chains as chains

    monkeypatch.setattr(chains, "get_llm", lambda *a, **k: fake_llm)
    monkeypatch.setattr(
        chains,
        "similar_cases",
        lambda q, k=3: [{"_id": "CASE_017", "text": "card testing ring", "score": 0.9}],
    )
    result = chains.rag_summary(TXN)
    assert result.grounded is True
    assert "CASE_017" in result.cited_case_ids
