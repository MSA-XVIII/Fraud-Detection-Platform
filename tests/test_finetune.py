"""Tests for the fine-tune dataset export from analyst feedback."""

import json

import fraud.assist.finetune as ft


def test_build_dataset_only_uses_thumbs_up(monkeypatch, tmp_path):
    feedback = [
        {"txn_id": "t1", "summary": "good grounded summary [CASE_017]", "score": 1,
         "prompt": {"txn": "Steam 12000", "shap": "amt_z=+0.4", "history": "none"}},
        {"txn_id": "t2", "summary": "bad summary", "score": 0, "prompt": {}},
        {"txn_id": "t3", "summary": "another good one", "score": 1,
         "prompt": {"txn": "Amazon 15", "shap": "txn_1m=+0.5", "history": "none"}},
    ]
    monkeypatch.setattr(ft, "load_feedback", lambda: feedback)
    out = tmp_path / "ds.jsonl"
    monkeypatch.setattr(ft.settings, "finetune_dataset_path", out)

    path = ft.build_dataset()
    lines = [json.loads(x) for x in path.read_text().splitlines()]
    assert len(lines) == 2  # only thumbs-up
    msgs = lines[0]["messages"]
    assert msgs[0]["role"] == "system"
    assert msgs[-1]["role"] == "assistant"
    assert "CASE_017" in msgs[-1]["content"]


def test_run_reports_insufficient_data(monkeypatch, tmp_path):
    monkeypatch.setattr(ft, "load_feedback", lambda: [])
    monkeypatch.setattr(ft.settings, "finetune_dataset_path", tmp_path / "ds.jsonl")
    monkeypatch.setattr(ft.settings, "finetune_min_examples", 10)
    result = ft.run()
    assert result["status"] == "insufficient_data"
