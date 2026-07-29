# Architecture

The platform is organised into **three planes**, each with a distinct SLA and
backing store. This separation is the single most important design idea.

## Hot path (< 1–2 s, deterministic, no LLM)

```
Producer -> Kafka(raw-transactions) -> Stream job -> MongoDB (serving)
                                          |            \-> Delta/Parquet (bronze)
                                          |-- rolling features (pure)
                                          |-- graph features  (Neo4j Aura)
                                          \-- ML score        (MLflow Production model)
```

- **Partitioned by `user_id`** so a user's events stay ordered on one partition —
  essential for velocity features.
- **At-least-once** ingest with manual offset commits *after* a successful,
  idempotent Mongo upsert → Kafka can be replayed with no double-effect.
- Poison messages go to a **dead-letter topic** instead of crashing the consumer.
- The **model hot-reloads** from the MLflow registry on a schedule, so a newly
  promoted model is picked up without restarting the stream.

## Analyst-assist layer (on-demand, off the hot path)

- **LangChain** orchestrates three capabilities, all button-triggered:
  1. **Case-summary chain** — rationale grounded in SHAP + user history.
  2. **RAG chain** — retrieves similar resolved cases + policy snippets from the
     vector store and cites case ids.
  3. **Investigation agent** — tools `query_graph`, `get_user_history`,
     `similar_cases` orchestrated to answer "is this part of a ring?".
- **Provider-agnostic** via `assist/llm.py`: `fake` (offline), `openai`,
  `anthropic`, or **`finetuned`** (the analyst-feedback model).
- **LangSmith** traces every run when configured; a no-op wrapper keeps local
  runs working otherwise.

## Cold path (batch)

- **Medallion lakehouse**: bronze (raw) → silver (cleaned) → gold (features).
  The lake is the source of truth for training; Mongo is serving only.
- **Drift detection** (PSI + score distribution) triggers retraining; a
  challenger is only promoted if it beats the incumbent on held-out PR-AUC.
- **Fine-tuning loop**: analyst 👍/👎 feedback → JSONL dataset → OpenAI or local
  LoRA fine-tune → served back through the same LangChain chains.

## Two observability stacks (the MLOps + LLMOps symmetry)

| Concern | Hot-path model | LLM analyst |
|---|---|---|
| Tracking | MLflow (params/metrics/registry) | LangSmith (prompt/context/tools/cost) |
| Quality | PR-AUC, precision/recall, drift | faithfulness / relevance / hallucination |
| Feedback | analyst labels → retrain | analyst 👍/👎 → eval + fine-tune |

## Data contracts

See `src/fraud/schemas.py`. Key documents:

- `flagged_transactions` (`_id == txn_id`) — enriched, scored decision + SHAP + graph.
- `user_rolling_state` (`_id == user_id`) — idempotent per-user aggregates.
- `alerts` — review lifecycle with `expected_loss = score * amount`.
- `case_embeddings` — vectors of resolved cases for RAG.
