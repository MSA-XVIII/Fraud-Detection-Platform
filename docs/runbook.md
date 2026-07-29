# Runbook — demo, inject attacks, recover

## 1. One-time setup

```bash
make install
cp .env.example .env
# Paste Neo4j Aura URI + password into .env (required for graph features).
# Optionally paste OPENAI_API_KEY / LANGCHAIN_API_KEY for real LLM + tracing.
make up          # Redpanda + MongoDB + MLflow
make seed        # topics, Mongo indexes, Neo4j rings, case embeddings
make train       # first model registered + promoted to Production in MLflow
```

Health checks:
- Redpanda console: http://localhost:8080
- MLflow: http://localhost:5000
- MongoDB: `mongodb://root:example@localhost:27017`

## 2. Live demo

Open three terminals:

```bash
make producer    # steady stream (~20/s) with ~2% attacks
make stream      # scores events, writes to Mongo, appends bronze
make dashboard   # http://localhost:8501
```

On the dashboard:
1. Watch the **live alerts** feed sort by expected loss.
2. Click a case → inspect **SHAP** top features + **graph/ring** info.
3. **"Explain this case"** → grounded, cited LLM summary. Give it a 👍/👎.
4. **Attack injector** (sidebar) → "card-testing burst" or "geo-impossible
   travel" → new high-risk alerts appear within seconds.
5. **"Is this part of a ring?"** → the investigation agent orchestrates the
   graph + history + retrieval tools.

## 3. Retraining without redeploying the stream

```bash
make train       # trains a challenger; promotes only if it beats production
```

The running `make stream` process reloads the new Production model on its next
reload tick (`MODEL_RELOAD_SECONDS`) — no restart. The dashboard "Model" metric
updates to the new version.

## 4. Drift → auto-retrain

```bash
make drift       # computes PSI vs a drifted distribution; retrains if drifted
```

## 5. Fine-tune the analyst LLM from feedback

```bash
# after collecting 👍 feedback on summaries in the dashboard:
make finetune                       # export -> fine-tune -> prints how to serve
# then set in .env and restart the dashboard:
#   LLM_PROVIDER=finetuned
#   FINETUNED_MODEL_ID=ft:gpt-...    (or the local adapter path)
```

Local fallback (no OpenAI key): `pip install -e ".[finetune]"` first.

## 6. Chaos / recovery drill

```bash
make chaos       # publishes known txns, kills the consumer mid-stream,
                 # restarts it, and verifies every txn persisted exactly once
```

Expected: `CHAOS RESULT: published=N persisted=N (no loss if equal)`.

## 7. Quality gates

```bash
make test        # pytest (features, schemas, scorer, chains, finetune)
make lint        # ruff
make typecheck   # mypy
make eval-llm    # LLM faithfulness / relevance / hallucination scores
```

## Troubleshooting

- **Neo4j "not configured"** in the dashboard → paste `NEO4J_URI` +
  `NEO4J_PASSWORD` in `.env`. Graph features degrade gracefully to zeros without it.
- **No model / heuristic scorer** in logs → run `make train`.
- **LLM summaries look canned** → that's the offline `FakeListLLM`; set
  `OPENAI_API_KEY` + `LLM_PROVIDER=openai` for real output.
- **Kafka connection refused** → `make up` and wait for Redpanda health.
