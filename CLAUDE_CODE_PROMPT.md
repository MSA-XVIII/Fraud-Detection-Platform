# Claude Code Build Prompt — Real-Time Fraud & Anomaly Detection Platform

> **How to use this:** Paste the entire block below into Claude Code at the root of an empty repository.
> It is written as a single, self-contained engineering brief. Claude Code should treat it as the
> source of truth, scaffold the repo, and build iteratively. Tell Claude Code to **ask before making
> irreversible cloud calls** (Neptune/Atlas provisioning) and to **stub anything that needs real
> credentials** so the whole system runs locally first.

---

## 🧠 ROLE & OPERATING RULES

You are a **senior distributed-systems + ML platform engineer**. Build a production-shaped (but
locally-runnable) **real-time fraud & anomaly detection platform**. Follow these rules for the whole session:

1. **Plan first, then build.** Start by printing a short build plan and the full directory tree you intend
   to create. Wait for my confirmation before writing code if the plan changes scope materially; otherwise proceed.
2. **Local-first.** Everything must run on my laptop via `docker compose` with **zero paid cloud
   dependencies by default**. Cloud services (AWS Neptune, MongoDB Atlas Vector Search, real LangSmith)
   must be **feature-flagged** behind env vars with working local fallbacks (see "Fallbacks" below).
3. **Incremental & verifiable.** Build in the phases defined below. After each phase, run it, show me the
   command to run it, and confirm it works before moving on. Never leave the repo in a broken state.
4. **Idempotent & typed.** Python 3.11+, full type hints, `pydantic` models for all event/DB schemas,
   `ruff` + `black` + `mypy` clean. Every external write must be idempotent.
5. **No secrets in code.** Use a single `.env` (provide `.env.example`). Read config through one typed
   `settings.py` (`pydantic-settings`).
6. **Test as you go.** Add `pytest` unit tests for feature engineering, scoring, schema validation, and the
   LangChain chain (mocked LLM). Provide a `make test` target.
7. **Document.** Maintain a top-level `README.md` with architecture diagram (ASCII or mermaid), quickstart,
   and a `docs/` folder. Add docstrings and a `Makefile` with all common tasks.
8. **Explain your choices** briefly in commit-message-style summaries after each phase.

---

## 🎯 WHAT WE'RE BUILDING (context)

A streaming system that ingests transaction events, scores each one for fraud risk in near-real-time,
persists decisions to a fast serving store, shows them on a live dashboard with a human-review workflow,
and continuously improves from analyst feedback. It has **three planes**:

- **Hot path** (< 1–2 s): Kafka → stream processor → ML scorer → serving DB. Deterministic, no LLM calls.
- **Analyst-assist layer** (on-demand, OFF the hot path): a LangChain "AI fraud analyst" that explains a
  flagged case using SHAP attributions + retrieval over past cases, traced/evaluated by LangSmith.
- **Cold path** (batch): durable history in a lakehouse layer, feature store, drift detection, and model
  retraining that promotes new models without redeploying the stream.

**Golden rule:** the LLM must **never** sit inline in the per-transaction scoring path. It is only invoked
when a human clicks "Explain this case."

---

## 🧱 TARGET TECH STACK

| Concern | Default (local) | Feature-flagged upgrade |
|---|---|---|
| Streaming bus | **Apache Kafka** (via `redpanda` or `confluentinc` image in docker-compose) | Confluent Cloud |
| Stream processing | **PySpark Structured Streaming** (local Spark). Provide a lightweight `faust`/plain-consumer fallback if Spark is heavy. | Databricks Structured Streaming |
| ML lifecycle | **MLflow** (local tracking server + registry, file/sqlite backend) | Databricks MLflow |
| Models | **IsolationForest** (bootstrap, scikit-learn) → **XGBoost/LightGBM** (supervised) | — |
| Explainability | **SHAP** | — |
| Graph | **Neo4j** (docker) with `neo4j` Python driver + community detection (GDS or networkx fallback) | AWS Neptune (Gremlin/openCypher) |
| Serving store | **MongoDB** (docker) | MongoDB Atlas + Atlas Vector Search |
| Vector search | **Local**: store embeddings in Mongo + brute-force/`faiss` cosine search | Atlas Vector Search |
| LLM orchestration | **LangChain** | — |
| LLM provider | **Configurable**: OpenAI/Anthropic if key present, else a local `FakeListLLM`/Ollama fallback | Hosted models |
| LLM observability | **LangSmith** if `LANGCHAIN_API_KEY` present, else no-op tracer wrapper | LangSmith Cloud |
| Lakehouse / history | **Delta Lake** via `delta-spark` locally (or Parquet medallion folders if Delta is heavy) | Databricks Delta |
| Orchestration | **Prefect** or a simple `APScheduler`/cron script for retraining (keep it light) | Airflow / Databricks Workflows |
| Data quality | **Great Expectations** (light suite on silver layer) | — |
| Dashboard | **Streamlit** | — |
| Embeddings | `sentence-transformers` (local, e.g. `all-MiniLM-L6-v2`) | Hosted embeddings |

If any component is too heavy to run reliably in `docker compose` on a laptop, **choose the lighter fallback
by default** and leave the heavier one behind a flag. State clearly what you chose and why.

---

## 📦 REPOSITORY LAYOUT (create this)

```
fraud-platform/
├── docker-compose.yml            # kafka/redpanda, mongo, neo4j, mlflow, (spark optional)
├── .env.example
├── Makefile                      # up, down, seed, stream, dashboard, train, test, lint
├── README.md
├── pyproject.toml                # deps, ruff/black/mypy config
├── docs/
│   ├── architecture.md           # diagram + plane explanation
│   └── runbook.md                # how to demo, inject attacks, recover
├── config/
│   └── settings.py               # pydantic-settings, one source of config
├── src/fraud/
│   ├── schemas.py                # pydantic: TransactionEvent, ScoredTransaction, Alert, CaseSummary
│   ├── producer/                 # synthetic transaction + attack generator -> Kafka
│   │   └── generator.py
│   ├── stream/                   # structured streaming job
│   │   ├── features.py           # rolling-window feature engineering (pure, unit-tested)
│   │   ├── graph_features.py     # neo4j ring lookups + graph features
│   │   ├── scorer.py             # loads MLflow model, scores, assigns risk band
│   │   └── job.py                # kafka -> features -> score -> mongo + delta
│   ├── ml/
│   │   ├── train.py              # trains IsolationForest/XGBoost, logs to MLflow, registers
│   │   ├── drift.py              # PSI / score-distribution drift; triggers retrain
│   │   └── explain.py            # SHAP top-features for a scored txn
│   ├── graph/
│   │   └── loader.py             # ETL flagged entities into Neo4j; community detection
│   ├── serving/
│   │   └── mongo.py              # idempotent upserts, indexes, vector search wrapper
│   ├── assist/                   # LLM analyst layer (LangChain) — OFF hot path
│   │   ├── retriever.py          # vector search over case_embeddings + policy docs
│   │   ├── chains.py             # case-summary chain + RAG chain
│   │   ├── agent.py              # investigation agent w/ tools: query_graph, get_history, similar_cases
│   │   ├── prompts.py            # grounded, cited prompt templates
│   │   └── tracing.py            # LangSmith wrapper w/ no-op fallback + feedback capture
│   └── dashboard/
│       └── app.py                # Streamlit: live feed, geo-map, review panel, "Explain", attack injector, cost/SLA panel
├── data/                         # local delta/parquet medallion + seed datasets
├── great_expectations/           # silver-layer suite
└── tests/
    ├── test_features.py
    ├── test_scorer.py
    ├── test_schemas.py
    └── test_chains.py            # mocked LLM
```

---

## 📐 KEY DATA CONTRACTS (implement as pydantic models)

**Kafka `raw-transactions` event:**
```json
{
  "txn_id": "uuid",
  "user_id": "str",
  "amount": 1299.00,
  "currency": "INR",
  "merchant": "str",
  "merchant_category": "str",
  "geo": {"lat": 19.07, "lon": 72.87, "country": "IN"},
  "device_id": "str",
  "ip": "str",
  "timestamp": "ISO-8601 UTC"
}
```

**MongoDB `flagged_transactions` document (idempotent, `_id == txn_id`):**
```json
{
  "_id": "txn_id",
  "user_id": "str",
  "amount": 1299.00,
  "risk_score": 0.87,
  "risk_band": "high|medium|low",
  "features": {"amt_z": 8.1, "txn_1m": 4, "new_geo": true, "...": "..."},
  "graph": {"ring_id": "R_22", "shared_device_flags": 3},
  "shap_top": [["amt_z", 0.41], ["txn_1m", 0.28]],
  "status": "open|in_review|closed",
  "label": null,
  "created_at": "ISO-8601"
}
```

Also define: `user_rolling_state` (`_id == user_id`), `alerts` (status/priority + `expected_loss = score*amount`),
and `case_embeddings` (vector + resolved-case metadata).

---

## 🔬 FEATURE ENGINEERING (make `features.py` pure & unit-tested)

Per user, over watermarked windows (1 min / 5 min / 1 hr):
- transaction count per window (velocity)
- rolling mean & std of amount → `amt_z` (z-score vs user baseline)
- distinct merchants & distinct geos in window
- time since last transaction
- `new_geo` / `new_device` boolean flags
- graph features (from Neo4j): `ring_id`, `shared_device_flags`, `component_size`

Keep the pure feature math independent of Spark so it is unit-testable with plain dicts/DataFrames.

---

## 🤖 ML LAYER

- **Bootstrap:** `IsolationForest` (unsupervised) so the system works before labels exist.
- **Mature:** `XGBoost`/`LightGBM` once analyst labels accumulate; optimise for **recall on the fraud class**;
  report precision/recall/PR-AUC.
- **Risk banding:** map score → `low`/`medium`/`high` (configurable thresholds); `high` auto-flags,
  `medium` queues for review.
- **MLflow:** log params/metrics/artifacts, register model, load the `Production`-aliased model in the stream.
  Promotion must NOT require restarting the stream (reload on a schedule or on a registry-change signal).
- **SHAP:** compute top-N feature attributions for each flagged txn; store `shap_top` in Mongo.
- **Drift:** implement PSI on key features + score-distribution drift; when a threshold is exceeded, **trigger
  a retraining run** and log a challenger; only promote if the challenger beats production on held-out metrics.

---

## 🕸️ GRAPH LAYER

- ETL flagged/related entities into Neo4j: `(:User)-[:USED]->(:Device)`, `-[:PAID]->(:Merchant)`,
  `-[:FROM_IP]->(:IP)`, `-[:BILLED_TO]->(:Address)`.
- Run **connected-components / community detection** (Neo4j GDS if available, else `networkx` fallback) to
  assign `ring_id` and compute `component_size` and `shared_device_flags`.
- Expose `query_graph(user_id)` used both by the stream (features) and the LangChain agent (investigation).

---

## 🦜 LLM ANALYST-ASSIST LAYER (LangChain) — OFF THE HOT PATH

Implement exactly three capabilities, all invoked on demand (never in scoring):

1. **Case-summary chain** — input: the flagged txn + `shap_top` + recent user history → output: a concise,
   plain-English rationale ("amount 8× the user's average; new geo; 4 txns in 60 s").
2. **RAG chain** — retrieve similar resolved cases + relevant policy snippets via vector search over
   `case_embeddings`, and ground the summary in them **with citations to the retrieved case ids**.
3. **Investigation agent** — a LangChain agent with tools:
   - `query_graph(user_id)` → ring / shared-entity info from Neo4j
   - `get_user_history(user_id)` → recent txns from Mongo
   - `similar_cases(txn)` → vector search
   The analyst can ask "is this part of a ring?" and the agent orchestrates the lookups.

**Prompting rules:** prompts must force grounding — the model may only assert what is supported by the
provided SHAP features / retrieved cases, and must cite retrieved case ids. If evidence is insufficient it
must say so. Keep summaries under ~120 words.

**LLM provider abstraction:** if `OPENAI_API_KEY`/`ANTHROPIC_API_KEY` present use the real model; otherwise
fall back to a deterministic `FakeListLLM` (or Ollama if available) so tests and demos run with no keys.

---

## 🔭 LLMOps — LangSmith

- Wrap all chains/agent runs so that when `LANGCHAIN_API_KEY` is set, runs are **traced** to LangSmith
  (prompt, retrieved context, tool calls, latency, token cost). When absent, use a **no-op tracer** so nothing
  breaks locally.
- Add a tiny **evaluation harness**: a handful of labelled "good summary" references + LangSmith evaluators
  (faithfulness / hallucination / relevance). Provide `make eval-llm`.
- Capture analyst **thumbs up/down** on each summary from the dashboard and persist it (feeds an eval/finetune set).

---

## 🧊 DATA ENGINEERING BACKBONE (keep light)

- **Medallion**: bronze (raw Kafka dump) → silver (cleaned/enriched) → gold (training features), as Delta
  (or Parquet folders if Delta is heavy locally). Delta/lake is the **source of truth for training**; Mongo is
  serving only.
- **Great Expectations** suite on the silver layer (null checks, range checks, allowed-values) that fails the
  batch loudly.
- **Retraining orchestration**: a Prefect flow (or scheduled script) that runs silver→gold→train→evaluate→
  (conditionally) promote. Wire the drift trigger to it.

---

## 🖥️ STREAMLIT DASHBOARD (make the demo gripping)

- **Live alerts feed** (auto-refresh) querying `flagged_transactions`, sorted by `expected_loss`.
- **Geo-map** of flagged activity + **score-distribution** histogram.
- **Review panel**: open a case → see SHAP top features + graph/ring info → mark fraud / not-fraud (writes
  `label` back to Mongo) → **"Explain this case"** button that calls the LangChain chain and shows the grounded,
  cited summary + a 👍/👎 control (feeds LangSmith).
- **Attack injector**: a button that tells the producer to emit a burst (e.g. "card-testing", "geo-impossible
  travel") so I can trigger detections live in a demo.
- **Ops panel**: current model version (from MLflow), flag rate, alerts/hour, and **p50/p95/p99 end-to-end
  scoring latency**, plus a simple **cost/throughput** readout (events processed vs. a mock/real cost figure).

---

## 🔐 SECURITY & RELIABILITY (implement the essentials)

- **PII handling**: tokenize/mask sensitive fields (e.g. a fake PAN) before storage; document where real
  field-level/queryable encryption would go.
- **Ingress validation**: reject malformed events at the consumer; route poison messages to a Kafka
  **dead-letter topic**.
- **Idempotency**: all Mongo writes upsert on `txn_id`/`user_id`; safe to replay Kafka.
- **Fault tolerance**: checkpointing on the stream; document how to kill a consumer and recover with no loss
  (add a `make chaos` that demonstrates it).

---

## ✅ BUILD PHASES (do them in order; verify each)

**Phase 0 — Scaffold.** Repo layout, `pyproject.toml`, `settings.py`, `.env.example`, `docker-compose.yml`
(kafka/redpanda + mongo + neo4j + mlflow), `Makefile`, empty modules with typed signatures, CI-style
`make lint test`. Verify `docker compose up` brings services healthy.

**Phase 1 — Hot path plumbing.** Synthetic producer → Kafka → consumer/stream that writes raw to
Delta/Parquet **and** upserts a dummy-scored doc to Mongo. Basic Streamlit feed reads from Mongo. Verify
end-to-end with a "dummy always-low-risk" scorer.

**Phase 2 — Real ML + features + SHAP.** Implement pure feature engineering (+ tests), train IsolationForest,
log/register in MLflow, load it in the stream, compute risk bands + SHAP, show SHAP in the review panel.

**Phase 3 — Graph layer.** Neo4j ETL + community detection, `ring_id`/graph features fed into scoring and
shown in the review panel.

**Phase 4 — LLM analyst (LangChain).** Retriever + case-summary + RAG chain + agent; wire the "Explain this
case" button; provider abstraction with fake-LLM fallback; tests with mocked LLM.

**Phase 5 — LLMOps + adaptivity.** LangSmith tracing/eval/feedback; drift detection → retraining flow →
challenger promotion (no stream redeploy); cost/SLA panel; attack injector; `make chaos` recovery demo.

**Phase 6 — Polish.** README with architecture diagram + quickstart + demo script (`docs/runbook.md`),
success-metric readouts, final lint/type/test pass.

---

## 📊 SUCCESS CRITERIA (the repo is "done" when)

- `make up && make seed && make stream && make dashboard` produces a live fraud dashboard on localhost with
  flagged transactions appearing in real time.
- Clicking a flagged case shows SHAP features, graph/ring info, and a **grounded, cited** LLM explanation.
- Triggering the attack injector visibly produces new high-risk alerts within seconds.
- `make train` retrains and registers a model; the running stream picks up the new `Production` version
  **without a restart**.
- `make test` and `make lint` pass; drift trigger and `make chaos` recovery are demonstrable.
- Everything runs with **no cloud keys**; adding keys transparently upgrades LLM/tracing/graph/vector paths.

---

### Start now
Print (1) your chosen defaults vs. fallbacks with one-line justifications, (2) the final directory tree, and
(3) the Phase 0 file list — then begin Phase 0. Pause for my confirmation only if you deviate from this brief.
