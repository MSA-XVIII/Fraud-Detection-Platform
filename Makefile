# Real-Time Fraud Detection Platform — task runner
# Works on Windows (PowerShell), macOS, Linux. Requires: docker, python 3.11+.

PY ?= python

.PHONY: help install up down logs seed create-topics stream producer dashboard demo \
        train drift graph-load eval-llm finetune test lint fmt typecheck chaos clean

help:
	@echo "Targets:"
	@echo "  install       Install python deps (pip install -e .[dev])"
	@echo "  up            Start local infra (redpanda, mongo, mlflow)"
	@echo "  down          Stop infra"
	@echo "  create-topics Create Kafka topics"
	@echo "  seed          Seed Neo4j graph + Mongo indexes + case embeddings"
	@echo "  producer      Run synthetic transaction producer"
	@echo "  stream        Run the scoring stream (Kafka -> score -> Mongo/Delta)"
	@echo "  dashboard     Launch the Streamlit dashboard"
	@echo "  demo          Launch the standalone demo dashboard (no infra needed)"
	@echo "  train         Train + register a model in MLflow"
	@echo "  graph-load    ETL flagged entities into Neo4j Aura + community detection"
	@echo "  drift         Run drift detection (may trigger retrain)"
	@echo "  eval-llm      Run the LangSmith LLM evaluation harness"
	@echo "  finetune      Export analyst feedback -> fine-tune analyst LLM"
	@echo "  test          Run pytest"
	@echo "  lint fmt typecheck   Static checks"
	@echo "  chaos         Kill+recover the consumer to prove no-loss recovery"

install:
	$(PY) -m pip install -e ".[dev]"

up:
	docker compose up -d
	@echo "Infra up. Redpanda console: http://localhost:8080  MLflow: http://localhost:5000"

down:
	docker compose down

logs:
	docker compose logs -f

create-topics:
	$(PY) -m fraud.producer.create_topics

seed: create-topics
	$(PY) -m fraud.serving.mongo --init
	$(PY) -m fraud.graph.loader --seed
	$(PY) -m fraud.assist.retriever --seed

producer:
	$(PY) -m fraud.producer.generator

stream:
	$(PY) -m fraud.stream.job

dashboard:
	$(PY) -m streamlit run src/fraud/dashboard/app.py

demo:
	$(PY) -m streamlit run src/fraud/dashboard/demo_app.py

train:
	$(PY) -m fraud.ml.train

graph-load:
	$(PY) -m fraud.graph.loader --load

drift:
	$(PY) -m fraud.ml.drift

eval-llm:
	$(PY) -m fraud.assist.eval

finetune:
	$(PY) -m fraud.assist.finetune

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check src config tests

fmt:
	$(PY) -m black src config tests
	$(PY) -m ruff check --fix src config tests

typecheck:
	$(PY) -m mypy src config

chaos:
	$(PY) -m fraud.stream.chaos

clean:
	docker compose down -v
	rm -rf data/bronze data/silver data/gold __pycache__
