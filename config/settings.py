"""Single, typed source of configuration for the fraud platform.

Reads from environment / .env via pydantic-settings. Every field has a safe
local default so the system runs with zero cloud credentials; adding secrets
transparently upgrades the corresponding capability.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application settings loaded from environment / .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # [1] Environment
    app_env: str = "local"
    log_level: str = "INFO"

    # [2] Kafka
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_raw_topic: str = "raw-transactions"
    kafka_dlq_topic: str = "raw-transactions-dlq"
    kafka_consumer_group: str = "fraud-stream"
    kafka_security_protocol: str = ""
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""

    # [3] MongoDB
    mongo_uri: str = "mongodb://root:example@localhost:27017"
    mongo_db: str = "fraud"
    mongo_use_atlas_vector: bool = False

    # [4] Neo4j Aura (graph — cloud cluster)
    neo4j_uri: str = ""
    neo4j_username: str = "neo4j"
    neo4j_password: str = ""
    neo4j_database: str = "neo4j"
    neo4j_use_gds: bool = False

    # [5] MLflow
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment: str = "fraud-scoring"
    mlflow_model_name: str = "fraud-scorer"
    mlflow_model_alias: str = "Production"

    # [6] LLM provider
    llm_provider: str = "fake"  # fake | openai | anthropic | finetuned
    llm_model: str = "gpt-4o-mini"
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    # [6b] Fine-tuned LLM (analyst-feedback loop)
    finetune_mode: str = "auto"  # auto | openai | local
    finetune_base_model: str = "distilgpt2"
    finetune_output_dir: Path = Field(default=Path("./models/finetuned"))
    finetune_dataset_path: Path = Field(default=Path("./data/finetune/analyst_summaries.jsonl"))
    finetune_min_examples: int = 10
    finetuned_model_id: str = ""

    # [7] LangSmith
    langchain_tracing_v2: bool = False
    langchain_endpoint: str = "https://api.smith.langchain.com"
    langchain_api_key: str = ""
    langchain_project: str = "fraud-analyst"

    # [8] Embeddings
    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_dim: int = 384

    # [9] Scoring thresholds
    risk_high_threshold: float = 0.80
    risk_medium_threshold: float = 0.50
    model_reload_seconds: int = 60

    # [10] Data / lakehouse
    data_dir: Path = Field(default=Path("./data"))
    bronze_dir: Path = Field(default=Path("./data/bronze"))
    silver_dir: Path = Field(default=Path("./data/silver"))
    gold_dir: Path = Field(default=Path("./data/gold"))

    # ---- derived helpers -------------------------------------------------

    @property
    def neo4j_configured(self) -> bool:
        """True when Neo4j Aura credentials have been supplied."""
        return bool(self.neo4j_uri and self.neo4j_password)

    @property
    def llm_enabled(self) -> bool:
        """True when a real LLM provider key is present."""
        if self.llm_provider == "openai":
            return bool(self.openai_api_key)
        if self.llm_provider == "anthropic":
            return bool(self.anthropic_api_key)
        return False

    @property
    def finetuned_enabled(self) -> bool:
        """True when a fine-tuned model has been configured for serving."""
        return self.llm_provider == "finetuned" and bool(self.finetuned_model_id)

    @property
    def langsmith_enabled(self) -> bool:
        """True when LangSmith tracing should be active."""
        return self.langchain_tracing_v2 and bool(self.langchain_api_key)

    def ensure_dirs(self) -> None:
        """Create local lakehouse directories if missing."""
        for d in (self.data_dir, self.bronze_dir, self.silver_dir, self.gold_dir):
            Path(d).mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings singleton."""
    return Settings()


settings = get_settings()
