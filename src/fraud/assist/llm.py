"""LLM provider abstraction for the LangChain analyst layer.

One factory returns a LangChain-compatible chat/LLM model for whichever provider
is configured, so every chain/agent is provider-agnostic:

    fake       -> deterministic FakeListLLM (no keys; tests & offline demo)
    openai     -> ChatOpenAI (needs OPENAI_API_KEY)
    anthropic  -> ChatAnthropic (needs ANTHROPIC_API_KEY)
    finetuned  -> the analyst-feedback fine-tuned model:
                    * ft:gpt-... id  -> served via ChatOpenAI
                    * local path     -> served via HuggingFacePipeline (LoRA adapter)

This is the single seam where the fine-tuned model joins the loop while LangChain
remains the orchestration layer for summary / RAG / agent chains.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from config.settings import settings
from fraud.logging_config import get_logger

log = get_logger("llm")

# Canned, grounded responses so FakeListLLM demos look realistic offline.
_FAKE_RESPONSES = [
    (
        "High risk: amount is 8.1x the user's average, from a new geo, with 4 "
        "transactions in 60s (card-testing pattern). Matches confirmed case "
        "CASE_017. Recommend hold + step-up auth."
    ),
    (
        "Medium risk: velocity spike (12 txns/5m) on a new device. Shares an IP "
        "with ring R_2 (component size 5). Precedent: CASE_004. Queue for review."
    ),
]


def _is_finetuned_local() -> bool:
    mid = settings.finetuned_model_id
    return bool(mid) and not mid.startswith("ft:") and Path(mid).exists()


def get_llm(temperature: float = 0.1) -> Any:
    """Return a LangChain LLM/chat model for the configured provider."""
    provider = settings.llm_provider

    if provider == "openai" and settings.openai_api_key:
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.openai_api_key,
            temperature=temperature,
        )

    if provider == "anthropic" and settings.anthropic_api_key:
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(
            model=settings.llm_model,
            api_key=settings.anthropic_api_key,
            temperature=temperature,
        )

    if provider == "finetuned" and settings.finetuned_model_id:
        return _get_finetuned_llm(temperature)

    # default: offline deterministic fallback
    from langchain_community.llms import FakeListLLM

    log.info("llm_provider", provider="fake")
    return FakeListLLM(responses=_FAKE_RESPONSES)


def _get_finetuned_llm(temperature: float) -> Any:
    """Serve the fine-tuned analyst model (hosted OpenAI ft or local LoRA)."""
    mid = settings.finetuned_model_id
    if mid.startswith("ft:") and settings.openai_api_key:
        from langchain_openai import ChatOpenAI

        log.info("llm_provider", provider="finetuned-openai", model=mid)
        return ChatOpenAI(model=mid, api_key=settings.openai_api_key, temperature=temperature)

    if _is_finetuned_local():
        return _load_local_finetuned(mid, temperature)

    log.warning("finetuned_unavailable_falling_back_to_fake", model_id=mid)
    from langchain_community.llms import FakeListLLM

    return FakeListLLM(responses=_FAKE_RESPONSES)


def _load_local_finetuned(adapter_path: str, temperature: float) -> Any:
    """Load a local base model + LoRA adapter and wrap as a LangChain pipeline."""
    from langchain_community.llms import HuggingFacePipeline
    from peft import PeftModel
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        pipeline,
    )

    base = settings.finetune_base_model
    tokenizer = AutoTokenizer.from_pretrained(base)
    model = AutoModelForCausalLM.from_pretrained(base)
    model = PeftModel.from_pretrained(model, adapter_path)
    gen = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=160,
        temperature=max(temperature, 0.01),
        do_sample=temperature > 0,
    )
    log.info("llm_provider", provider="finetuned-local", adapter=adapter_path)
    return HuggingFacePipeline(pipeline=gen)


def describe_provider() -> str:
    """Human-readable provider label for the dashboard ops panel."""
    if settings.llm_provider == "finetuned" and settings.finetuned_model_id:
        kind = "openai-ft" if settings.finetuned_model_id.startswith("ft:") else "local-lora"
        return f"finetuned ({kind})"
    if settings.llm_enabled:
        return f"{settings.llm_provider}:{settings.llm_model}"
    return "fake (offline)"
