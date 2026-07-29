"""Fine-tune the analyst LLM from captured 👍/👎 feedback (keeps LangChain serving).

Pipeline:
    1. EXPORT  analyst thumbs-up, grounded summaries -> chat JSONL dataset.
    2. TRAIN   * OpenAI fine-tuning API  (if OPENAI_API_KEY + mode in {auto, openai})
               * local LoRA/PEFT adapter (fallback; needs the `finetune` extra)
    3. SERVE   set LLM_PROVIDER=finetuned + FINETUNED_MODEL_ID -> assist.llm loads it
               into the SAME LangChain chains/agent (summary / RAG / investigation).

Run:  make finetune   (or python -m fraud.assist.finetune)
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from config.settings import settings
from fraud.assist.prompts import SYSTEM_RULES
from fraud.assist.tracing import load_feedback
from fraud.logging_config import get_logger

log = get_logger("finetune")


# --- 1. dataset export -----------------------------------------------------


def _user_prompt(prompt: dict[str, Any]) -> str:
    return (
        f"Flagged transaction:\n{prompt.get('txn', '')}\n\n"
        f"Top SHAP feature attributions:\n{prompt.get('shap', '')}\n\n"
        f"Recent user history:\n{prompt.get('history', '')}\n\n"
        "Write a concise, grounded rationale."
    )


def build_dataset() -> Path:
    """Export thumbs-up, grounded summaries to a chat-format JSONL file."""
    records = [r for r in load_feedback() if int(r.get("score", 0)) == 1 and r.get("summary")]
    out = Path(settings.finetune_dataset_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for r in records:
            prompt = r.get("prompt") or {}
            example = {
                "messages": [
                    {"role": "system", "content": SYSTEM_RULES},
                    {"role": "user", "content": _user_prompt(prompt)},
                    {"role": "assistant", "content": r["summary"]},
                ]
            }
            fh.write(json.dumps(example) + "\n")
    log.info("dataset_built", examples=len(records), path=str(out))
    return out


# --- 2a. OpenAI fine-tuning ------------------------------------------------


def finetune_openai(dataset_path: Path) -> dict[str, Any]:
    """Submit an OpenAI fine-tuning job and poll to completion."""
    from openai import OpenAI

    client = OpenAI(api_key=settings.openai_api_key)
    up = client.files.create(file=dataset_path.open("rb"), purpose="fine-tune")
    job = client.fine_tuning.jobs.create(training_file=up.id, model="gpt-4o-mini-2024-07-18")
    log.info("openai_ft_submitted", job_id=job.id)

    while True:
        job = client.fine_tuning.jobs.retrieve(job.id)
        log.info("openai_ft_status", status=job.status)
        if job.status in {"succeeded", "failed", "cancelled"}:
            break
        time.sleep(30)

    if job.status != "succeeded":
        return {"status": job.status, "model_id": None}
    log.info("openai_ft_done", model=job.fine_tuned_model)
    print(
        "\nFine-tune complete. To serve it, set in .env:\n"
        f"  LLM_PROVIDER=finetuned\n  FINETUNED_MODEL_ID={job.fine_tuned_model}\n"
    )
    return {"status": "succeeded", "model_id": job.fine_tuned_model}


# --- 2b. local LoRA / PEFT fallback ---------------------------------------


def finetune_local(dataset_path: Path) -> dict[str, Any]:
    """LoRA fine-tune a small local base model (needs the `finetune` extra)."""
    try:
        import torch  # noqa: F401
        from datasets import Dataset
        from peft import LoraConfig, get_peft_model
        from transformers import (
            AutoModelForCausalLM,
            AutoTokenizer,
            DataCollatorForLanguageModeling,
            Trainer,
            TrainingArguments,
        )
    except ImportError as exc:
        raise SystemExit(
            "Local fine-tuning needs extra deps. Install:  pip install -e \".[finetune]\""
        ) from exc

    base = settings.finetune_base_model
    tokenizer = AutoTokenizer.from_pretrained(base)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(base)

    lora = LoraConfig(r=8, lora_alpha=16, lora_dropout=0.05, task_type="CAUSAL_LM")
    model = get_peft_model(model, lora)
    model.print_trainable_parameters()

    rows = []
    with dataset_path.open(encoding="utf-8") as fh:
        for line in fh:
            msgs = json.loads(line)["messages"]
            text = "".join(f"<|{m['role']}|>\n{m['content']}\n" for m in msgs)
            rows.append({"text": text})
    if not rows:
        raise SystemExit("No fine-tune examples. Collect thumbs-up feedback first.")

    ds = Dataset.from_list(rows)

    def _tok(batch: dict[str, list[str]]) -> dict[str, Any]:
        out = tokenizer(batch["text"], truncation=True, max_length=512, padding="max_length")
        return out

    ds = ds.map(_tok, batched=True, remove_columns=["text"])

    out_dir = Path(settings.finetune_output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    args = TrainingArguments(
        output_dir=str(out_dir / "checkpoints"),
        per_device_train_batch_size=2,
        num_train_epochs=3,
        learning_rate=2e-4,
        logging_steps=5,
        save_strategy="no",
        report_to=[],
    )
    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=ds,
        data_collator=DataCollatorForLanguageModeling(tokenizer, mlm=False),
    )
    trainer.train()
    model.save_pretrained(str(out_dir))
    tokenizer.save_pretrained(str(out_dir))
    log.info("local_ft_done", adapter=str(out_dir))
    print(
        "\nLocal fine-tune complete. To serve it, set in .env:\n"
        f"  LLM_PROVIDER=finetuned\n  FINETUNED_MODEL_ID={out_dir}\n"
    )
    return {"status": "succeeded", "model_id": str(out_dir)}


# --- orchestrator ----------------------------------------------------------


def run(mode: str | None = None) -> dict[str, Any]:
    mode = mode or settings.finetune_mode
    dataset = build_dataset()
    n = sum(1 for _ in dataset.open(encoding="utf-8"))
    if n < settings.finetune_min_examples:
        msg = (
            f"Only {n} thumbs-up examples (need >= {settings.finetune_min_examples}). "
            "Collect more analyst feedback from the dashboard, then re-run."
        )
        log.warning("insufficient_examples", have=n, need=settings.finetune_min_examples)
        print(msg)
        return {"status": "insufficient_data", "examples": n}

    use_openai = (mode == "openai") or (mode == "auto" and bool(settings.openai_api_key))
    if use_openai:
        return finetune_openai(dataset)
    return finetune_local(dataset)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["auto", "openai", "local"], default=None)
    ap.add_argument("--export-only", action="store_true", help="build the dataset and exit")
    args = ap.parse_args()
    if args.export_only:
        build_dataset()
    else:
        print(run(args.mode))
