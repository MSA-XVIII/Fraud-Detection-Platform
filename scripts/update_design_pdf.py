"""Generate a styled 'Fine-Tuned Analyst LLM' section and insert it into the
design PDF, right after the LangChain / LangSmith pages. Keeps a backup of the
original. Matches the A4 page size and the document's section styling closely.
"""

from __future__ import annotations

import io
import shutil
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas

SRC = Path("Fraud_Detection_Platform_Design.pdf")
BACKUP = Path("Fraud_Detection_Platform_Design_original.pdf")

CORAL = HexColor("#E4635A")
PURPLE = HexColor("#6C5CA6")
DARK = HexColor("#1F2430")
GREY = HexColor("#5A6172")
LIGHT = HexColor("#EEF0F5")
GREEN = HexColor("#2E9E7B")

PAGE_W, PAGE_H = A4
MARGIN = 22 * mm


def _footer(c: canvas.Canvas, page_label: str) -> None:
    c.setStrokeColor(LIGHT)
    c.setLineWidth(0.8)
    c.line(MARGIN, 22 * mm, PAGE_W - MARGIN, 22 * mm)
    c.setFont("Helvetica", 7.5)
    c.setFillColor(GREY)
    c.drawString(MARGIN, 17 * mm, "Real-Time Fraud & Anomaly Detection Platform · System Design Specification")
    c.drawString(MARGIN, 13.5 * mm, "Kafka · Databricks · MongoDB · Streamlit · LangChain · LangSmith")
    c.setFillColor(CORAL)
    c.drawRightString(PAGE_W - MARGIN, 13.5 * mm, page_label)


def _section_header(c: canvas.Canvas, number: str, title: str, y: float) -> float:
    c.setFillColor(CORAL)
    c.setFont("Helvetica-Bold", 30)
    c.drawString(MARGIN, y, number)
    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 17)
    c.drawString(MARGIN + 34 * mm, y + 4, title)
    c.setStrokeColor(CORAL)
    c.setLineWidth(2)
    c.line(MARGIN, y - 6 * mm, PAGE_W - MARGIN, y - 6 * mm)
    return y - 16 * mm


def _wrap(c: canvas.Canvas, text: str, x: float, y: float, width: float,
          font: str = "Helvetica", size: float = 10.5, leading: float = 15,
          color=DARK) -> float:
    from reportlab.pdfbase.pdfmetrics import stringWidth

    c.setFont(font, size)
    c.setFillColor(color)
    words = text.split()
    line = ""
    for w in words:
        test = f"{line} {w}".strip()
        if stringWidth(test, font, size) > width:
            c.drawString(x, y, line)
            y -= leading
            line = w
        else:
            line = test
    if line:
        c.drawString(x, y, line)
        y -= leading
    return y


def _bullet(c: canvas.Canvas, head: str, body: str, x: float, y: float, width: float) -> float:
    c.setFillColor(PURPLE)
    c.circle(x + 2, y + 3, 2, stroke=0, fill=1)
    c.setFont("Helvetica-Bold", 10.5)
    c.setFillColor(DARK)
    c.drawString(x + 9, y, head)
    from reportlab.pdfbase.pdfmetrics import stringWidth

    head_w = stringWidth(head + "  ", "Helvetica-Bold", 10.5)
    y = _wrap(c, body, x + 9 + head_w, y, width - head_w - 9, color=GREY)
    return y - 4


def build_page() -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    # top accent band
    c.setFillColor(PURPLE)
    c.rect(0, PAGE_H - 8 * mm, PAGE_W, 8 * mm, stroke=0, fill=1)

    y = PAGE_H - 30 * mm
    y = _section_header(c, "07B", "Fine-Tuned Analyst LLM (Feedback Loop)", y)

    y = _wrap(
        c,
        "The analyst-assist layer is not static. Every summary the analyst rates with a "
        "thumbs-up/down is captured, and those judgements are used to fine-tune a dedicated "
        "analyst LLM. Crucially, LangChain is retained as the orchestration layer — the "
        "fine-tuned model is swapped in behind the same summary / RAG / agent chains, so the "
        "rest of the system is unchanged.",
        MARGIN, y, PAGE_W - 2 * MARGIN, leading=15.5,
    )
    y -= 4 * mm

    # The loop
    c.setFillColor(PURPLE)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(MARGIN, y, "The closed-loop pipeline")
    y -= 8 * mm
    steps = [
        ("Capture", "The dashboard persists each thumbs-up / thumbs-down with the prompt context (transaction, SHAP features, retrieved cases) locally and to LangSmith."),
        ("Curate", "Thumbs-up, grounded, cited summaries are exported to a chat-format JSONL dataset once a minimum example threshold is reached."),
        ("Fine-tune", "A managed fine-tune (OpenAI) runs when a key is present; otherwise a local LoRA / PEFT adapter is trained on a small base model — no cloud required."),
        ("Serve", "Setting LLM_PROVIDER=finetuned routes the LangChain chains to the fine-tuned model (hosted ft: id or local adapter) via one provider seam."),
        ("Evaluate", "LangSmith faithfulness / relevance / hallucination evaluators gate the fine-tuned model before it replaces the incumbent analyst LLM."),
    ]
    for head, body in steps:
        y = _bullet(c, head + " — ", body, MARGIN, y, PAGE_W - 2 * MARGIN)
    y -= 3 * mm

    # provider table
    c.setFillColor(PURPLE)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(MARGIN, y, "Provider abstraction (one seam, four modes)")
    y -= 7 * mm

    rows = [
        ("Mode", "Model served", "When it is used"),
        ("fake", "Deterministic FakeListLLM", "No keys — offline demos & unit tests"),
        ("openai / anthropic", "Hosted foundation model", "API key present; base analyst quality"),
        ("finetuned (hosted)", "ft:gpt-… fine-tune", "After a managed fine-tune job succeeds"),
        ("finetuned (local)", "Base + LoRA adapter", "Air-gapped / no-cost fine-tune fallback"),
    ]
    col_x = [MARGIN, MARGIN + 42 * mm, MARGIN + 92 * mm]
    row_h = 8.4 * mm
    # header row
    c.setFillColor(LIGHT)
    c.rect(MARGIN - 2 * mm, y - 2 * mm, PAGE_W - 2 * MARGIN + 4 * mm, row_h, stroke=0, fill=1)
    for i, cell in enumerate(rows[0]):
        c.setFillColor(DARK)
        c.setFont("Helvetica-Bold", 9.5)
        c.drawString(col_x[i], y, cell)
    y -= row_h
    for r in rows[1:]:
        for i, cell in enumerate(r):
            c.setFillColor(CORAL if i == 0 else GREY)
            c.setFont("Helvetica-Bold" if i == 0 else "Helvetica", 9.5)
            c.drawString(col_x[i], y, cell)
        c.setStrokeColor(LIGHT)
        c.setLineWidth(0.6)
        c.line(MARGIN - 2 * mm, y - 2.4 * mm, PAGE_W - MARGIN + 2 * mm, y - 2.4 * mm)
        y -= row_h
    y -= 3 * mm

    # design principle callout
    box_h = 24 * mm
    c.setFillColor(HexColor("#F3F1FA"))
    c.rect(MARGIN - 2 * mm, y - box_h, PAGE_W - 2 * MARGIN + 4 * mm, box_h, stroke=0, fill=1)
    c.setFillColor(GREEN)
    c.rect(MARGIN - 2 * mm, y - box_h, 2.5 * mm, box_h, stroke=0, fill=1)
    c.setFillColor(PURPLE)
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(MARGIN + 4 * mm, y - 6 * mm, "Design principle #4 — keep the LLM swappable and off the hot path")
    _wrap(
        c,
        "LangChain stays the stable orchestration seam; the model behind it evolves from "
        "foundation → fine-tuned as analyst feedback accumulates. The fine-tuned model still "
        "only runs on demand when an analyst clicks “Explain”, never inline in scoring.",
        MARGIN + 4 * mm, y - 11 * mm, PAGE_W - 2 * MARGIN - 6 * mm, size=9.5, leading=13, color=GREY,
    )

    _footer(c, "Page 8b")
    c.showPage()
    c.save()
    return buf.getvalue()


def main() -> None:
    if not BACKUP.exists():
        shutil.copyfile(SRC, BACKUP)
        print(f"Backup written -> {BACKUP}")

    reader = PdfReader(str(BACKUP))
    new_page = PdfReader(io.BytesIO(build_page())).pages[0]

    writer = PdfWriter()
    # Insert after the LLMOps / LangSmith page (section 08 = page 10 = index 9).
    insert_after = 9
    for i, page in enumerate(reader.pages):
        writer.add_page(page)
        if i == insert_after:
            writer.add_page(new_page)

    with SRC.open("wb") as fh:
        writer.write(fh)
    print(f"Updated PDF written -> {SRC} ({len(reader.pages) + 1} pages)")


if __name__ == "__main__":
    main()
