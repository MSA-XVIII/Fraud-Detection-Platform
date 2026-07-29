"""Render INTERVIEW_PREP.md into a styled PDF and merge it behind the design spec.

Produces `Fraud_Detection_Complete_Guide.pdf`:

    [ Fraud_Detection_Platform_Design.pdf   (18 pages, unchanged) ]
    [ Part II divider page                                        ]
    [ INTERVIEW_PREP.md rendered to match the design's styling     ]

Styling deliberately mirrors scripts/update_design_pdf.py (A4, same palette,
same footer treatment) so the merged document reads as one artefact.

Run:  python scripts/build_interview_pdf.py
"""

from __future__ import annotations

import io
import os
import re
import unicodedata
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    CondPageBreak,
    Flowable,
    Frame,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
DESIGN_PDF = ROOT / "Fraud_Detection_Platform_Design.pdf"
PREP_MD = ROOT / "INTERVIEW_PREP.md"
OUT_PDF = ROOT / "Fraud_Detection_Complete_Guide.pdf"

# --- palette lifted from the design deck ----------------------------------
CORAL = HexColor("#E4635A")
PURPLE = HexColor("#6C5CA6")
DARK = HexColor("#1F2430")
GREY = HexColor("#5A6172")
LIGHT = HexColor("#EEF0F5")
GREEN = HexColor("#2E9E7B")
CODE_BG = HexColor("#F7F8FB")
CODE_FG = HexColor("#243044")
QUOTE_BG = HexColor("#F3F1FA")

PAGE_W, PAGE_H = A4
MARGIN = 18 * mm
TOP_MARGIN = 20 * mm
BOTTOM_MARGIN = 24 * mm
CONTENT_W = PAGE_W - 2 * MARGIN

FOOT_L1 = "Real-Time Fraud & Anomaly Detection Platform \u00b7 Interview Preparation Guide"
FOOT_L2 = "Kafka \u00b7 MongoDB \u00b7 Neo4j \u00b7 MLflow \u00b7 SHAP \u00b7 LangChain \u00b7 LangSmith \u00b7 LoRA"


# ==========================================================================
# fonts
# ==========================================================================

def register_fonts() -> tuple[str, str, str, str]:
    """Register Unicode-capable TTFs; fall back to the built-in Type 1 fonts.

    Set FRAUD_PDF_LIGHT=1 to skip TTF embedding entirely (~100 KB smaller file, but
    arrows/currency get transliterated and box-drawing characters are dropped).
    """
    if os.environ.get("FRAUD_PDF_LIGHT") == "1":
        return "Helvetica", "Helvetica-Bold", "Helvetica-Oblique", "Courier"
    win = Path("C:/Windows/Fonts")
    try:
        pdfmetrics.registerFont(TTFont("Body", win / "arial.ttf"))
        pdfmetrics.registerFont(TTFont("Body-Bold", win / "arialbd.ttf"))
        pdfmetrics.registerFont(TTFont("Body-Italic", win / "ariali.ttf"))
        pdfmetrics.registerFont(TTFont("Mono", win / "consola.ttf"))
        pdfmetrics.registerFont(TTFont("Mono-Bold", win / "consolab.ttf"))
        pdfmetrics.registerFontFamily(
            "Body", normal="Body", bold="Body-Bold", italic="Body-Italic"
        )
        return "Body", "Body-Bold", "Body-Italic", "Mono"
    except Exception:
        return "Helvetica", "Helvetica-Bold", "Helvetica-Oblique", "Courier"


F, FB, FI, FM = register_fonts()


# ==========================================================================
# text sanitising
# ==========================================================================

# Emoji / pictographs cannot render in Arial or the Type 1 fonts: drop them.
# NB: the Arrows block (U+2190-21FF) is deliberately NOT here - Arial has those
# glyphs, and _SUBS transliterates them when we fall back to Type 1 fonts.
_EMOJI = re.compile(
    "[\U0001F000-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF"
    "\U0000FE0F\U000020E3\U00002B00-\U00002BFF]+"
)
_SUBS = {
    "\U0001F44D": "thumbs-up",
    "\U0001F44E": "thumbs-down",
    "\u2192": "\u2192" if F == "Body" else "->",
    "\u2265": "\u2265" if F == "Body" else ">=",
    "\u2264": "\u2264" if F == "Body" else "<=",
    "\u226a": "\u226a" if F == "Body" else "<<",
    "\u00d7": "\u00d7",
    "\u20b9": "\u20b9" if F == "Body" else "Rs.",
    "\u2248": "\u2248" if F == "Body" else "~",
    "\u00b2": "\u00b2",
}


def de_emoji(text: str) -> str:
    """Substitute the emoji we care about, drop the rest. Safe for code blocks."""
    for src, dst in _SUBS.items():
        text = text.replace(src, dst)
    return _EMOJI.sub("", text)


def sanitize(text: str) -> str:
    text = de_emoji(text)
    # strip any leftover unrenderable symbol/other codepoints
    text = "".join(ch for ch in text if unicodedata.category(ch) not in ("So", "Cs", "Co"))
    return re.sub(r"  +", " ", text).strip()


def esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def inline(text: str) -> str:
    """Convert inline markdown to reportlab intra-paragraph markup."""
    text = sanitize(text)

    # pull out `code` spans first so their contents are never re-parsed
    spans: list[str] = []

    def _stash(m: re.Match[str]) -> str:
        spans.append(
            f'<font face="{FM}" size="8.4" color="#243044">{esc(m.group(1))}</font>'
        )
        return f"\x00{len(spans) - 1}\x00"

    text = re.sub(r"`([^`]+)`", _stash, text)
    text = esc(text)

    # [label](target) -> label (targets are repo-relative and meaningless in print)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"<b><i>\1</i></b>", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\w*])\*([^*\n]+?)\*(?![\w*])", r"<i>\1</i>", text)

    for i, span in enumerate(spans):
        text = text.replace(f"\x00{i}\x00", span)
    return text


# ==========================================================================
# styles
# ==========================================================================

def _ps(name: str, **kw) -> ParagraphStyle:
    base = dict(
        name=name, fontName=F, fontSize=9.2, leading=13.2, textColor=DARK,
        alignment=TA_LEFT, spaceBefore=0, spaceAfter=0,
    )
    base.update(kw)
    return ParagraphStyle(**base)


S = {
    "h1": _ps("h1", fontName=FB, fontSize=17, leading=21, textColor=DARK, spaceBefore=2, spaceAfter=7),
    "h2": _ps("h2", fontName=FB, fontSize=12.6, leading=16, textColor=PURPLE, spaceBefore=11, spaceAfter=5),
    "h3": _ps("h3", fontName=FB, fontSize=10.6, leading=14, textColor=CORAL, spaceBefore=9, spaceAfter=4),
    "h4": _ps("h4", fontName=FB, fontSize=9.6, leading=13, textColor=DARK, spaceBefore=7, spaceAfter=3),
    "body": _ps("body", spaceAfter=5),
    "bullet": _ps("bullet", leftIndent=11, bulletIndent=2, spaceAfter=3.4),
    "bullet2": _ps("bullet2", leftIndent=23, bulletIndent=14, spaceAfter=3, fontSize=8.9, leading=12.6),
    "quote": _ps("quote", fontSize=8.9, leading=12.8, textColor=GREY, leftIndent=8, rightIndent=6,
                 spaceBefore=3, spaceAfter=3),
    "th": _ps("th", fontName=FB, fontSize=8.1, leading=10.6, textColor=colors.white),
    "td": _ps("td", fontSize=8.0, leading=10.6, textColor=DARK),
    "td1": _ps("td1", fontName=FB, fontSize=8.0, leading=10.6, textColor=CORAL),
    "code": _ps("code", fontName=FM, fontSize=7.5, leading=9.5, textColor=CODE_FG),
    "cover_t": _ps("cover_t", fontName=FB, fontSize=30, leading=34, textColor=DARK),
    "cover_s": _ps("cover_s", fontSize=11.5, leading=17, textColor=GREY),
    "kicker": _ps("kicker", fontName=FB, fontSize=9, leading=12, textColor=CORAL),
}


# ==========================================================================
# custom flowables
# ==========================================================================

class HRule(Flowable):
    """Coral section rule."""

    def __init__(self, width: float, thickness: float = 1.6, color=CORAL, pad: float = 4):
        super().__init__()
        self.width, self.thickness, self.color, self.pad = width, thickness, color, pad
        self.height = thickness + pad * 2

    def draw(self) -> None:
        self.canv.setStrokeColor(self.color)
        self.canv.setLineWidth(self.thickness)
        self.canv.line(0, self.pad, self.width, self.pad)


class CodeBlock(Flowable):
    """A shaded, left-accented code box with hard-wrapped lines."""

    PAD_X = 6.5
    PAD_Y = 5.5
    FS = 7.5
    LEAD = 9.5

    def __init__(self, code: str, width: float, _lines: list[str] | None = None):
        super().__init__()
        self.width = width
        self.lines = _lines if _lines is not None else self._wrap(code, width - 2 * self.PAD_X - 3)
        self.height = len(self.lines) * self.LEAD + 2 * self.PAD_Y

    def split(self, availWidth: float, availHeight: float) -> list[Flowable]:
        """Split line-wise so a long listing flows across pages."""
        if availHeight >= self.height:
            return [self]
        n = int((availHeight - 2 * self.PAD_Y) // self.LEAD)
        if n < 4:                      # not worth a stub: move the whole block on
            return []
        return [
            CodeBlock("", self.width, _lines=self.lines[:n]),
            CodeBlock("", self.width, _lines=self.lines[n:]),
        ]

    def _wrap(self, code: str, avail: float) -> list[str]:
        cw = pdfmetrics.stringWidth("M", FM, self.FS)
        limit = max(int(avail / cw), 20)
        out: list[str] = []
        for raw in code.split("\n"):
            line = de_emoji(raw.replace("\t", "    ")).rstrip()
            if not line:
                out.append("")
                continue
            while len(line) > limit:
                cut = line.rfind(" ", 0, limit)
                if cut < limit * 0.55:
                    cut = limit
                out.append(line[:cut])
                indent = len(line) - len(line.lstrip())
                line = " " * (indent + 4) + line[cut:].lstrip()
            out.append(line)
        return out

    def draw(self) -> None:
        c = self.canv
        c.setFillColor(CODE_BG)
        c.rect(0, 0, self.width, self.height, stroke=0, fill=1)
        c.setFillColor(PURPLE)
        c.rect(0, 0, 2.2, self.height, stroke=0, fill=1)
        c.setFont(FM, self.FS)
        c.setFillColor(CODE_FG)
        y = self.height - self.PAD_Y - self.FS + 1
        for line in self.lines:
            if line:
                c.drawString(self.PAD_X + 3, y, line)
            y -= self.LEAD


class Callout(Flowable):
    """Blockquote rendered as a tinted box with a green accent bar."""

    PAD = 6.0

    def __init__(self, paras: list[Paragraph], width: float):
        super().__init__()
        self.width = width
        self.paras = paras
        self.height = 0.0

    def wrap(self, aw: float, ah: float) -> tuple[float, float]:
        inner = self.width - 2 * self.PAD - 4
        total = 0.0
        self._sizes = []
        for p in self.paras:
            _, h = p.wrap(inner, ah)
            self._sizes.append(h)
            total += h + 3
        self.height = total - 3 + 2 * self.PAD
        return self.width, self.height

    def split(self, availWidth: float, availHeight: float) -> list[Flowable]:
        """Split at paragraph boundaries; give up (unbox) if a single para is too tall."""
        if availHeight >= self.height:
            return [self]
        if len(self.paras) < 2:
            return self.paras                      # degrade to plain paragraphs
        run = 2 * self.PAD
        for i, h in enumerate(self._sizes):
            if run + h > availHeight and i > 0:
                return [
                    Callout(self.paras[:i], self.width),
                    Callout(self.paras[i:], self.width),
                ]
            run += h + 3
        return []

    def draw(self) -> None:
        c = self.canv
        c.setFillColor(QUOTE_BG)
        c.rect(0, 0, self.width, self.height, stroke=0, fill=1)
        c.setFillColor(GREEN)
        c.rect(0, 0, 2.4, self.height, stroke=0, fill=1)
        y = self.height - self.PAD
        for p, h in zip(self.paras, self._sizes, strict=False):
            p.drawOn(c, self.PAD + 4, y - h)
            y -= h + 3


# ==========================================================================
# markdown parsing
# ==========================================================================

TOKEN_RE_TABLE_SEP = re.compile(r"^\s*\|?[\s:|-]+\|[\s:|-]*$")


def split_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    # respect escaped pipes and pipes inside `code`
    cells, buf, in_code = [], "", False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "`":
            in_code = not in_code
            buf += ch
        elif ch == "\\" and i + 1 < len(line) and line[i + 1] == "|":
            buf += "|"
            i += 1
        elif ch == "|" and not in_code:
            cells.append(buf.strip())
            buf = ""
        else:
            buf += ch
        i += 1
    cells.append(buf.strip())
    return cells


def col_widths(rows: list[list[str]], total: float) -> list[float]:
    n = max(len(r) for r in rows)
    norm = [r + [""] * (n - len(r)) for r in rows]
    # weight by mean cell length, with a floor so narrow columns stay readable
    weights = []
    for i in range(n):
        lens = [len(re.sub(r"[`*]", "", r[i])) for r in norm]
        weights.append(max(sum(lens) / len(lens), 5) ** 0.82)
    s = sum(weights)
    raw = [total * w / s for w in weights]
    floor = min(16 * mm, total / (n + 1))
    out = [max(w, floor) for w in raw]
    scale = total / sum(out)
    return [w * scale for w in out]


def build_table(rows: list[list[str]], width: float) -> Flowable:
    n = max(len(r) for r in rows)
    norm = [r + [""] * (n - len(r)) for r in rows]
    widths = col_widths(norm, width)

    data = []
    for ri, row in enumerate(norm):
        cells = []
        for ci, cell in enumerate(row):
            if ri == 0:
                cells.append(Paragraph(inline(cell), S["th"]))
            else:
                cells.append(Paragraph(inline(cell), S["td1"] if ci == 0 and n > 2 else S["td"]))
        data.append(cells)

    t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT", splitInRow=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), PURPLE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 3.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3.4),
        ("LEFTPADDING", (0, 0), (-1, -1), 4.5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4.5),
        ("LINEBELOW", (0, 1), (-1, -2), 0.4, LIGHT),
        ("BOX", (0, 0), (-1, -1), 0.5, LIGHT),
    ]
    for ri in range(1, len(data)):
        if ri % 2 == 0:
            style.append(("BACKGROUND", (0, ri), (-1, ri), HexColor("#FAFAFC")))
    t.setStyle(TableStyle(style))
    return t


def md_to_flowables(md: str) -> list[Flowable]:
    lines = md.split("\n")
    out: list[Flowable] = []
    i = 0
    first_h2 = True

    def para(text: str, style: str = "body", bullet: str | None = None) -> None:
        kw = {"bulletText": bullet} if bullet else {}
        out.append(Paragraph(inline(text), S[style], **kw))

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # --- fenced code -------------------------------------------------
        if stripped.startswith("```"):
            i += 1
            buf: list[str] = []
            while i < len(lines) and not lines[i].strip().startswith("```"):
                buf.append(lines[i])
                i += 1
            i += 1
            out.append(Spacer(1, 3))
            out.append(CodeBlock("\n".join(buf), CONTENT_W))
            out.append(Spacer(1, 5))
            continue

        # --- blank -------------------------------------------------------
        if not stripped:
            i += 1
            continue

        # --- horizontal rule --------------------------------------------
        if re.fullmatch(r"-{3,}|\*{3,}|_{3,}", stripped):
            out.append(Spacer(1, 2))
            i += 1
            continue

        # --- headings ----------------------------------------------------
        m = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if m:
            level, text = len(m.group(1)), m.group(2)
            text = re.sub(r"\s*\{#.*?\}\s*$", "", text)
            if level == 1:
                out.append(Paragraph(inline(text), S["h1"]))
                out.append(HRule(CONTENT_W))
            elif level == 2:
                if not first_h2:
                    out.append(PageBreak())
                first_h2 = False
                out.append(Paragraph(inline(text), S["h2"]))
                out.append(HRule(CONTENT_W, thickness=1.0, color=PURPLE, pad=2.5))
            elif level == 3:
                out.append(CondPageBreak(46 * mm))
                out.append(Paragraph(inline(text), S["h3"]))
            else:
                out.append(Paragraph(inline(text), S["h4"]))
            i += 1
            continue

        # --- blockquote / callout ---------------------------------------
        if stripped.startswith(">"):
            buf = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                buf.append(re.sub(r"^\s*>\s?", "", lines[i]))
                i += 1
            chunks, cur = [], []
            for b in buf:
                if b.strip():
                    cur.append(b.strip())
                elif cur:
                    chunks.append(" ".join(cur))
                    cur = []
            if cur:
                chunks.append(" ".join(cur))
            # strip a leading list marker only - lstrip("-* ") would eat leading **bold**
            paras = [
                Paragraph(inline(re.sub(r"^[-*+]\s+", "", c)), S["quote"])
                for c in chunks if c.strip()
            ]
            if paras:
                out.append(Spacer(1, 3))
                out.append(Callout(paras, CONTENT_W))
                out.append(Spacer(1, 5))
            continue

        # --- table -------------------------------------------------------
        if "|" in stripped and i + 1 < len(lines) and TOKEN_RE_TABLE_SEP.match(lines[i + 1]):
            rows = [split_row(lines[i])]
            i += 2
            while i < len(lines) and "|" in lines[i] and lines[i].strip():
                rows.append(split_row(lines[i]))
                i += 1
            out.append(Spacer(1, 3))
            out.append(build_table(rows, CONTENT_W))
            out.append(Spacer(1, 6))
            continue

        # --- lists (one level of nesting) --------------------------------
        m = re.match(r"^(\s*)([-*+]|\d+[.)])\s+(.*)$", line)
        if m:
            indent, marker, text = len(m.group(1)), m.group(2), m.group(3)
            # gather continuation lines
            i += 1
            while i < len(lines):
                nxt = lines[i]
                if not nxt.strip():
                    break
                if re.match(r"^\s*([-*+]|\d+[.)])\s+", nxt) or nxt.strip().startswith(("#", ">", "```", "|")):
                    break
                text += " " + nxt.strip()
                i += 1
            ordered = not marker[0] in "-*+"
            bullet = f"{marker}" if ordered else "\u2022"
            para(text, "bullet2" if indent >= 2 else "bullet", bullet=bullet)
            continue

        # --- paragraph ---------------------------------------------------
        buf = [stripped]
        i += 1
        while i < len(lines) and lines[i].strip() and not re.match(
            r"^\s*(#{1,6}\s|[-*+]\s|\d+[.)]\s|>|```|\|)", lines[i]
        ) and not re.fullmatch(r"-{3,}", lines[i].strip()):
            buf.append(lines[i].strip())
            i += 1
        para(" ".join(buf))

    return out


# ==========================================================================
# page furniture
# ==========================================================================

class PrepDoc(BaseDocTemplate):
    def __init__(self, buf: io.BytesIO):
        super().__init__(
            buf, pagesize=A4,
            pageCompression=1,          # zlib the content streams (reportlab default is off)
            invariant=1,                # no timestamps/ids => identical rebuilds
            leftMargin=MARGIN, rightMargin=MARGIN,
            topMargin=TOP_MARGIN, bottomMargin=BOTTOM_MARGIN,
            title="Real-Time Fraud Detection Platform - Interview Preparation Guide",
            author="Interview Preparation Guide",
            subject="Streaming fraud detection: architecture, code, trade-offs and interview drills",
        )
        frame = Frame(
            MARGIN, BOTTOM_MARGIN, CONTENT_W,
            PAGE_H - TOP_MARGIN - BOTTOM_MARGIN, id="body",
            leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
        )
        self.addPageTemplates([
            PageTemplate(id="body", frames=[frame], onPage=self._furniture),
        ])

    def _band(self, c) -> None:
        c.setFillColor(PURPLE)
        c.rect(0, PAGE_H - 6 * mm, PAGE_W, 6 * mm, stroke=0, fill=1)
        c.setFillColor(CORAL)
        c.rect(0, PAGE_H - 6 * mm, PAGE_W * 0.34, 6 * mm, stroke=0, fill=1)

    def _furniture(self, c, doc) -> None:
        self._band(c)
        if doc.page == 1:            # divider page: band only, no footer
            return
        c.setStrokeColor(LIGHT)
        c.setLineWidth(0.8)
        c.line(MARGIN, 19 * mm, PAGE_W - MARGIN, 19 * mm)
        c.setFont(F, 7.2)
        c.setFillColor(GREY)
        c.drawString(MARGIN, 14.6 * mm, FOOT_L1)
        c.drawString(MARGIN, 11.4 * mm, FOOT_L2)
        c.setFont(FB, 7.6)
        c.setFillColor(CORAL)
        c.drawRightString(PAGE_W - MARGIN, 11.4 * mm, f"Part II \u00b7 Page {doc.page - 1}")


def divider_flowables() -> list[Flowable]:
    """The 'Part II' divider that separates the design spec from the prep guide."""
    out: list[Flowable] = [Spacer(1, 46 * mm)]
    out.append(Paragraph("PART II", S["kicker"]))
    out.append(Spacer(1, 4))
    out.append(Paragraph("Interview<br/>Preparation Guide", S["cover_t"]))
    out.append(Spacer(1, 7))
    out.append(HRule(CONTENT_W, thickness=2.4))
    out.append(Spacer(1, 9))
    out.append(Paragraph(
        "Part I of this document is the system design specification. Part II is the "
        "companion interview preparation guide: the elevator pitches, the use cases and "
        "data sourcing, twenty annotated code exhibits, the design decisions and their "
        "costs, the alternatives that were considered and rejected, the known gaps to "
        "volunteer before an interviewer finds them, and roughly a hundred practice "
        "questions with model answers.",
        S["cover_s"]))
    out.append(Spacer(1, 12))

    contents = [
        ["Section", "What it covers"],
        ["1  Elevator pitches", "Four lengths: one-line, 30s, 90s, 3-minute technical"],
        ["2  Skills & tools inventory", "Vocabulary surface + lift-verbatim resume bullets"],
        ["2B  Use cases & data sourcing", "Who uses it, the attack patterns, and where real data comes from"],
        ["3  Architecture walkthrough", "The data path, narrated file by file"],
        ["3B  Annotated code exhibits", "20 code blocks with 'what to say' for each"],
        ["4  Design decisions", "26 decisions, each with the cost accepted"],
        ["5  Alternatives not used", "~70 alternatives across 15 categories, and why not"],
        ["6  Difficulties", "Solved, latent, and production-grade"],
        ["7  Known gaps", "18 real weaknesses + the honest line to say"],
        ["8  Questions by skill", "~90 Q&As across 12 skill areas"],
        ["9  Scale-up whiteboard", "20/s to 50,000/s, bottleneck first"],
        ["10  Numbers cheat sheet", "Every default, and how to measure real latency"],
        ["11  STAR stories", "Six behavioural answers drawn from the build"],
        ["12  Questions to ask", "Nine questions, and what each one signals"],
        ["13  48-hour prep plan", "Hour-by-hour drill schedule"],
    ]
    out.append(build_table(contents, CONTENT_W))
    out.append(PageBreak())
    return out


# ==========================================================================
# main
# ==========================================================================

def render_prep() -> bytes:
    md = PREP_MD.read_text(encoding="utf-8")

    # Drop the top-level H1 + the leading "how to use" quote is kept; the H1 becomes
    # the divider title instead, so remove it to avoid duplication.
    md = re.sub(r"^#\s+.*?\n", "", md, count=1)

    story: list[Flowable] = divider_flowables()
    story += md_to_flowables(md)

    buf = io.BytesIO()
    PrepDoc(buf).build(story)
    return buf.getvalue()


def main() -> None:
    if not DESIGN_PDF.exists():
        raise SystemExit(f"Design PDF not found: {DESIGN_PDF}")
    if not PREP_MD.exists():
        raise SystemExit(f"Prep markdown not found: {PREP_MD}")

    prep_bytes = render_prep()

    writer = PdfWriter()
    design = PdfReader(str(DESIGN_PDF))
    for page in design.pages:
        writer.add_page(page)
    prep = PdfReader(io.BytesIO(prep_bytes))
    for page in prep.pages:
        writer.add_page(page)

    # --- size reduction: recompress streams + drop duplicate/orphan objects ---
    for page in writer.pages:
        try:
            page.compress_content_streams(level=9)
        except Exception:
            pass
    try:
        writer.compress_identical_objects(remove_duplicates=True, remove_unreferenced=True)
    except TypeError:  # pypdf < 6.x argument names
        writer.compress_identical_objects(remove_identicals=True, remove_orphans=True)

    writer.add_metadata({
        "/Title": "Real-Time Fraud Detection Platform - Design Spec & Interview Guide",
        "/Subject": "System design specification + comprehensive interview preparation",
        "/Keywords": ("Kafka, Redpanda, MongoDB, Neo4j, MLflow, XGBoost, IsolationForest, "
                      "SHAP, LangChain, LangSmith, RAG, LoRA, PEFT, Streamlit, MLOps, LLMOps"),
        "/Creator": "scripts/build_interview_pdf.py",
    })

    with OUT_PDF.open("wb") as fh:
        writer.write(fh)

    print(f"Design spec  : {len(design.pages)} pages")
    print(f"Prep guide   : {len(prep.pages)} pages")
    print(f"Written      : {OUT_PDF.name} ({len(design.pages) + len(prep.pages)} pages)")


if __name__ == "__main__":
    main()
