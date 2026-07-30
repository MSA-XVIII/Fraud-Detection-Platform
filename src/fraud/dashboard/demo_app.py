"""Standalone demo dashboard — present the fraud pipeline with example cases.

Replays scripted fraud scenarios through the REAL feature engineering
(`fraud.stream.features`) and the REAL heuristic scorer (`fraud.stream.scorer`)
entirely in-process. No Kafka, MongoDB, Neo4j, or MLflow required.

Run:  make demo   (streamlit run src/fraud/dashboard/demo_app.py)
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from config.settings import settings

from fraud.dashboard.demo_cases import SCENARIOS, run_scenario, score_contributions
from fraud.schemas import Features
from fraud.stream.scorer import HeuristicScorer, band_for

st.set_page_config(page_title="Fraud Detection — Demo", layout="wide", page_icon="🛡️")

# --- palette (validated: dataviz reference palette; band colors never used without text) ---
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BLUE = "#2a78d6"
BAND_COLOR = {"low": "#0ca30c", "medium": "#fab219", "high": "#d03b3b"}
BAND_LABEL = {"low": "🟢 low", "medium": "🟡 medium", "high": "🔴 high"}

HIGH_T = settings.risk_high_threshold
MED_T = settings.risk_medium_threshold


def _base_layout(fig: go.Figure, height: int = 320) -> go.Figure:
    fig.update_layout(
        height=height,
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font={"family": 'system-ui, -apple-system, "Segoe UI", sans-serif', "color": INK_2, "size": 13},
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        hoverlabel={"bgcolor": "white", "font_color": INK},
    )
    fig.update_xaxes(gridcolor=GRID, linecolor="#c3c2b7", zeroline=False, tickcolor=MUTED)
    fig.update_yaxes(gridcolor=GRID, linecolor="#c3c2b7", zeroline=False, tickcolor=MUTED)
    return fig


def timeline_chart(df: pd.DataFrame, show_no_graph: bool) -> go.Figure:
    fig = go.Figure()
    fig.add_hline(y=HIGH_T, line_dash="dot", line_color=BAND_COLOR["high"], line_width=1,
                  annotation_text=f"high ≥ {HIGH_T:.2f}", annotation_font_color=INK_2)
    fig.add_hline(y=MED_T, line_dash="dot", line_color="#c98500", line_width=1,
                  annotation_text=f"flagged ≥ {MED_T:.2f}", annotation_font_color=INK_2)
    if show_no_graph:
        fig.add_trace(
            go.Scatter(
                x=df["seq"], y=df["risk_score_no_graph"], name="without graph signals",
                mode="lines+markers", line={"color": MUTED, "width": 2, "dash": "dash"},
                marker={"size": 8, "color": MUTED, "line": {"width": 2, "color": SURFACE}},
                hovertemplate="txn %{x} · score %{y:.3f} (no graph)<extra></extra>",
            )
        )
    fig.add_trace(
        go.Scatter(
            x=df["seq"], y=df["risk_score"], name="risk score",
            mode="lines+markers", line={"color": BLUE, "width": 2},
            marker={"size": 11, "color": [BAND_COLOR[b] for b in df["risk_band"]],
                        "line": {"width": 2, "color": SURFACE}},
            customdata=df[["merchant", "amount", "risk_band", "note"]],
            hovertemplate=(
                "<b>%{customdata[0]}</b> · ₹%{customdata[1]:,.0f}<br>"
                "score %{y:.3f} · band %{customdata[2]}<br>%{customdata[3]}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        showlegend=show_no_graph,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        yaxis={"range": [0, 1.02], "title": "risk score"},
        xaxis={"title": "transaction # in sequence", "dtick": 1},
    )
    return _base_layout(fig)


def contribution_chart(contributions: dict[str, float]) -> go.Figure:
    items = sorted(contributions.items(), key=lambda kv: kv[1])
    labels = [k for k, _ in items]
    values = [v for _, v in items]
    fig = go.Figure(
        go.Bar(
            x=values, y=labels, orientation="h", marker_color=BLUE,
            marker_line={"width": 0}, width=0.55,
            text=[f"+{v:.2f}" if v else "0" for v in values],
            textposition="outside", textfont_color=INK_2, cliponaxis=False,
            hovertemplate="%{y}: +%{x:.3f}<extra></extra>",
        )
    )
    fig.update_layout(xaxis={"range": [0, 0.45], "title": "contribution to score (capped per signal)"})
    return _base_layout(fig, height=230)


def score_bullet(score: float) -> go.Figure:
    band = band_for(score).value
    fig = go.Figure(
        go.Bar(
            x=[score], y=["score"], orientation="h",
            marker_color=BAND_COLOR[band], width=0.5,
            text=[f"{score:.2f} · {band}"], textposition="outside",
            textfont={"color": INK, "size": 14}, cliponaxis=False,
            hovertemplate=f"score {score:.3f} · {band}<extra></extra>",
        )
    )
    fig.add_vline(x=MED_T, line_dash="dot", line_color="#c98500", line_width=1)
    fig.add_vline(x=HIGH_T, line_dash="dot", line_color=BAND_COLOR["high"], line_width=1)
    fig.update_layout(xaxis={"range": [0, 1.12], "tickvals": [0, MED_T, HIGH_T, 1.0]},
                      yaxis={"showticklabels": False})
    return _base_layout(fig, height=140)


def geo_map(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(
        go.Scattergeo(
            lat=df["lat"], lon=df["lon"], mode="markers",
            marker={"size": 10, "color": [BAND_COLOR[b] for b in df["risk_band"]],
                        "line": {"width": 2, "color": SURFACE}},
            customdata=df[["merchant", "city", "risk_score", "risk_band"]],
            hovertemplate=(
                "<b>%{customdata[1]}</b> · %{customdata[0]}<br>"
                "score %{customdata[2]:.3f} · band %{customdata[3]}<extra></extra>"
            ),
        )
    )
    fig.update_geos(
        projection_type="natural earth", bgcolor=SURFACE, showcountries=True,
        countrycolor=GRID, landcolor="#f0efec", showocean=True, oceancolor=SURFACE,
        coastlinecolor=GRID,
    )
    return _base_layout(fig, height=300)


# --------------------------------------------------------------------------
# Sidebar
# --------------------------------------------------------------------------

with st.sidebar:
    st.header("🛡️ Fraud demo")
    st.caption(
        "Runs the platform's real feature engineering and scorer in-process — "
        "no Kafka / MongoDB / Neo4j needed."
    )
    scenario_key = st.radio(
        "Example case",
        options=list(SCENARIOS),
        format_func=lambda k: f"{SCENARIOS[k].icon} {SCENARIOS[k].title}",
    )
    st.divider()
    st.caption(f"Risk bands: flagged ≥ {MED_T:.2f} · high ≥ {HIGH_T:.2f}")
    st.caption(
        "Scoring uses the deterministic heuristic fallback (`HeuristicScorer`) so results "
        "are reproducible; production serves the MLflow-registered XGBoost model with SHAP."
    )

scenario = SCENARIOS[scenario_key]

st.title("🛡️ Real-Time Fraud Detection — Interactive Demo")
tab_cases, tab_playground, tab_how = st.tabs(
    ["📋 Example cases", "🎛️ What-if playground", "🧭 How it works"]
)

# --------------------------------------------------------------------------
# Tab 1 — example cases
# --------------------------------------------------------------------------

with tab_cases:
    rows = run_scenario(scenario)
    df = pd.DataFrame([{k: v for k, v in r.items() if k not in ("features", "contributions")} for r in rows])
    flagged = df[df["risk_band"].isin(["medium", "high"])]

    st.subheader(f"{scenario.icon} {scenario.title}")
    st.markdown(scenario.summary)
    st.info(f"**What to watch:** {scenario.expected}", icon="👀")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Transactions", len(df))
    m2.metric("Flagged (medium+)", len(flagged))
    m3.metric("Peak risk score", f"{df['risk_score'].max():.2f}")
    m4.metric("Expected loss exposure", f"₹{flagged['expected_loss'].sum():,.0f}")

    show_no_graph = bool((df["risk_score"] - df["risk_score_no_graph"]).abs().max() > 1e-9)
    st.plotly_chart(timeline_chart(df, show_no_graph), width="stretch")
    if show_no_graph:
        st.caption(
            "Dashed gray line: what the score would be **without** Neo4j graph features — "
            "the ring only becomes visible through shared-device links."
        )

    table = df[["seq", "time", "user_id", "merchant", "city", "amount",
                "risk_score", "risk_band", "note"]].copy()
    table["risk_band"] = table["risk_band"].map(BAND_LABEL)
    st.dataframe(
        table,
        width="stretch",
        hide_index=True,
        column_config={
            "seq": st.column_config.NumberColumn("#", width="small"),
            "time": st.column_config.DatetimeColumn("time", format="HH:mm:ss"),
            "amount": st.column_config.NumberColumn("amount", format="₹%.0f"),
            "risk_score": st.column_config.ProgressColumn(
                "risk score", min_value=0.0, max_value=1.0, format="%.3f"
            ),
            "risk_band": st.column_config.TextColumn("band"),
        },
    )

    left, right = st.columns([1, 1])

    with left:
        st.markdown("#### 🔎 Case inspector")
        options = {f"#{r['seq']} · {r['merchant']} · ₹{r['amount']:,.0f}": r for r in rows}
        picked = options[st.selectbox("Inspect a transaction", list(options))]

        i1, i2, i3 = st.columns(3)
        i1.metric("Risk score", f"{picked['risk_score']:.3f}")
        i2.metric("Band", BAND_LABEL[picked["risk_band"]])
        i3.metric("Expected loss", f"₹{picked['expected_loss']:,.0f}")

        if picked["travel_kmh"] > 900:
            st.error(
                f"✈️ Implied travel: **{picked['travel_km']:,.0f} km** since this user's previous "
                f"transaction ⇒ **{picked['travel_kmh']:,.0f} km/h** — physically impossible.",
                icon="🚨",
            )

        st.plotly_chart(contribution_chart(picked["contributions"]), width="stretch")

        feats = picked["features"]
        st.caption(
            f"features: amt_z={feats['amt_z']} · txn_1m={feats['txn_1m']} · "
            f"new_geo={feats['new_geo']} · new_device={feats['new_device']} · "
            f"shared_device_flags={feats['shared_device_flags']} · "
            f"ring={feats['ring_id'] or '—'} (component={feats['component_size']})"
        )

    with right:
        st.markdown("#### 🗺️ Where it happened")
        if df["city"].nunique() > 1:
            st.plotly_chart(geo_map(df), width="stretch")
        else:
            st.caption(f"All activity in **{df['city'].iloc[0]}** — no geographic spread to map.")
            st.map(df[["lat", "lon"]].drop_duplicates(), zoom=9)

# --------------------------------------------------------------------------
# Tab 2 — what-if playground
# --------------------------------------------------------------------------

PRESETS = {
    "🛒 Normal purchase": {"amt_z": 0.3, "txn_1m": 1, "new_geo": False, "new_device": False, "ring": 0},
    "💳 Card-test probe": {"amt_z": -9.0, "txn_1m": 8, "new_geo": True, "new_device": True, "ring": 0},
    "✈️ Geo jump": {"amt_z": 6.5, "txn_1m": 1, "new_geo": True, "new_device": True, "ring": 0},
    "🕸️ Ring member": {"amt_z": 1.0, "txn_1m": 2, "new_geo": True, "new_device": True, "ring": 7},
}


def _apply_preset(p: dict) -> None:
    st.session_state.update(
        w_amt_z=p["amt_z"], w_txn1m=p["txn_1m"], w_geo=p["new_geo"],
        w_dev=p["new_device"], w_ring=p["ring"],
    )


for _k, _v in {"w_amt_z": 0.3, "w_txn1m": 1, "w_geo": False, "w_dev": False, "w_ring": 0}.items():
    st.session_state.setdefault(_k, _v)


with tab_playground:
    st.subheader("🎛️ What-if playground")
    st.markdown(
        "Move the signals the stream computes for every transaction and watch the score respond — "
        "this calls the exact `HeuristicScorer` used on the hot path."
    )

    pc = st.columns(len(PRESETS))
    for col, (name, preset) in zip(pc, PRESETS.items(), strict=False):
        col.button(name, on_click=_apply_preset, args=(preset,), width="stretch")

    ctl, viz = st.columns([1, 1.4])
    with ctl:
        amt_z = st.slider("Amount z-score vs user baseline (amt_z)", -12.0, 12.0, step=0.1, key="w_amt_z")
        txn_1m = st.slider("Transactions in the last minute (txn_1m)", 1, 15, key="w_txn1m")
        new_geo = st.toggle("Never-seen location (new_geo)", key="w_geo")
        new_device = st.toggle("Never-seen device (new_device)", key="w_dev")
        ring = st.slider("Shared-device ring links (shared_device_flags)", 0, 10, key="w_ring")
        amount = st.number_input("Transaction amount (₹)", 10, 1_000_000, 5_000, step=500)

    features = Features(
        amt_z=amt_z, txn_1m=txn_1m, new_geo=new_geo, new_device=new_device,
        shared_device_flags=ring,
    )
    score = HeuristicScorer().score_one(features.to_vector())
    band = band_for(score).value

    with viz:
        s1, s2, s3 = st.columns(3)
        s1.metric("Risk score", f"{score:.3f}")
        s2.metric("Band", BAND_LABEL[band])
        s3.metric("Expected loss", f"₹{score * amount:,.0f}")
        st.plotly_chart(score_bullet(score), width="stretch")
        st.plotly_chart(contribution_chart(score_contributions(features)), width="stretch")
        if band == "low":
            st.success("Below the review threshold — would pass through silently.", icon="✅")
        elif band == "medium":
            st.warning("Flagged — lands in the analyst review queue, ranked by expected loss.", icon="🟡")
        else:
            st.error("High risk — top of the queue; SHAP + LLM case summary generated.", icon="🔴")

# --------------------------------------------------------------------------
# Tab 3 — how it works
# --------------------------------------------------------------------------

with tab_how:
    st.subheader("🧭 What this demo exercises")
    st.markdown(
        f"""
| Stage | Production | In this demo |
|---|---|---|
| **Ingest** | Kafka (`raw-transactions`), at-least-once, DLQ | scripted example cases |
| **Features** | `compute_features` — rolling windows, Welford z-score, geo/device novelty | **same code, in-process** |
| **Graph** | Neo4j Aura — shared-device rings, community detection | per-case graph values |
| **Score** | MLflow XGBoost (`@{settings.mlflow_model_alias}`) with hot-reload; heuristic fallback | **the real heuristic fallback** |
| **Serve** | MongoDB `flagged_transactions`, alerts ranked by expected loss | in-memory dataframe |
| **Explain** | SHAP top features + LangChain RAG analyst | per-signal contribution chart |

**Why the scores behave the way they do**

- Each user gets a rolling baseline (mean/std via Welford's algorithm) — the same ₹30,000 that is
  routine for one user is a 60× anomaly for another.
- Signals are capped (`amt_z` → 0.40, velocity → 0.25, ring → 0.20, geo 0.15, device 0.10), so no
  single noisy feature can flag on its own; fraud patterns stack several.
- Graph features come from Neo4j in production — the **{SCENARIOS['fraud_ring'].icon} fraud-ring** case shows scores
  with and without them.

To run the full platform (Kafka → stream → Mongo → dashboard): `make up && make seed && make producer & make stream & make dashboard`.
        """
    )

st.caption("Demo mode · deterministic replay · fraud-platform v0.1")
