"""Streamlit dashboard — live feed, review workflow, LLM analyst, ops panel.

Run:  make dashboard   (streamlit run src/fraud/dashboard/app.py)
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from config.settings import settings
from fraud.assist.agent import investigate
from fraud.assist.chains import rag_summary
from fraud.assist.llm import describe_provider
from fraud.assist.tracing import record_feedback
from fraud.producer.generator import inject_attack
from fraud.serving import mongo

st.set_page_config(page_title="Fraud Detection Console", layout="wide", page_icon="🛡️")


# --- data helpers ---------------------------------------------------------


@st.cache_data(ttl=3)
def load_alerts(limit: int = 100) -> pd.DataFrame:
    docs = mongo.top_alerts(limit=limit)
    if not docs:
        return pd.DataFrame()
    rows = []
    for d in docs:
        rows.append(
            {
                "txn_id": d.get("_id"),
                "user_id": d.get("user_id"),
                "amount": d.get("amount"),
                "risk_score": d.get("risk_score"),
                "risk_band": d.get("risk_band"),
                "expected_loss": round(d.get("risk_score", 0) * d.get("amount", 0), 2),
                "merchant": d.get("merchant"),
                "ring_id": (d.get("graph") or {}).get("ring_id"),
                "status": d.get("status"),
                "lat": (d.get("geo") or {}).get("lat"),
                "lon": (d.get("geo") or {}).get("lon"),
                "created_at": d.get("created_at"),
            }
        )
    return pd.DataFrame(rows)


def _ops_metrics(df: pd.DataFrame) -> dict[str, object]:
    total = mongo.flagged().count_documents({})
    flagged = mongo.flagged().count_documents({"risk_band": {"$in": ["high", "medium"]}})
    version = None
    if not df.empty:
        latest = mongo.flagged().find_one(sort=[("created_at", -1)])
        version = (latest or {}).get("model_version")
    return {
        "total_scored": total,
        "flagged": flagged,
        "flag_rate": round(flagged / total, 3) if total else 0.0,
        "model_version": version or "n/a",
    }


# --- header + ops panel ---------------------------------------------------

st.title("🛡️ Real-Time Fraud Detection Console")

df = load_alerts()
ops = _ops_metrics(df)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Scored txns", ops["total_scored"])
c2.metric("Flagged", ops["flagged"])
c3.metric("Flag rate", f"{ops['flag_rate'] * 100:.1f}%")
c4.metric("Model", f"v{ops['model_version']}")
c5.metric("Analyst LLM", describe_provider())

with st.sidebar:
    st.header("⚙️ Controls")
    st.caption("Neo4j Aura: " + ("connected" if settings.neo4j_configured else "NOT configured"))
    st.caption("LangSmith: " + ("on" if settings.langsmith_enabled else "off"))
    st.subheader("💥 Attack injector")
    if st.button("Inject card-testing burst"):
        inject_attack("card_testing")
        st.success("Card-testing burst queued to the producer.")
    if st.button("Inject geo-impossible travel"):
        inject_attack("geo_impossible")
        st.success("Geo-impossible travel queued to the producer.")
    auto = st.checkbox("Auto-refresh (3s)", value=True)

# --- live feed + charts ---------------------------------------------------

left, right = st.columns([2, 1])

with left:
    st.subheader("🚨 Live alerts (by expected loss)")
    if df.empty:
        st.info("No alerts yet. Run `make producer` and `make stream`.")
    else:
        st.dataframe(
            df[["txn_id", "user_id", "amount", "risk_score", "risk_band", "expected_loss", "ring_id"]],
            use_container_width=True,
            height=320,
        )

with right:
    st.subheader("📊 Score distribution")
    if not df.empty:
        st.bar_chart(df["risk_score"])
    st.subheader("🗺️ Geo of flagged activity")
    if not df.empty and df[["lat", "lon"]].dropna().shape[0] > 0:
        st.map(df[["lat", "lon"]].dropna())

# --- review panel ---------------------------------------------------------

st.divider()
st.subheader("🔎 Case review")

if not df.empty:
    txn_id = st.selectbox("Select a case", df["txn_id"].tolist())
    doc = mongo.flagged().find_one({"_id": txn_id})
    if doc:
        rc1, rc2 = st.columns(2)
        with rc1:
            st.markdown("**Transaction**")
            st.json(
                {
                    "user_id": doc.get("user_id"),
                    "amount": doc.get("amount"),
                    "merchant": doc.get("merchant"),
                    "risk_score": doc.get("risk_score"),
                    "risk_band": doc.get("risk_band"),
                }
            )
            st.markdown("**SHAP top features**")
            st.table(pd.DataFrame(doc.get("shap_top", []), columns=["feature", "contribution"]))
            st.markdown("**Graph / ring**")
            st.json(doc.get("graph", {}))

        with rc2:
            st.markdown("**Label this case**")
            lc1, lc2 = st.columns(2)
            if lc1.button("🚩 Mark FRAUD"):
                mongo.set_label(txn_id, True)
                st.success("Labelled fraud (feeds retraining).")
            if lc2.button("✅ Mark NOT fraud"):
                mongo.set_label(txn_id, False)
                st.success("Labelled legit (feeds retraining).")

            st.markdown("**🦜 LLM analyst (LangChain, off hot path)**")
            if st.button("Explain this case"):
                history = mongo.get_user_history(doc.get("user_id"), limit=5)
                with st.spinner("Generating grounded, cited summary..."):
                    summary = rag_summary(doc, history)
                st.session_state["summary"] = summary.model_dump()
                st.session_state["summary_prompt"] = {
                    "txn": f"{doc.get('merchant')} {doc.get('amount')}",
                    "shap": str(doc.get("shap_top", [])),
                    "history": str(history[:3]),
                }

            if "summary" in st.session_state:
                s = st.session_state["summary"]
                st.info(s["summary"])
                st.caption(
                    f"model={s['model']} · grounded={s['grounded']} · "
                    f"cited={', '.join(s['cited_case_ids']) or 'none'}"
                )
                fb1, fb2 = st.columns(2)
                if fb1.button("👍 Good summary"):
                    record_feedback(
                        txn_id, s["summary"], 1, st.session_state.get("summary_prompt")
                    )
                    st.success("Thumbs up saved (feeds LangSmith + fine-tune set).")
                if fb2.button("👎 Bad summary"):
                    record_feedback(
                        txn_id, s["summary"], 0, st.session_state.get("summary_prompt")
                    )
                    st.warning("Thumbs down saved.")

            st.markdown("**🕵️ Investigation agent**")
            if st.button("Is this part of a ring?"):
                with st.spinner("Agent orchestrating graph + history + retrieval..."):
                    result = investigate(
                        doc.get("user_id"), "Is this user part of a fraud ring?", doc
                    )
                st.text_area("Agent answer", result["answer"], height=200)
                st.caption(f"mode={result['mode']} · model={result['model']}")

st.caption(f"Rendered {datetime.now(timezone.utc).isoformat()} · serving: {settings.mongo_db}")

if auto:
    time.sleep(3)
    st.rerun()
