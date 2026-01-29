import time
import requests
import pandas as pd
import streamlit as st

DEFAULT_API = "http://127.0.0.1:8000"

st.set_page_config(page_title="Fraud Review Dashboard", layout="wide")

st.title("Fraud / Anomaly Detection Dashboard")

with st.sidebar:
    st.header("Connection")
    api_base = st.text_input("API Base URL", value=DEFAULT_API)

    st.header("Filters")
    flagged_only = st.checkbox("Flagged only", value=True)
    user_id = st.text_input("User ID (optional)", value="")
    min_risk = st.slider("Min risk score", min_value=0, max_value=100, value=70)
    limit = st.slider("Limit", min_value=10, max_value=300, value=100)
    auto_refresh = st.checkbox("Auto refresh", value=False)
    refresh_seconds = st.slider("Refresh interval (sec)", 2, 30, 5)

    st.header("Actions")
    retrain_percentile = st.slider("Retrain percentile", 90.0, 99.9, 98.0)
    do_retrain = st.button("Retrain Model")


def api_get(path: str, params: dict | None = None):
    r = requests.get(f"{api_base}{path}", params=params, timeout=20)
    if r.status_code >= 300:
        raise RuntimeError(f"GET {path} failed {r.status_code}: {r.text}")
    return r.json()


def api_post(path: str):
    r = requests.post(f"{api_base}{path}", timeout=120)
    if r.status_code >= 300:
        raise RuntimeError(f"POST {path} failed {r.status_code}: {r.text}")
    return r.json()


if do_retrain:
    try:
        out = api_post(f"/retrain?percentile={float(retrain_percentile)}")
        st.success(f"Retrained. threshold={out['threshold']:.4f}, rows={out['trained_rows']}, pctl={out['percentile']}")
    except Exception as e:
        st.error(str(e))

st.subheader("Model Runs")

try:
    runs = api_get("/model-runs", params={"limit": 10})
    runs_df = pd.DataFrame(runs)
    if len(runs_df) == 0:
        st.info("No model runs yet. Click Retrain Model.")
    else:
        st.dataframe(runs_df, use_container_width=True, hide_index=True)
except Exception as e:
    st.error(f"Could not load model runs: {e}")

st.subheader("Fraud Review Queue")

params = {
    "limit": int(limit),
    "flagged_only": "true" if flagged_only else "false",
    "min_risk": float(min_risk),
}
if user_id.strip() != "":
    params["user_id"] = user_id.strip()

try:
    scores = api_get("/scores", params=params)
    df = pd.DataFrame(scores)

    if len(df) == 0:
        st.info("No events match current filters.")
    else:
        df["flagged"] = df["flagged"].astype(bool)
        df["reasons_short"] = df["reasons"].apply(lambda rs: ", ".join([r.get("reason", "") for r in rs]) if isinstance(rs, list) else "")

        left, right = st.columns([2, 1])

        with left:
            cols = [
                "scored_at_utc",
                "event_id",
                "user_id",
                "merchant_id",
                "amount",
                "risk_score",
                "flagged",
                "reasons_short",
            ]
            view_df = df[cols].copy()
            st.dataframe(view_df, use_container_width=True, hide_index=True)

        with right:
            st.markdown("### Score Distribution")
            st.bar_chart(df["risk_score"])

            st.markdown("### Flagged Count")
            st.metric("Flagged", int(df["flagged"].sum()))
            st.metric("Total shown", int(len(df)))

        st.markdown("### Drill-down")
        selected_event = st.selectbox("Pick an event_id to inspect", df["event_id"].tolist())
        row = df[df["event_id"] == selected_event].iloc[0].to_dict()
        st.json(row)

except Exception as e:
    st.error(f"Could not load scores: {e}")

if auto_refresh:
    time.sleep(int(refresh_seconds))
    st.rerun()