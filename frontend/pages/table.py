# /pages/2_Alerts_Table.py
import streamlit as st
import pandas as pd
import requests

st.set_page_config(page_title="Anomaly Alerts", layout="wide")
st.title("Anomaly Alerts")

# Example API endpoint; replace with a real one
DEFAULT_API_URL = "https://api.example.com/anomalies"

# A sample data structure matching the table; used as fallback and for local testing
SAMPLE_DATA = [
    {
        "id": "ALRT-001",
        "metric": "transactions_error_rate",
        "value": 7.4,
        "threshold": 5.0,
        "status": "alert",
        "severity": "high",
        "timestamp": "2025-09-20T18:05:00Z",
        "link": "https://example.com/alerts/ALRT-001"
    },
    {
        "id": "ALRT-002",
        "metric": "latency_p95_ms",
        "value": 480,
        "threshold": 500,
        "status": "ok",
        "severity": "low",
        "timestamp": "2025-09-20T18:10:00Z",
        "link": "https://example.com/alerts/ALRT-002"
    },
    {
        "id": "ALRT-003",
        "metric": "dropped_events_pct",
        "value": 2.2,
        "threshold": 1.0,
        "status": "alert",
        "severity": "critical",
        "timestamp": "2025-09-20T18:12:00Z",
        "link": "https://example.com/alerts/ALRT-003"
    }
]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_alerts(url: str):
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


# Controls
api_url = st.text_input("API URL", value=DEFAULT_API_URL,
                        help="Enter an endpoint that returns a JSON array of records")
use_sample = st.toggle("Use sample data (no API)", value=False)

# Fetch or load data
if use_sample:
    data = SAMPLE_DATA
else:
    try:
        with st.spinner("Fetching data from API..."):
            data = fetch_alerts(api_url)
    except Exception as e:
        st.warning(f"API request failed ({e}); falling back to sample data.")
        data = SAMPLE_DATA

# Normalize into a DataFrame
df = pd.DataFrame(data)

# Ensure expected columns exist; add missing with defaults
expected_cols = ["id", "metric", "value", "threshold",
                 "status", "severity", "timestamp", "link"]
for c in expected_cols:
    if c not in df.columns:
        df[c] = None

# Convert numeric columns safely
for c in ["value", "threshold"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Build conditional styles:
# - value column red when value >= threshold
# - status column red for "alert" or "fail"
# - severity column red for "high" or "critical"


def style_value_col(_series):
    styles = []
    for v, t in zip(df["value"], df["threshold"]):
        styles.append("color: red" if pd.notna(
            v) and pd.notna(t) and v >= t else None)
    return styles


def style_status_col(series):
    return ["color: red" if str(s).lower() in ("alert", "fail") else None for s in series]


def style_severity_col(series):
    return ["color: red" if str(s).lower() in ("high", "critical") else None for s in series]


styled = (
    df.style
      .apply(style_value_col, subset=["value"])
      .apply(style_status_col, subset=["status"])
      .apply(style_severity_col, subset=["severity"])
)

st.dataframe(styled, use_container_width=True, hide_index=True)
