# /pages/2_Alerts_Table.py
import streamlit as st
import pandas as pd
import requests
from io import StringIO

st.set_page_config(page_title="Anomaly Alerts", layout="wide")
st.title("Anomaly Alerts")

# Example API endpoint; replace with a real one
DEFAULT_API_URL = "https://api.example.com/anomalies"

# --- Embedded CSV sample data (replaces old SAMPLE_DATA list) ---
CSV_DATA = """SK_ID_CURR,SK_ID_BUREAU,CREDIT_ACTIVE,CREDIT_CURRENCY,DAYS_CREDIT,CREDIT_DAY_OVERDUE,DAYS_CREDIT_ENDDATE,DAYS_ENDDATE_FACT,AMT_CREDIT_MAX_OVERDUE,CNT_CREDIT_PROLONG,AMT_CREDIT_SUM,AMT_CREDIT_SUM_DEBT,AMT_CREDIT_SUM_LIMIT,AMT_CREDIT_SUM_OVERDUE,CREDIT_TYPE,DAYS_CREDIT_UPDATE,AMT_ANNUITY
215354,5714462,Closed,currency 1,-497,0,-153.0,-153.0,,0,91323.0,0.0,,0.0,Consumer credit,-131,
215354,5714463,Active,currency 1,-208,0,1075.0,,,0,225000.0,171342.0,,0.0,Credit card,-20,
215354,5714464,Active,currency 1,-203,0,528.0,,,0,464323.5,,,0.0,Consumer credit,-16,
215354,5714465,Active,currency 1,-203,0,,,,0,90000.0,,,0.0,Credit card,-16,
215354,5714466,Active,currency 1,-629,0,1197.0,,77674.5,0,2700000.0,,,0.0,Consumer credit,-21,
215354,5714467,Active,currency 1,-273,0,27460.0,,0.0,0,180000.0,71017.38,108982.62,0.0,Credit card,-31,
215354,5714468,Active,currency 1,-43,0,79.0,,0.0,0,42103.8,42103.8,0.0,0.0,Consumer credit,-22,
162297,5714469,Closed,currency 1,-1896,0,-1684.0,-1710.0,14985.0,0,76878.45,0.0,0.0,0.0,Consumer credit,-1710,
162297,5714470,Closed,currency 1,-1146,0,-811.0,-840.0,0.0,0,103007.7,0.0,0.0,0.0,Consumer credit,-840,
162297,5714471,Active,currency 1,-1146,0,-484.0,,0.0,0,4500.0,0.0,0.0,0.0,Credit card,-690,
162297,5714472,Active,currency 1,-1146,0,-180.0,,0.0,0,337500.0,0.0,0.0,0.0,Credit card,-690,
162297,5714473,Closed,currency 1,-2456,0,-629.0,-825.0,,0,675000.0,0.0,0.0,0.0,Consumer credit,-706,
162297,5714474,Active,currency 1,-277,0,5261.0,,0.0,0,7033500.0,,,0.0,Mortgage,-31,
402440,5714475,Active,currency 1,-96,0,269.0,,0.0,0,89910.0,76905.0,0.0,0.0,Consumer credit,-22,
238881,5714482,Closed,currency 1,-318,0,-187.0,-187.0,,0,0.0,0.0,0.0,0.0,Credit card,-185,
238881,5714484,Closed,currency 1,-2911,0,-2607.0,-2604.0,,0,48555.0,,,0.0,Consumer credit,-2601,
238881,5714485,Closed,currency 1,-2148,0,-1595.0,-987.0,,0,135000.0,,,0.0,Consumer credit,-984,
238881,5714486,Active,currency 1,-381,0,,,,0,450000.0,520920.0,,0.0,Consumer credit,-4,
238881,5714487,Active,currency 1,-95,0,1720.0,,,0,67500.0,8131.5,,0.0,Credit card,-7,
238881,5714488,Closed,currency 1,-444,0,-77.0,-77.0,0.0,0,107184.06,0.0,0.0,0.0,Consumer credit,-71,
238881,5714489,Active,currency 1,-392,0,,,0.0,0,252000.0,23679.0,228320.1,0.0,Credit card,-22,
222183,5714491,Active,currency 1,-784,0,1008.0,,0.0,0,0.0,-411.615,411.615,0.0,Credit card,-694,
222183,5714492,Active,currency 1,-774,0,625.0,,,0,127840.5,0.0,0.0,0.0,Credit card,-210,
222183,5714493,Active,currency 1,-395,0,1431.0,,,0,1350000.0,1185493.5,0.0,0.0,Consumer credit,-24,
222183,5714495,Closed,currency 1,-2744,0,-2561.0,-2559.0,310.5,0,18157.5,,,0.0,Consumer credit,-2559,
222183,5714496,Closed,currency 1,-1103,0,-7.0,-343.0,20493.27,0,675000.0,0.0,0.0,0.0,Consumer credit,-343,
222183,5714497,Active,currency 1,-315,0,1512.0,,88821.0,0,3709552.5,,,0.0,Car loan,-32,
426155,5714498,Closed,currency 1,-1331,0,-994.0,-1023.0,1350.0,0,39433.5,0.0,0.0,0.0,Consumer credit,-1023,
426155,5714499,Closed,currency 1,-2534,0,-2352.0,-2347.0,,0,38830.5,0.0,0.0,0.0,Consumer credit,-2345,
"""

# Parse the CSV into a list of dicts for the sample path
SAMPLE_DATA = pd.read_csv(StringIO(CSV_DATA)).to_dict(orient="records")


@st.cache_data(ttl=300, show_spinner=False)
def fetch_alerts(url: str):
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


# Controls
api_url = st.text_input(
    "API URL",
    value=DEFAULT_API_URL,
    help="Enter an endpoint that returns a JSON array of records"
)
use_sample = st.toggle("Use sample data (CSV)", value=True)

# Fetch or load data
if use_sample:
    data = SAMPLE_DATA
else:
    try:
        with st.spinner("Fetching data from API..."):
            data = fetch_alerts(api_url)
    except Exception as e:
        st.warning(
            f"API request failed ({e}); falling back to CSV sample data.")
        data = SAMPLE_DATA

# Normalize into a DataFrame
df = pd.DataFrame(data)

# Styling: color DAYS_CREDIT_UPDATE red when negative


def style_days_credit_update(series: pd.Series):
    vals = pd.to_numeric(series, errors="coerce")
    return ["color: red" if pd.notna(v) and v < 0 else None for v in vals]


# Build Styler conditionally
styled = df.style
if "DAYS_CREDIT_UPDATE" in df.columns:
    styled = styled.apply(style_days_credit_update,
                          subset=["DAYS_CREDIT_UPDATE"])

st.dataframe(styled, use_container_width=True, hide_index=True)
