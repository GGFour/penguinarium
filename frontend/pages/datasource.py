import os
import requests
import pandas as pd
import streamlit as st
from data import data_sources
from navigator import go_to_selected_data_source

API_BASE = f"http://{os.environ['API_HOST']}:{os.environ['API_PORT']}/api"


def go_back_to_list():
    st.session_state["selected_data_source"] = None
    st.switch_page("dashboard.py")


st.set_page_config(
    page_title="Data Source Alerts - Tupic",
    page_icon="🐧",
)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_alerts_for_data_source(api_base: str, source_id: str | int):
    url = f"{api_base}/data-sources/{source_id}/alerts"
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.json()


# Ensure a data-source is selected
if "selected_data_source" not in st.session_state or st.session_state["selected_data_source"] is None:
    st.write("No data-source selected. Please go back to the dashboard.")
    if st.button("Go back to Dashboard"):
        st.switch_page("dashboard.py")
else:
    source_id = st.session_state["selected_data_source"]

    # Resolve data source info if available locally
    ds = next((d for d in data_sources() if d["id"] == source_id), None)

    # Header
    title_text = f"Data Source {source_id}" if not ds else f"{ds.get('name', f'Data Source {source_id}')}"
    st.title(title_text)

    # Optional data source info
    if ds:
        st.write("---")
        st.subheader("Data Source")
        st.write(f"Name: {ds.get('name', source_id)}")
        st.write(f"Type: {ds.get('type', 'unknown')}")
        st.write(
            f"Connection Status: {ds.get('connection_status', 'unknown')}")
        if st.button(f"View {ds.get('name', 'Source')} Details", key="view_datasource"):
            go_to_selected_data_source(ds["id"])

    # Fetch and display alerts for this data-source
    st.write("---")
    try:
        with st.spinner("Fetching alerts for this data-source..."):
            alerts = fetch_alerts_for_data_source(API_BASE, source_id)
    except Exception as e:
        st.error(f"Failed to fetch alerts: {e}")
        alerts = []

    # Render alerts
    st.subheader("Alerts")
    if isinstance(alerts, list) and alerts:
        if isinstance(alerts[0], dict):
            st.caption(f"{len(alerts)} alert(s) found.")
            st.dataframe(pd.DataFrame(alerts),
                         use_container_width=True, hide_index=True)
        else:
            st.json(alerts)
    else:
        st.info("No alerts found for this data-source.")

    st.write("---")
    if st.button("Back to Dashboard"):
        go_back_to_list()
