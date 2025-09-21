import streamlit as st
from navigator import display_alerts_menu, go_to_selected_data_source
from data import data_sources
import requests


# if "selected_alert_id" in st.session_state:
#     show_alert_detail(st.session_state["selected_alert_id"])
# else:
#     show_alert_list()
# st.title("Dashboard")
# st.sidebar.success("Select page")
API_URL = "https://api.example.com/datapoint"  # replace with your API endpoint
FIELD_NAME = "file"  # or e.g. "files[]" if your API expects that
left, right = st.columns([1, 0.5])
with right:

    uploaded_files = st.file_uploader(
        "Choose one or more CSV files",
        type=["csv"],
        accept_multiple_files=True
    )
    if uploaded_files:
        st.write("Selected files:")
    for f in uploaded_files:
        st.write(f.name)

    if st.button("Send all to API"):
        # Build a list of (field_name, (filename, bytes, content_type)) tuples
        files = [
            (FIELD_NAME, (f.name, f.getvalue(), "text/csv"))
            for f in uploaded_files
        ]

        try:
            resp = requests.post(API_URL, files=files, timeout=240)
            st.write(f"Status: {resp.status_code}")
            st.text(resp.text[:2000])
        except requests.RequestException as e:
            st.error(f"Request failed: {e}")
with left:
    col1, col2 = st.columns(2)  # Split page into two equal columns

    with col1:
        st.header("Data Sources")
        st.write("Click on a data source to view details.")
        for data_source in data_sources():
            if st.button(data_source["name"], key=data_source["id"]):
                go_to_selected_data_source(data_source["id"])

    with col2:
        display_alerts_menu()
