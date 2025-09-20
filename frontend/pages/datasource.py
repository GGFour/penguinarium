import streamlit as st
from data import data_sources
from navigator import display_alerts_menu


def go_back_to_list():
    st.session_state["selected_data_source"] = None
    st.switch_page("dashboard.py")


st.set_page_config(
    page_title="Data Source Detail - Tupic",
    page_icon="🐧",
)

if "selected_data_source" not in st.session_state or st.session_state["selected_data_source"] is None:
    st.write("No data source selected. Please go back to the dashboard.")
    if st.button("Go back to Dashboard"):
        st.switch_page("dashboard.py")
else:
    data_source = next(
        (ds for ds in data_sources if ds["id"] == st.session_state["selected_data_source"]), None)
    if data_source:
        col1, col2 = st.columns(2)  # Split page into two equal columns
        with col1:
            st.title(data_source["name"])
            st.write(f"**Type:** {data_source['type']}")
            st.write(
                f"**Connection Status:** {data_source['connection status']}")
            if st.button("Back to Dashboard"):
                go_back_to_list()
        with col2:
            display_alerts_menu(data_source["id"])
    else:
        st.write("Data source not found.")
        if st.button("Go back to Dashboard"):
            st.switch_page("dashboard.py")
