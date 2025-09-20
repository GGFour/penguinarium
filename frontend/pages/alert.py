import streamlit as st
from data import alerts_list, data_sources
from navigator import go_to_selected_data_source


def go_back_to_list():
    st.session_state["selected_alert_id"] = None
    st.switch_page("dashboard.py")


st.set_page_config(
    page_title="Alert Detail - Tupic",
    page_icon="🐧",
)

if "selected_alert_id" not in st.session_state or st.session_state["selected_alert_id"] is None:
    st.write("No alert selected. Please go back to the alert list.")
    if st.button("Go back to Dashboard"):
        st.switch_page("dashboard.py")
else:
    alert = next(
        (a for a in alerts_list() if a["id"] == st.session_state["selected_alert_id"]), None)
    if alert:
        st.title(alert["name"])
        st.write(alert["detail"])

        # Display data source information
        data_source = next(
            (ds for ds in data_sources() if ds["id"] == alert["source_id"]), None)
        if data_source:
            st.write("---")
            st.subheader("Data Source")
            st.write(f"**Name:** {data_source['name']}")
            st.write(f"**Type:** {data_source['type']}")
            st.write(
                f"**Connection Status:** {data_source['connection_status']}")

            # Add link to data source
            if st.button(f"View {data_source['name']} Details", key="view_datasource"):
                go_to_selected_data_source(data_source["id"])

        st.write("---")
        if st.button("Back to Alert List"):
            go_back_to_list()
    else:
        st.write("Alert not found.")
        if st.button("Go back to Dashboard"):
            st.switch_page("dashboard.py")
