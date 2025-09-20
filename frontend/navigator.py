import streamlit as st
from data import alerts_list


def go_to_selected_data_source(source_id):
    st.session_state.selected_data_source = source_id
    st.switch_page("pages/datasource.py")


def go_to_selected_alert_id(id: int):
    st.session_state["selected_alert_id"] = id
    st.switch_page("pages/alert.py")


def show_alert_list(source_id=None):
    st.header("Data Anomaly Alerts")
    st.write("Click on an alert to learn more about the issue.")

    # Filter alerts by source_id if provided
    filtered_alerts = alerts_list
    if source_id is not None:
        filtered_alerts = [alert for alert in alerts_list if alert.get(
            "source_id") == source_id]

    for alert in filtered_alerts:
        if st.button(alert["name"], key=f"alert_{alert['id']}"):
            go_to_selected_alert_id(alert["id"])


def display_alerts_menu(source_id=None):
    show_alert_list(source_id)
