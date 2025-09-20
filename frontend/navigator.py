import streamlit as st
from data import alerts_list


def go_to_selected_data_source(source_id):
    st.session_state.selected_data_source = source_id
    st.switch_page("pages/datasource.py")


def go_to_selected_alert_id(id: int):
    st.session_state["selected_alert_id"] = id
    st.switch_page("pages/alert.py")


def show_alert_list():
    st.header("Data Anomaly Alerts")
    st.write("Click on an alert to learn more about the issue.")
    for alert in alerts_list:
        if st.button(alert["name"], key=f"alert_{alert['id']}"):
            go_to_selected_alert_id(alert["id"])


def display_alerts_menu():
    show_alert_list()
