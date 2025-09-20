import streamlit as st

# Example alerts data with details
alerts_list = [
    {
        "id": 1,
        "name": "Missing Values Detected",
        "detail": "This alert occurs when your dataset contains missing or null values that need to be addressed."
    },
    {
        "id": 2,
        "name": "Duplicate Rows Found",
        "detail": "This alert indicates that your data contains duplicate records."
    },
    {
        "id": 3,
        "name": "Outlier Values Detected",
        "detail": "Detected data points that are significantly different from others, possibly indicating errors or special cases."
    },
    {
        "id": 4,
        "name": "Schema Change Alert",
        "detail": "The structure of your dataset has changed, such as new or missing columns."
    }
]


def set_selected_alert_id(alert_id: int):
    st.session_state["selected_alert_id"] = alert_id


def show_alert_list():
    if not "selected_alert_id" in st.session_state:
        st.session_state["selected_alert_id"] = None
    st.title("Data Anomaly Alerts")
    st.write("Click on an alert to learn more about the issue.")
    for alert in alerts_list:
        st.button(alert["name"], key=alert["id"],
                  on_click=alert_navigate(alert["id"]))


def show_alert_detail(alert_id):
    alert = next((a for a in alerts_list if a["id"] == alert_id), None)
    if alert:
        st.title(alert["name"])
        st.write(alert["detail"])
        st.button("Back to Alert List",
                  on_click=alert_navigate(None))


def alert_navigate(alert_id=None):
    if not "selected_alert_id" in st.session_state:
        st.session_state.selected_alert_id = alert_id
    if alert_id is not None:
        st.switch_page("pages/alert.py")
    else:
        st.switch_page("dashboard.py")


def display_alerts_menu():
    if not "selected_alert_id" in st.session_state:
        st.session_state["selected_alert_id"] = None
    if st.session_state["selected_alert_id"] is not None:
        alert_navigate(st.session_state["selected_alert_id"])
    else:
        show_alert_list()
