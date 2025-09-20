import streamlit as st
from data import alerts_list


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
        (a for a in alerts_list if a["id"] == st.session_state["selected_alert_id"]), None)
    if alert:
        st.title(alert["name"])
        st.write(alert["detail"])
        if st.button("Back to Alert List"):
            go_back_to_list()
    else:
        st.write("Alert not found.")
        if st.button("Go back to Dashboard"):
            st.switch_page("dashboard.py")
