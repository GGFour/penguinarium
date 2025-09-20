import streamlit as st
from alerts import alerts_list, alert_navigate

if not "selected_alert_id" in st.session_state:
    st.session_state.selected_alert_id = None
alert = next((a for a in alerts_list if a["id"] ==
             st.session_state("selected_alert_id")), None)
if alert:
    st.title(alert["name"])
    st.write(alert["detail"])
    st.button("Back to Alert List",
              on_click=alert_navigate(None))
