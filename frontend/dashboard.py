import streamlit as st
from pages.alerts import display_alerts_menu

st.set_page_config(
    page_title="Tupic",
    page_icon="🐧",
)
# if "selected_alert_id" in st.session_state:
#     show_alert_detail(st.session_state["selected_alert_id"])
# else:
#     show_alert_list()
# st.title("Dashboard")
# st.sidebar.success("Select page")

col1, col2 = st.columns(2)  # Split page into two equal columns

with col1:
    st.header("Left Side")
    st.write("Data sources go here")

with col2:
    if not "selected_alert_id" in st.session_state:
        st.session_state["selected_alert_id"] = None
    display_alerts_menu()
