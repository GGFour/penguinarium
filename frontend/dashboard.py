import streamlit as st
from navigator import show_alert_list, go_to_selected_data_source
from data import data_sources


# if "selected_alert_id" in st.session_state:
#     show_alert_detail(st.session_state["selected_alert_id"])
# else:
#     show_alert_list()
# st.title("Dashboard")
# st.sidebar.success("Select page")

col1, col2 = st.columns(2)  # Split page into two equal columns

with col1:
    st.header("Data Sources")
    st.write("Click on a data source to view details.")
    for data_source in data_sources:
        if st.button(data_source["name"], key=data_source["id"]):
            go_to_selected_data_source(data_source["id"])

with col2:
    show_alert_list()
