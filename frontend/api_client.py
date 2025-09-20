import streamlit as st
import requests
import json
from typing import List, Dict, Optional

# API Configuration
API_BASE_URL = "http://localhost:8000/api"


@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_data_sources() -> List[Dict]:
    """Fetch data sources from the API with caching."""
    try:
        response = requests.get(f"{API_BASE_URL}/data-sources/", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch data sources: {str(e)}")
        return []


@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_alerts() -> List[Dict]:
    """Fetch alerts from the API with caching."""
    try:
        response = requests.get(f"{API_BASE_URL}/alerts/", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch alerts: {str(e)}")
        return []


def get_data_source_by_id(data_source_id: int) -> Optional[Dict]:
    """Get a specific data source by ID."""
    data_sources = fetch_data_sources()
    return next((ds for ds in data_sources if ds["data_source_id"] == data_source_id), None)


def get_alert_by_id(alert_id: int) -> Optional[Dict]:
    """Get a specific alert by ID."""
    alerts = fetch_alerts()
    return next((alert for alert in alerts if alert["alert_id"] == alert_id), None)


def get_alerts_by_data_source(data_source_id: int) -> List[Dict]:
    """Get alerts filtered by data source ID."""
    alerts = fetch_alerts()
    return [alert for alert in alerts if alert.get("data_source_id") == data_source_id]
