from api_client import fetch_data_sources, fetch_alerts


def get_data_sources():
    """Get data sources from API and transform to frontend format."""
    api_data = fetch_data_sources()
    return [
        {
            "id": ds["data_source_id"],
            "name": ds["name"],
            "type": ds["type"],
            # You might want to add this to API
            "connection_status": "Connection established",
            "global_id": ds["global_id"],
            "connection_info": ds["connection_info"],
            "created_at": ds["created_at"],
            "updated_at": ds["updated_at"]
        }
        for ds in api_data
    ]


def get_alerts_list():
    """Get alerts from API and transform to frontend format."""
    api_data = fetch_alerts()
    return [
        {
            "id": alert["alert_id"],
            "name": alert["name"],
            "detail": alert.get("description", "No description available"),
            "source_id": alert.get("data_source_id"),
            "severity": alert.get("severity", "unknown"),
            "status": alert.get("status", "active"),
            "created_at": alert.get("created_at"),
            "updated_at": alert.get("updated_at")
        }
        for alert in api_data
    ]

# For backward compatibility, we'll create functions that can be called


def data_sources():
    return get_data_sources()


def alerts_list():
    return get_alerts_list()
