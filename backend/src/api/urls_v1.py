from django.urls import path

from .views.v1 import (
    UsersCreateView,
    UsersRetrieveView,
    UserDataSourcesListView,
    DataSourceRetrieveView,
    DataSourceStatusView,
    DataSourceAlertsListView,
    DataSourceTablesListView,
    AlertRetrieveView,
)


urlpatterns = [
    # Users
    path("users", UsersCreateView.as_view(), name="v1-users-create"),
    path("users/<str:user_id>", UsersRetrieveView.as_view(), name="v1-users-retrieve"),
    path("users/<str:user_id>/datasources", UserDataSourcesListView.as_view(), name="v1-user-datasources"),

    # Data sources
    path("datasources/<str:datasource_id>", DataSourceRetrieveView.as_view(), name="v1-datasource-retrieve"),
    path("datasources/<str:datasource_id>/status", DataSourceStatusView.as_view(), name="v1-datasource-status"),
    path("datasources/<str:datasource_id>/alerts", DataSourceAlertsListView.as_view(), name="v1-datasource-alerts"),
    path("datasources/<str:datasource_id>/tables", DataSourceTablesListView.as_view(), name="v1-datasource-tables"),

    # Alerts
    path("alerts/<str:alert_id>", AlertRetrieveView.as_view(), name="v1-alert-retrieve"),
]
