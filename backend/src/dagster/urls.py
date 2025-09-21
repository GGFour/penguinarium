from django.urls import path
from .views import RunDagsterJobView

urlpatterns = [
    path("jobs/<str:job_name>/run", RunDagsterJobView.as_view(), name="dagster-run-job"),
]
