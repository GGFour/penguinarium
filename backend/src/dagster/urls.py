from django.urls import path
from .views import RunDagsterJobView

urlpatterns = [
    path("jobs/<str:job_name>/run", RunDagsterJobView.as_view(), name="dagster-run-job"),
    # Legacy/alternate path support: /api/dagster/run/<job_name>
    path("run/<str:job_name>", RunDagsterJobView.as_view(), name="dagster-run-job-legacy"),
]
