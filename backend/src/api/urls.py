from django.urls import path, include
from rest_framework.routers import DefaultRouter

# Explicitly import from the views package to avoid ambiguity with views.py
from .views.data_source import DataSourceViewSet

router = DefaultRouter()
# Accept both with and without trailing slash
router.trailing_slash = '/?'
router.register(r'data-sources', DataSourceViewSet, basename='data-source')

# Explicit mapping for the custom detail action to avoid 404s due to trailing-slash or router nuances
tables_view = DataSourceViewSet.as_view({'get': 'tables'})

urlpatterns = [
    path('', include(router.urls)),
    path('data-sources/<int:pk>/tables', tables_view, name='data-source-tables-no-slash'),
    path('data-sources/<int:pk>/tables/', tables_view, name='data-source-tables'),
]
