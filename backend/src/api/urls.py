from django.urls import path, include
from rest_framework.routers import DefaultRouter

# Explicitly import from the views package to avoid ambiguity with views.py
from .views.data_source import DataSourceViewSet

router = DefaultRouter()
router.register(r'data-sources', DataSourceViewSet, basename='data-source')

urlpatterns = [
    path('', include(router.urls)),
]
