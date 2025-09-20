from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import DataSourceViewSet

router = DefaultRouter()
router.register(r'data-sources', DataSourceViewSet, basename='data-source')

urlpatterns = [
    path('', include(router.urls)),
]
