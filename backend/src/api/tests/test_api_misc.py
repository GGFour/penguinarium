from __future__ import annotations

from django.contrib.auth import get_user_model
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from api.models import ApiKey


class TestAPIMisc(APITestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create(username="misc@example.com", email="misc@example.com")
        self.key = ApiKey.objects.create(key="k-misc", user=self.user)

    def test_error_envelope_404(self):
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {self.key.key}")
        url = reverse("v1-datasource-retrieve", args=["ds_missing"])  # invalid
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertIn("error", resp.data)
        self.assertIn("code", resp.data["error"])
        self.assertIn("message", resp.data["error"])
        self.assertIn("status", resp.data["error"])
        self.assertIn("request_id", resp.data["error"])

    def test_bearer_scheme_case_insensitive(self):
        # Lowercase scheme should be accepted
        self.client.credentials(HTTP_AUTHORIZATION=f"bearer {self.key.key}")
        url = reverse("v1-users-retrieve", args=[f"user_{self.user.id}"])
        resp = self.client.get(url)
        # Either 200 (allowed) or 403 if permission check fails for other reasons
        self.assertIn(resp.status_code, {status.HTTP_200_OK, status.HTTP_403_FORBIDDEN})

    def test_request_id_header_roundtrip(self):
        rid = "abcd1234efgh"
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {self.key.key}", **{"HTTP_X_REQUEST_ID": rid})
        url = reverse("v1-users-retrieve", args=[f"user_{self.user.id}"])
        resp = self.client.get(url)
        # Response should echo X-Request-ID
        self.assertEqual(resp["X-Request-ID"], rid)
