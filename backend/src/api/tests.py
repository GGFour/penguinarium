"""Shim module so `manage.py test api` works with tests stored in `api/tests/`.

This re-exports tests from the subpackage to satisfy unittest discovery when
the label `api` is used, which imports a module named `tests` at the app root.
"""

# Re-export tests from the subpackage
from .tests.test_data_source_api import *  # noqa: F401,F403
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from pulling.models.data_source import DataSource


class DataSourceAPITests(APITestCase):
	def setUp(self):
		self.list_url = reverse('data-source-list')

	def test_create_and_list_data_sources(self):
		payload = {
			"name": "Test Source",
			"type": DataSource.DataSourceType.API,
			"connection_info": {"base_url": "https://example.com", "token": "x"},
		}

		# Create
		resp = self.client.post(self.list_url, payload, format='json')
		self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
		self.assertIn('data_source_id', resp.data)
		ds_id = resp.data['data_source_id']

		# List
		resp = self.client.get(self.list_url)
		self.assertEqual(resp.status_code, status.HTTP_200_OK)
		self.assertEqual(len(resp.data), 1)
		self.assertEqual(resp.data[0]['name'], 'Test Source')

		# Update (partial)
		detail_url = reverse('data-source-detail', args=[ds_id])
		resp = self.client.patch(detail_url, {"name": "Renamed"}, format='json')
		self.assertEqual(resp.status_code, status.HTTP_200_OK)
		self.assertEqual(resp.data['name'], 'Renamed')
