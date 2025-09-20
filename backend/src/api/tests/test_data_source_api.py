from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from pulling.models.data_source import DataSource
from typing import Dict, Any


class DataSourceAPITests(APITestCase):
	def setUp(self):
		self.list_url = reverse('data-source-list')

	def test_create_data_source(self):
		payload: Dict[str, Any] = {
			"name": "Test Source",
			"type": DataSource.DataSourceType.API,
			"connection_info": {"base_url": "https://example.com", "token": "x"},
		}

		resp = self.client.post(self.list_url, payload, format='json')
		self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
		self.assertIn('data_source_id', resp.data)

	def test_list_data_sources(self):
		# Ensure at least one data source exists
		payload: Dict[str, Any] = {
			"name": "Listable Source",
			"type": DataSource.DataSourceType.API,
			"connection_info": {"base_url": "https://example.com", "token": "x"},
		}
		create_resp = self.client.post(self.list_url, payload, format='json')
		self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)

		resp = self.client.get(self.list_url)
		self.assertEqual(resp.status_code, status.HTTP_200_OK)
		self.assertGreaterEqual(len(resp.data), 1)
		self.assertTrue(any(item['name'] == 'Listable Source' for item in resp.data))

	def test_partial_update_data_source(self):
		# Create a data source to update
		payload: Dict[str, Any] = {
			"name": "Updatable Source",
			"type": DataSource.DataSourceType.API,
			"connection_info": {"base_url": "https://example.com", "token": "x"},
		}
		create_resp = self.client.post(self.list_url, payload, format='json')
		self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
		ds_id = create_resp.data['data_source_id']

		detail_url = reverse('data-source-detail', args=[ds_id])
		resp = self.client.patch(detail_url, {"name": "Renamed"}, format='json')
		self.assertEqual(resp.status_code, status.HTTP_200_OK)
		self.assertEqual(resp.data['name'], 'Renamed')
