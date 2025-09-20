from django.urls import reverse
from django.contrib.auth import get_user_model
from rest_framework import status
from rest_framework.test import APITestCase
from pulling.models.data_source import DataSource
from typing import Dict, Any


class TestDataSourceAPI(APITestCase):
	def setUp(self):
		self.list_url = reverse('data-source-list')
		# No auth required
		User = get_user_model()
		self.user = User.objects.create(username="ds@example.com", email="ds@example.com")

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

	def test_alerts_empty_list(self):
		# Create one DS and call alerts endpoint
		payload: Dict[str, Any] = {
			"name": "Alerts Source",
			"type": DataSource.DataSourceType.API,
			"connection_info": {"base_url": "https://example.com"},
		}
		create_resp = self.client.post(self.list_url, payload, format='json')
		self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
		ds_id = create_resp.data['data_source_id']

		# without trailing slash
		alerts_url = reverse('data-source-alerts-no-slash', args=[ds_id])
		resp = self.client.get(alerts_url)
		self.assertEqual(resp.status_code, status.HTTP_200_OK)
		self.assertEqual(resp.data, [])

		# with trailing slash
		alerts_url2 = reverse('data-source-alerts', args=[ds_id])
		resp2 = self.client.get(alerts_url2)
		self.assertEqual(resp2.status_code, status.HTTP_200_OK)
		self.assertEqual(resp2.data, [])

	def test_status_and_tables_and_type_filter(self):
		# Create a database type DS with tables
		create_resp = self.client.post(self.list_url, {
			"name": "DB Source",
			"type": DataSource.DataSourceType.DATABASE,
			"connection_info": {"host": "localhost"},
		}, format='json')
		self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
		ds_id = create_resp.data['data_source_id']
		# Status endpoint
		status_url = reverse('data-source-status', args=[ds_id])
		resp_status = self.client.get(status_url)
		self.assertEqual(resp_status.status_code, status.HTTP_200_OK)
		self.assertIn('datasource_id', resp_status.data)
		# Tables endpoint: initially empty list
		tables_url = reverse('data-source-tables-no-slash', args=[ds_id])
		resp_tables = self.client.get(tables_url)
		self.assertEqual(resp_tables.status_code, status.HTTP_200_OK)
		self.assertIsInstance(resp_tables.data, list)
		# Type filter: request only API type should exclude DB Source
		resp_list = self.client.get(self.list_url + '?type=api')
		self.assertEqual(resp_list.status_code, status.HTTP_200_OK)
		self.assertTrue(all(item['type'] == DataSource.DataSourceType.API for item in resp_list.data))

	def test_alerts_invalid_id_404_and_method_not_allowed(self):
		# Non-existent data source id should 404
		alerts_url = reverse('data-source-alerts', args=[999999])
		resp = self.client.get(alerts_url)
		self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
		# POST is not allowed on alerts action
		payload: Dict[str, Any] = {"x": 1}
		resp2 = self.client.post(alerts_url, payload, format='json')
		self.assertEqual(resp2.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

	def test_alerts_content_type(self):
		# Create DS to hit alerts
		payload: Dict[str, Any] = {
			"name": "CT Source",
			"type": DataSource.DataSourceType.API,
			"connection_info": {"base_url": "https://example.com"},
		}
		create_resp = self.client.post(self.list_url, payload, format='json')
		self.assertEqual(create_resp.status_code, status.HTTP_201_CREATED)
		ds_id = create_resp.data['data_source_id']
		alerts_url = reverse('data-source-alerts', args=[ds_id])
		resp = self.client.get(alerts_url)
		self.assertEqual(resp.status_code, status.HTTP_200_OK)
		self.assertTrue(str(resp['Content-Type']).startswith('application/json'))
