# Penguinarium Backend API

## API endpoints

- Base URL: /api/

### DataSource

- List: GET /api/data-sources/
- Create: POST /api/data-sources/
  - body: { "name": "My Source", "type": "api|database|file|stream|cloud|other", "connection_info": { ... } }
- Retrieve: GET /api/data-sources/{id}/
- Update: PUT/PATCH /api/data-sources/{id}/
- Status: GET /api/data-sources/{id}/status

### v1 Endpoints (API keys)

- Users: POST /api/v1/users, GET /api/v1/users/{user_id}
- User data sources: GET /api/v1/users/{user_id}/datasources
- Data source: GET /api/v1/datasources/{datasource_id}
- Data source status: GET /api/v1/datasources/{datasource_id}/status
- Data source alerts: GET /api/v1/datasources/{datasource_id}/alerts
- Data source tables: GET /api/v1/datasources/{datasource_id}/tables

## Run locally (uses uv)

1. Run checks

```powershell
uv run python src\manage.py check
```

1. Run tests

```powershell
uv run python src\manage.py test -v 2
```

1. Start dev server

```powershell
uv run python src\manage.py runserver 8000
```

## Examples

Create a data source (PowerShell):

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/data-sources/ -ContentType 'application/json' -Body '{"name":"Demo","type":"api","connection_info":{"base_url":"https://example.com"}}'
```
