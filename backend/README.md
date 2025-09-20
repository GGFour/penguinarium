# Penguinarium Backend API

## API endpoints

- Base URL: /api/

### DataSource

- List: GET /api/data-sources/
- Create: POST /api/data-sources/
	- body: { "name": "My Source", "type": "api|database|file|stream|cloud|other", "connection_info": { ... } }
- Retrieve: GET /api/data-sources/{id}/
- Update: PUT/PATCH /api/data-sources/{id}/

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
