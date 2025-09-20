# Dagster Data Quality Platform

This service packages the Dagster pipelines for static, AI-assisted, and agentic data quality checks. It mirrors the
reference layout provided by the user and uses `uv` for dependency management.

## Local development

```bash
uv sync
uv run dagster-webserver -m dq_platform -h 0.0.0.0 -p 3000
```

## Testing

```bash
uv run pytest
```

## Docker

```bash
docker build -t dq-dagster:latest ./dagster_app
docker run --rm -p 3000:3000 dq-dagster:latest
```
