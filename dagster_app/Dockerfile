FROM python:3.11-bookworm

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    DAGSTER_HOME=/app/.dagster_home

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY dagster_app/pyproject.toml ./pyproject.toml
# Copy lock file if present
COPY dagster_app/uv.lock ./uv.lock

COPY dagster_app /app

RUN uv sync --frozen --no-dev || uv sync --no-dev

RUN uv sync --no-dev && \
    mkdir -p ${DAGSTER_HOME}

EXPOSE 3000

CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
