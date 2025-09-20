FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl libpq-dev libssl-dev libffi-dev \
    unixodbc-dev libaio1 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml ./
COPY uv.lock* . 2>/dev/null || true

RUN uv sync --frozen --no-dev || uv sync --no-dev
ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONPATH="/app/src"

COPY dagster.yaml ./dagster.yaml
COPY src ./src
COPY run_configs ./run_configs

ENV DAGSTER_HOME=/app

EXPOSE 3000

CMD ["uv", "run", "dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "dq_platform"]
