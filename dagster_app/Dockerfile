###############################################################################
# Dagster application image
#
# Key points:
#  - Use Python 3.11 (Dagster 1.5.x supports this; 3.13 may not yet be stable)
#  - Install deps with uv into /opt/venv so bind-mounted /app doesn't wipe them
#  - Cache-friendly layering: copy only project metadata first, then source
#  - Non-root user for runtime security
#  - Single dependency resolution step (removed duplicate uv sync)
###############################################################################

FROM python:3.11-slim AS runtime

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    DAGSTER_HOME=/dagster_home \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /app

# System deps (kept lean). build-essential helps if wheels miss a platform wheel.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv first (could also use standalone installer; pip keeps it simple here)
RUN pip install --no-cache-dir uv

# Copy project metadata first (for dependency layer cache) plus minimal package stub
COPY pyproject.toml uv.lock ./
COPY dagster_app/__init__.py ./dagster_app/__init__.py

# Install production dependencies (no dev). Editable build requires package presence.
RUN uv sync --frozen --no-dev || uv sync --no-dev

# Copy the rest of the application source AFTER deps so code changes don't invalidate dependency layer
COPY dagster_app ./dagster_app

# Create Dagster home directory & non-root user
RUN mkdir -p "${DAGSTER_HOME}" \
    && adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser ${DAGSTER_HOME} /app /opt/venv

USER appuser

EXPOSE 3000

# Healthcheck (optional) – uncomment if you want container health integrated with compose
# HEALTHCHECK --interval=30s --timeout=10s --retries=5 CMD dagster asset list > /dev/null 2>&1 || exit 1

CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
