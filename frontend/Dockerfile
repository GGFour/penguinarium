FROM python:3.13.5-slim-bookworm

RUN addgroup --system appuser && adduser --system --group --home /home/appuser appuser
RUN mkdir /app && chown appuser:appuser /app
WORKDIR /app
COPY --chown=appuser:appuser . /app/

COPY --from=ghcr.io/astral-sh/uv:0.5.9 /uv /bin/uv
USER appuser
RUN uv sync

CMD ["uv", "run", "streamlit", "run", "dashboard.py"]