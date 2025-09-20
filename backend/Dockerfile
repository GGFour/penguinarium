ARG PYTHON_VERSION=3.13
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-trixie-slim AS app
LABEL maintainer="Ivan Trufanov <ivantrufan27@gmail.com>"



ARG UID=1000
ARG GID=1000

RUN apt-get update \
  && apt-get install -y --no-install-recommends curl libpq-dev \
  && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
  && apt-get clean \
  && groupadd -g "${GID}" python \
  && useradd --create-home --no-log-init -u "${UID}" -g "${GID}" python \
  && mkdir -p /public_collected public

USER python

ARG DEBUG="false"
ENV DEBUG="${DEBUG}" \
  PYTHONUNBUFFERED="true" \
  PYTHONPATH="." \
  UV_PROJECT_ENVIRONMENT="/home/python/.local" \
  PATH="${PATH}:/home/python/.local/bin" \
  USER="python"

# COPY --chown=python:python --from=assets /app/public /public
# COPY --chown=python:python --from=app-build /home/python/.local /home/python/.local
# COPY --from=app-build /usr/local/bin/uv /usr/local/bin/uvx /usr/local/bin/
COPY --chown=python:python . .

# Normalize Windows CRLF -> LF for shell scripts (avoids exec "no such file" issues)
USER root
RUN sed -i 's/\r$//' /entrypoint.sh \
  && chmod +x /entrypoint.sh
USER python

# ENTRYPOINT ["/app/bin/docker-entrypoint-web"]
WORKDIR /
EXPOSE ${DJANGO_PORT}
RUN uv sync
ENTRYPOINT ["/entrypoint.sh"]
