#!/usr/bin/env sh
set -e

# Apply migrations, then start the Django development server
uv run src/manage.py migrate

exec uv run src/manage.py runserver 0.0.0.0:${DJANGO_PORT}