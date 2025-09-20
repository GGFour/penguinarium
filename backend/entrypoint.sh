#!/bin/bash

# Start Django development server
exec uv run src/manage.py runserver 0.0.0.0:$DJANGO_PORT