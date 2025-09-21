#!/usr/bin/env bash
set -euo pipefail

uvicorn fakesnow.server:app --host 0.0.0.0 --port "${SNOWFLAKE_PORT}" --log-level info &
server_pid=$!

cleanup() {
    kill "${server_pid}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

python /bootstrap.py

wait "${server_pid}"
