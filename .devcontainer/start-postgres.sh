#!/usr/bin/env bash
set -euo pipefail

# This script will start the optional Postgres service using the compose file
# It expects the host docker daemon to be available via /var/run/docker.sock

COMPOSE_FILE="/workspace/.devcontainer/docker-compose.postgres.yml"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI is not available in this container"
  exit 1
fi

echo "Starting optional Postgres via docker compose..."
docker compose -f "$COMPOSE_FILE" up -d
echo "Postgres should be running (postgres:15) mapped to host port 5432"
