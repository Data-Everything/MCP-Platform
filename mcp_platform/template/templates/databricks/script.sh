#!/bin/bash
# Databricks MCP Server startup script

set -e

# Default values
TRANSPORT="${MCP_TRANSPORT:-stdio}"
HOST="${MCP_HOST:-0.0.0.0}"
PORT="${MCP_PORT:-7072}"

# Check required environment variables
if [ -z "$DATABRICKS_HOST" ]; then
    echo "Error: DATABRICKS_HOST environment variable is required"
    exit 1
fi

# Check authentication
if [ -z "$DATABRICKS_TOKEN" ] && [ -z "$DATABRICKS_OAUTH_TOKEN" ] && [ -z "$DATABRICKS_USERNAME" ]; then
    echo "Error: One of DATABRICKS_TOKEN, DATABRICKS_OAUTH_TOKEN, or DATABRICKS_USERNAME must be set"
    exit 1
fi

# Username/password authentication requires both username and password
if [ -n "$DATABRICKS_USERNAME" ] && [ -z "$DATABRICKS_PASSWORD" ]; then
    echo "Error: DATABRICKS_PASSWORD is required when using username authentication"
    exit 1
fi

# Warning for write mode
if [ "$DATABRICKS_READ_ONLY" = "false" ]; then
    echo "⚠️  WARNING: Read-only mode is disabled. This allows write operations!"
    echo "⚠️  Use with extreme caution to avoid accidental data modification."
fi

echo "Starting Databricks MCP Server..."
echo "Transport: $TRANSPORT"
echo "Host: $DATABRICKS_HOST"
echo "Read-only mode: ${DATABRICKS_READ_ONLY:-true}"

# Start the server
exec python server.py