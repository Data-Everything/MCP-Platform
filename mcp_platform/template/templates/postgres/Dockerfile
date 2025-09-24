# PostgreSQL MCP Server - Python FastMCP implementation
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 mcpuser && chown -R mcpuser:mcpuser /app
USER mcpuser

# Default environment variables (non-sensitive defaults only)
ENV PG_HOST="postgres" \
    PG_PORT="5432" \
    PG_USER="postgres" \
    PG_DATABASE="postgres" \
    PG_SSL_MODE="prefer" \
    PG_CONNECTION_TIMEOUT="10" \
    PG_QUERY_TIMEOUT="300" \
    PG_MAX_RESULTS="1000" \
    PG_READ_ONLY="true" \
    PG_ALLOWED_SCHEMAS="*" \
    PG_SSH_TUNNEL="false" \
    PG_SSH_PORT="22" \
    PG_SSH_LOCAL_PORT="0" \
    MCP_TRANSPORT="http" \
    MCP_HOST="0.0.0.0" \
    MCP_PORT="7080" \
    MCP_LOG_LEVEL="info"

# Expose the port
EXPOSE 7080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:${MCP_PORT}/health')" || exit 1

# Run the server
CMD ["python", "-m", "server"]
