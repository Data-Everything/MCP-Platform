# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user for security
RUN groupadd -r mcpuser && useradd -r -g mcpuser mcpuser

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories for credentials and data
RUN mkdir -p /app/credentials /app/data && \
    chown -R mcpuser:mcpuser /app

# Switch to non-root user
USER mcpuser

# Expose the default port
EXPOSE 7072

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:7072/health')" || exit 1

# Default command - can be overridden
CMD ["python", "server.py"]

# Labels for metadata
LABEL maintainer="Data Everything <tooling@dataeverything.com>"
LABEL description="Production-ready BigQuery MCP server with secure authentication and access controls"
LABEL version="1.0.0"
LABEL org.opencontainers.image.title="BigQuery MCP Server"
LABEL org.opencontainers.image.description="Secure BigQuery MCP server for dataset querying and exploration"
LABEL org.opencontainers.image.vendor="Data Everything"
LABEL org.opencontainers.image.source="https://github.com/Data-Everything/MCP-Platform"
LABEL org.opencontainers.image.documentation="https://data-everything.github.io/MCP-Platform"