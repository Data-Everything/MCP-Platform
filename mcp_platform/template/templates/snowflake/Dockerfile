FROM python:3.12-slim

LABEL maintainer="Data Everything <support@dataeverything.ai>"
LABEL description="Snowflake MCP Server - Production-ready Snowflake data warehouse access"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 snowflake && chown -R snowflake:snowflake /app
USER snowflake

# Expose port
EXPOSE 7081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:7081/health || exit 1

# Run the server
CMD ["python", "server.py"]