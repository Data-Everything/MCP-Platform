# BigQuery MCP Server

Production-ready BigQuery MCP server for secure, controlled access to Google BigQuery datasets with comprehensive authentication, access controls, and advanced querying capabilities.

This template provides a robust interface to Google BigQuery, enabling AI assistants and automation tools to query datasets, explore schemas, and analyze data with enterprise-grade security features.

## Overview

The BigQuery MCP Server offers a comprehensive interface to Google BigQuery, providing secure access to datasets with configurable authentication methods and fine-grained access controls. It's designed for production environments where data security and access governance are paramount.

### Key Features

- **🔐 Multiple Authentication Methods**: Service Account, OAuth2, Application Default Credentials
- **🛡️ Security First**: Read-only mode by default with explicit warnings for write operations
- **🎯 Advanced Access Control**: Dataset filtering via patterns, regex, and wildcards
- **📊 Schema Discovery**: Comprehensive table and dataset exploration capabilities
- **⚡ Query Execution**: SQL query execution with safety limits, dry-run validation, and monitoring
- **🔍 Job Management**: BigQuery job status tracking and monitoring
- **🚀 Enterprise Ready**: Docker support, health checks, and production deployment patterns

## Architecture

The BigQuery MCP server follows a secure, modular architecture:

1. **Authentication Layer**: Supports multiple Google Cloud authentication methods
2. **Access Control Engine**: Configurable dataset filtering and permission management
3. **Query Engine**: SQL execution with safety controls and result limiting
4. **Schema Browser**: Dataset and table exploration with metadata retrieval
5. **Job Monitor**: BigQuery job status tracking and management
6. **Transport Layer**: Supports HTTP and stdio protocols for flexible integration

## Quick Start

### Application Default Credentials (Development)

Perfect for local development and testing:

```bash
# Set up Google Cloud credentials
gcloud auth application-default login

# Deploy the BigQuery MCP server
python -m mcp_platform deploy bigquery \
  --config project_id=my-gcp-project
```

### Service Account (Production)

Recommended for production environments:

```bash
# Deploy with service account authentication
python -m mcp_platform deploy bigquery \
  --config project_id=my-gcp-project \
  --config auth_method=service_account \
  --config service_account_path=/path/to/service-account.json
```

### With Security Controls

Deploy with dataset restrictions and read-only mode:

```bash
python -m mcp_platform deploy bigquery \
  --config project_id=my-gcp-project \
  --config allowed_datasets="analytics_*,public_data" \
  --config read_only=true \
  --config max_results=500
```

## Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `GOOGLE_CLOUD_PROJECT` | Google Cloud project ID | - | Yes |
| `BIGQUERY_AUTH_METHOD` | Authentication method (`service_account`, `oauth2`, `application_default`) | `application_default` | No |
| `GOOGLE_APPLICATION_CREDENTIALS` | Service account key file path | - | If using service account |
| `BIGQUERY_READ_ONLY` | Enable read-only mode (blocks data modifications) | `true` | No |
| `BIGQUERY_ALLOWED_DATASETS` | Dataset access patterns (comma-separated) | `*` | No |
| `BIGQUERY_DATASET_REGEX` | Advanced regex filter for datasets | - | No |
| `BIGQUERY_QUERY_TIMEOUT` | Maximum query execution time (seconds) | `300` | No |
| `BIGQUERY_MAX_RESULTS` | Maximum rows returned per query | `1000` | No |
| `MCP_LOG_LEVEL` | Logging level (`debug`, `info`, `warning`, `error`) | `info` | No |

### Authentication Methods

#### 1. Application Default Credentials (Recommended for Development)

Uses your local Google Cloud SDK credentials:

```bash
# Set up ADC
gcloud auth application-default login

# Verify setup
gcloud auth application-default print-access-token
```

#### 2. Service Account (Recommended for Production)

Create and configure a service account for production use:

```bash
# Create service account
gcloud iam service-accounts create bigquery-mcp \
  --description="BigQuery MCP Server" \
  --display-name="BigQuery MCP"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# For job creation (optional)
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Download service account key
gcloud iam service-accounts keys create service-account.json \
  --iam-account=bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com
```

#### 3. OAuth2 (Interactive)

For interactive user-based authentication:

```bash
gcloud auth login
```

### Access Control Configuration

#### Pattern-Based Dataset Filtering

Control dataset access using glob patterns:

```bash
# Allow specific datasets
--config allowed_datasets="analytics_prod,reporting_*,public_data"

# Allow all analytics datasets
--config allowed_datasets="analytics_*"

# Allow multiple patterns
--config allowed_datasets="prod_*,staging_analytics,public_*"

# Deny all datasets (for testing)
--config allowed_datasets=""
```

#### Regex-Based Filtering

For advanced filtering requirements:

```bash
# Only production datasets
--config dataset_regex="^(prod|production)_.*$"

# Exclude sensitive datasets
--config dataset_regex="^(?!.*(?:sensitive|private|internal)).*$"

# Allow specific prefixes
--config dataset_regex="^(analytics|reporting|public)_.*$"
```

#### Security Configuration

The server operates in read-only mode by default, blocking all data modification operations:

```bash
# Explicitly enable read-only mode (default)
--config read_only=true

# ⚠️ WARNING: Enable write mode (requires explicit acknowledgment)
--config read_only=false
```

**Blocked Operations in Read-Only Mode:**
- INSERT, UPDATE, DELETE statements
- CREATE, DROP, ALTER operations
- TRUNCATE, MERGE operations
- Any DDL or DML statements

## Available Tools

### list_datasets

List all accessible BigQuery datasets in the project, filtered by access controls.

**Parameters:** None

**Returns:** Array of datasets with metadata including ID, friendly name, location, and creation time.

**Example:**
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_datasets", "params": {}}'
```

### list_tables

List tables in a specific dataset with metadata.

**Parameters:**
- `dataset_id` (string, required): Dataset ID to list tables from

**Returns:** Array of tables with schema information and statistics.

**Example:**
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_tables", "params": {"dataset_id": "analytics_prod"}}'
```

### describe_table

Get detailed schema information for a specific table.

**Parameters:**
- `dataset_id` (string, required): Dataset ID containing the table
- `table_id` (string, required): Table ID to describe

**Returns:** Complete table schema with column types, descriptions, and constraints.

**Example:**
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "describe_table", 
    "params": {
      "dataset_id": "analytics_prod", 
      "table_id": "user_events"
    }
  }'
```

### execute_query

Execute SQL queries against BigQuery with safety controls and result limiting.

**Parameters:**
- `query` (string, required): SQL query to execute
- `dry_run` (boolean, optional): Validate query without executing (default: false)

**Returns:** Query results with metadata, including row count and execution statistics.

**Example:**
```bash
# Execute query
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "SELECT COUNT(*) as total_events FROM `my-project.analytics.events` WHERE DATE(timestamp) = CURRENT_DATE()"
    }
  }'

# Dry run validation
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "SELECT user_id, COUNT(*) FROM `my-project.analytics.events` GROUP BY user_id",
      "dry_run": true
    }
  }'
```

### get_job_status

Monitor BigQuery job status and execution progress.

**Parameters:**
- `job_id` (string, required): BigQuery job ID to check

**Returns:** Job status, progress, and error information if applicable.

**Example:**
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "get_job_status", "params": {"job_id": "job_abc123"}}'
```

### get_dataset_info

Get comprehensive information about a dataset.

**Parameters:**
- `dataset_id` (string, required): Dataset ID to get information for

**Returns:** Dataset metadata including location, description, access time, and configuration.

**Example:**
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "get_dataset_info", "params": {"dataset_id": "analytics_prod"}}'
```

## Usage Examples

### Data Exploration

```bash
# List all accessible datasets
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_datasets", "params": {}}'

# Explore a specific dataset
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_tables", "params": {"dataset_id": "analytics"}}'

# Get table schema
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "describe_table",
    "params": {
      "dataset_id": "analytics",
      "table_id": "user_events"
    }
  }'
```

### Analytics Queries

```bash
# Daily active users
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "SELECT DATE(timestamp) as date, COUNT(DISTINCT user_id) as dau FROM `my-project.analytics.events` WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) GROUP BY DATE(timestamp) ORDER BY date"
    }
  }'

# Top events by volume
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "SELECT event_name, COUNT(*) as event_count FROM `my-project.analytics.events` WHERE DATE(timestamp) = CURRENT_DATE() GROUP BY event_name ORDER BY event_count DESC LIMIT 10"
    }
  }'
```

### Query Validation

```bash
# Validate complex query before execution
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "WITH user_metrics AS (SELECT user_id, COUNT(*) as session_count, AVG(session_duration) as avg_duration FROM `my-project.analytics.sessions` GROUP BY user_id) SELECT * FROM user_metrics WHERE session_count > 10",
      "dry_run": true
    }
  }'
```

## Transport Modes

### HTTP Transport (Default)

Perfect for web applications and API integration:

```bash
# Start HTTP server
python -m mcp_platform deploy bigquery \
  --config project_id=my-project \
  --config mcp_transport=http \
  --config mcp_port=7072

# Access via HTTP
curl http://localhost:7072/health
```

### Stdio Transport

Ideal for Claude Desktop and direct MCP clients:

```bash
# Run in stdio mode
python server.py --project-id my-project --transport stdio

# Docker stdio mode
docker run -i --rm \
  -e GOOGLE_CLOUD_PROJECT=my-project \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account.json \
  -v /path/to/service-account.json:/creds/service-account.json:ro \
  dataeverything/mcp-bigquery:latest
```

## Integration Examples

### Claude Desktop

Add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "bigquery": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-e", "GOOGLE_CLOUD_PROJECT=my-project",
        "-e", "GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account.json",
        "-v", "/path/to/service-account.json:/creds/service-account.json:ro",
        "dataeverything/mcp-bigquery:latest"
      ]
    }
  }
}
```

### Python Client

```python
from fastmcp.client import FastMCPClient
import asyncio

async def bigquery_analytics():
    # Connect to BigQuery MCP server
    client = FastMCPClient(endpoint='http://localhost:7072')
    
    # List available datasets
    datasets = await client.call('list_datasets')
    print(f"Found {len(datasets['datasets'])} datasets")
    
    # Explore analytics dataset
    tables = await client.call('list_tables', dataset_id='analytics')
    print(f"Analytics dataset has {len(tables['tables'])} tables")
    
    # Get daily active users
    dau_query = """
    SELECT 
        DATE(timestamp) as date,
        COUNT(DISTINCT user_id) as daily_active_users
    FROM `my-project.analytics.events`
    WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY DATE(timestamp)
    ORDER BY date
    """
    
    result = await client.call('execute_query', query=dau_query)
    print(f"DAU analysis returned {result['num_rows']} days of data")
    
    # Analyze table schema
    schema = await client.call('describe_table', 
                              dataset_id='analytics', 
                              table_id='events')
    print(f"Events table has {len(schema['schema'])} columns")
    
    await client.close()

# Run the analytics
asyncio.run(bigquery_analytics())
```

### JavaScript/Node.js Client

```javascript
const axios = require('axios');

class BigQueryMCPClient {
    constructor(endpoint = 'http://localhost:7072') {
        this.endpoint = endpoint;
    }

    async call(method, params = {}) {
        const response = await axios.post(`${this.endpoint}/call`, {
            method,
            params
        });
        return response.data;
    }

    async getDatasetOverview(datasetId) {
        const [tables, info] = await Promise.all([
            this.call('list_tables', { dataset_id: datasetId }),
            this.call('get_dataset_info', { dataset_id: datasetId })
        ]);

        return {
            info,
            tables: tables.tables,
            tableCount: tables.tables.length
        };
    }

    async runAnalyticsQuery(query) {
        // Validate first
        await this.call('execute_query', { query, dry_run: true });
        
        // Execute
        return await this.call('execute_query', { query });
    }
}

// Usage example
async function analyzeUserBehavior() {
    const client = new BigQueryMCPClient();
    
    const query = `
        SELECT 
            event_name,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users
        FROM \`my-project.analytics.events\`
        WHERE DATE(timestamp) = CURRENT_DATE()
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT 10
    `;
    
    const result = await client.runAnalyticsQuery(query);
    console.log('Top events today:', result.rows);
}
```

## Docker Deployment

### Using Pre-built Image

```bash
# Basic deployment
docker run -p 7072:7072 \
  -e GOOGLE_CLOUD_PROJECT=my-project \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account.json \
  -v /path/to/service-account.json:/creds/service-account.json:ro \
  dataeverything/mcp-bigquery:latest

# With security controls
docker run -p 7072:7072 \
  -e GOOGLE_CLOUD_PROJECT=my-project \
  -e BIGQUERY_READ_ONLY=true \
  -e BIGQUERY_ALLOWED_DATASETS="analytics_*,public_*" \
  -e BIGQUERY_MAX_RESULTS=500 \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account.json \
  -v /path/to/service-account.json:/creds/service-account.json:ro \
  dataeverything/mcp-bigquery:latest
```

### Docker Compose

```yaml
version: '3.8'
services:
  bigquery-mcp:
    image: dataeverything/mcp-bigquery:latest
    ports:
      - "7072:7072"
    environment:
      GOOGLE_CLOUD_PROJECT: my-project
      BIGQUERY_AUTH_METHOD: service_account
      GOOGLE_APPLICATION_CREDENTIALS: /creds/service-account.json
      BIGQUERY_READ_ONLY: true
      BIGQUERY_ALLOWED_DATASETS: "analytics_*,public_data"
      BIGQUERY_QUERY_TIMEOUT: 300
      BIGQUERY_MAX_RESULTS: 1000
      MCP_LOG_LEVEL: info
    volumes:
      - ./service-account.json:/creds/service-account.json:ro
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7072/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bigquery-mcp
  labels:
    app: bigquery-mcp
spec:
  replicas: 2
  selector:
    matchLabels:
      app: bigquery-mcp
  template:
    metadata:
      labels:
        app: bigquery-mcp
    spec:
      containers:
      - name: bigquery-mcp
        image: dataeverything/mcp-bigquery:latest
        ports:
        - containerPort: 7072
        env:
        - name: GOOGLE_CLOUD_PROJECT
          value: "my-project"
        - name: BIGQUERY_AUTH_METHOD
          value: "service_account"
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: "/creds/service-account.json"
        - name: BIGQUERY_READ_ONLY
          value: "true"
        - name: BIGQUERY_ALLOWED_DATASETS
          value: "analytics_*,public_*"
        volumeMounts:
        - name: service-account
          mountPath: /creds
          readOnly: true
        livenessProbe:
          httpGet:
            path: /health
            port: 7072
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 7072
          initialDelaySeconds: 10
          periodSeconds: 10
      volumes:
      - name: service-account
        secret:
          secretName: bigquery-service-account
---
apiVersion: v1
kind: Service
metadata:
  name: bigquery-mcp-service
spec:
  selector:
    app: bigquery-mcp
  ports:
  - port: 7072
    targetPort: 7072
  type: ClusterIP
```

## Security Best Practices

### 1. Use Read-Only Mode

Always operate in read-only mode unless write operations are explicitly required:

```bash
# Default and recommended
--config read_only=true

# Only enable writes when absolutely necessary
--config read_only=false  # ⚠️ Use with caution
```

### 2. Implement Dataset Access Controls

Restrict access to only necessary datasets:

```bash
# Specific datasets only
--config allowed_datasets="prod_analytics,public_reference"

# Pattern-based restrictions
--config allowed_datasets="prod_*,staging_analytics"

# Advanced regex filtering
--config dataset_regex="^(prod|production)_.*$"
```

### 3. Use Service Accounts in Production

Configure service accounts with minimal required permissions:

```bash
# Required IAM roles for read operations
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# For job creation and monitoring
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Optional: For specific dataset access
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

### 4. Configure Query Limits

Set appropriate limits to prevent resource abuse:

```bash
# Limit query execution time
--config query_timeout=300  # 5 minutes

# Limit result size
--config max_results=1000   # Maximum 1000 rows

# Combined security configuration
--config query_timeout=180 --config max_results=500 --config read_only=true
```

### 5. Enable Monitoring and Logging

Configure comprehensive logging for security monitoring:

```bash
# Enable detailed logging
--config log_level=info

# For debugging (contains sensitive information)
--config log_level=debug  # Use only during troubleshooting
```

### 6. Network Security

Implement network-level security controls:

```bash
# Bind to localhost only (for local deployment)
docker run -p 127.0.0.1:7072:7072 dataeverything/mcp-bigquery

# Use reverse proxy with authentication
# nginx, Traefik, or cloud load balancer with auth
```

## Troubleshooting

### Authentication Issues

#### Could not automatically determine credentials

```bash
# Solution 1: Set up Application Default Credentials
gcloud auth application-default login

# Solution 2: Verify service account file
ls -la /path/to/service-account.json
cat /path/to/service-account.json | jq .type  # Should return "service_account"

# Solution 3: Check environment variables
echo $GOOGLE_APPLICATION_CREDENTIALS
echo $GOOGLE_CLOUD_PROJECT
```

#### Permission denied errors

```bash
# Check current authentication
gcloud auth list

# Verify project access
gcloud projects describe $GOOGLE_CLOUD_PROJECT

# Test BigQuery access
bq ls --project_id=$GOOGLE_CLOUD_PROJECT

# Grant required permissions
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member="user:your-email@domain.com" \
  --role="roles/bigquery.user"
```

### Dataset Access Issues

#### Access to dataset 'dataset_name' is not allowed

```bash
# Check current configuration
curl http://localhost:7072/health

# Update allowed datasets
--config allowed_datasets="dataset_name,other_dataset"

# Use wildcard patterns
--config allowed_datasets="analytics_*,dataset_name"

# Check dataset exists and you have access
bq ls --project_id=$GOOGLE_CLOUD_PROJECT dataset_name
```

### Query Execution Issues

#### Query timeout exceeded

```bash
# Increase timeout
--config query_timeout=600  # 10 minutes

# Optimize query (add LIMIT clause)
SELECT * FROM large_table LIMIT 1000

# Use dry_run to validate without executing
{"method": "execute_query", "params": {"query": "...", "dry_run": true}}
```

#### Result size exceeded maximum

```bash
# Increase max_results
--config max_results=5000

# Add LIMIT to query
SELECT * FROM table ORDER BY timestamp DESC LIMIT 1000

# Use pagination in application logic
```

### Container Issues

#### Container won't start

```bash
# Check Docker logs
docker logs container_id

# Verify environment variables
docker run --rm dataeverything/mcp-bigquery env | grep BIGQUERY

# Test with minimal configuration
docker run --rm -e GOOGLE_CLOUD_PROJECT=test dataeverything/mcp-bigquery
```

#### Health check failures

```bash
# Check health endpoint
curl -v http://localhost:7072/health

# Verify port binding
netstat -tulpn | grep 7072

# Check container status
docker ps
docker inspect container_id
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
# Enable debug logging
--config log_level=debug

# Check server logs
docker logs -f container_id

# Test individual operations
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_datasets", "params": {}}' | jq .
```

## Performance Optimization

### Query Performance

1. **Use appropriate LIMIT clauses** to restrict result sizes
2. **Leverage BigQuery partitioning** in your queries
3. **Use dry_run** to validate complex queries before execution
4. **Monitor query costs** with BigQuery console

### Connection Management

1. **Use persistent connections** with keep-alive settings
2. **Implement connection pooling** in client applications
3. **Configure appropriate timeouts** based on query complexity

### Memory Management

1. **Set reasonable max_results** limits to control memory usage
2. **Monitor container memory** in production deployments
3. **Use streaming results** for large datasets when possible

## Health Monitoring

The server provides comprehensive health check endpoints:

```bash
# Basic health check
curl http://localhost:7072/health

# Detailed status (includes authentication and configuration)
curl http://localhost:7072/health | jq .
```

**Health Check Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "bigquery_connection": "connected",
  "authentication_method": "service_account",
  "project_id": "my-project",
  "read_only_mode": true,
  "allowed_datasets": ["analytics_*", "public_*"],
  "server_info": {
    "version": "1.0.0",
    "transport": "http",
    "port": 7072
  }
}
```

## Contributing

This template follows the MCP Platform contribution guidelines:

1. **Code Quality**: Follow PEP 8 and use type hints
2. **Testing**: Add comprehensive tests for new features
3. **Documentation**: Update both README.md and this user guide
4. **Security**: Consider security implications of all changes

See [CONTRIBUTING.md](../../../../CONTRIBUTING.md) for detailed guidelines.

## Support

- **Template Issues**: [MCP Platform Repository](https://github.com/Data-Everything/MCP-Platform)
- **Feature Requests**: [GitHub Discussions](https://github.com/Data-Everything/MCP-Platform/discussions)
- **Documentation**: [MCP Platform Docs](https://data-everything.github.io/MCP-Platform)
- **Community**: [Discord Server](https://discord.gg/mcp-platform)

## License

This template is part of the MCP Platform and is licensed under the Elastic License 2.0. See [LICENSE](../../../../LICENSE) for details.