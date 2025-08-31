# BigQuery MCP Server

Production-ready BigQuery MCP server that provides secure, controlled access to Google BigQuery datasets with comprehensive authentication, access controls, and query capabilities.

## Features

- **🔐 Multiple Authentication Methods**: Service Account, OAuth2, Application Default Credentials
- **🛡️ Security First**: Read-only mode by default with explicit warnings for write operations
- **🎯 Access Control**: Configurable dataset filtering via patterns and regex
- **📊 Schema Browsing**: Comprehensive table and dataset exploration tools
- **⚡ Query Execution**: SQL query execution with safety limits and monitoring
- **🔍 Job Management**: BigQuery job status tracking and monitoring

## Quick Start

### Basic Setup

Deploy with minimal configuration using Application Default Credentials:

```bash
# Set up Google Cloud credentials first
gcloud auth application-default login

# Deploy the server
python -m mcp_platform deploy bigquery \
  --config project_id=my-gcp-project
```

### Service Account Authentication

For production environments, use service account authentication:

```bash
python -m mcp_platform deploy bigquery \
  --config project_id=my-gcp-project \
  --config auth_method=service_account \
  --config service_account_path=/path/to/service-account.json
```

### With Dataset Restrictions

Limit access to specific datasets for security:

```bash
python -m mcp_platform deploy bigquery \
  --config project_id=my-gcp-project \
  --config allowed_datasets="analytics_*,public_data,reporting_*" \
  --config read_only=true
```

## Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `GOOGLE_CLOUD_PROJECT` | Google Cloud project ID | - | Yes |
| `BIGQUERY_AUTH_METHOD` | Authentication method | `application_default` | No |
| `GOOGLE_APPLICATION_CREDENTIALS` | Service account key path | - | If using service account |
| `BIGQUERY_READ_ONLY` | Enable read-only mode | `true` | No |
| `BIGQUERY_ALLOWED_DATASETS` | Dataset access patterns | `*` | No |
| `BIGQUERY_DATASET_REGEX` | Regex for dataset filtering | - | No |
| `BIGQUERY_QUERY_TIMEOUT` | Query timeout in seconds | `300` | No |
| `BIGQUERY_MAX_RESULTS` | Max rows returned | `1000` | No |
| `MCP_LOG_LEVEL` | Logging level | `info` | No |

### Authentication Methods

#### 1. Application Default Credentials (Recommended for Development)

Uses your local Google Cloud SDK credentials:

```bash
gcloud auth application-default login
```

#### 2. Service Account (Recommended for Production)

Create and download a service account key:

```bash
# Create service account
gcloud iam service-accounts create bigquery-mcp \
  --description="BigQuery MCP Server" \
  --display-name="BigQuery MCP"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# Download key
gcloud iam service-accounts keys create service-account.json \
  --iam-account=bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com
```

#### 3. OAuth2 (Interactive)

For user-based authentication:

```bash
gcloud auth login
```

### Dataset Access Control

#### Pattern-Based Filtering

Use glob patterns to control dataset access:

```bash
# Allow specific datasets
--config allowed_datasets="analytics_prod,reporting_*,public_data"

# Allow all datasets (default)
--config allowed_datasets="*"

# Deny all datasets (for testing)
--config allowed_datasets=""
```

#### Regex-Based Filtering

For advanced filtering, use regex patterns:

```bash
# Only production datasets
--config dataset_regex="^(prod|production)_.*$"

# Exclude sensitive datasets
--config dataset_regex="^(?!.*(?:sensitive|private|internal)).*$"
```

### Security Configuration

#### Read-Only Mode (Default)

By default, the server operates in read-only mode, blocking:
- INSERT, UPDATE, DELETE operations
- CREATE, DROP, ALTER statements  
- TRUNCATE, MERGE operations
- Any data modification queries

```bash
# Explicitly enable read-only mode
--config read_only=true

# ⚠️ WARNING: Enable write mode (unsafe!)
--config read_only=false
```

When write mode is enabled, you'll see this warning:
```
⚠️  WARNING: BigQuery write mode is ENABLED! This allows data modifications and is potentially unsafe.
```

## API Reference

### Available Tools

#### `list_datasets`
List all accessible BigQuery datasets in the project.

**Parameters**: None

**Returns**: Array of datasets with metadata

**Example**:
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_datasets", "params": {}}'
```

#### `list_tables`
List tables in a specific dataset.

**Parameters**:
- `dataset_id` (string, required): Dataset ID to list tables from

**Example**:
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_tables", "params": {"dataset_id": "analytics_prod"}}'
```

#### `describe_table`
Get detailed schema information for a table.

**Parameters**:
- `dataset_id` (string, required): Dataset ID containing the table
- `table_id` (string, required): Table ID to describe

**Example**:
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "describe_table", "params": {"dataset_id": "analytics_prod", "table_id": "user_events"}}'
```

#### `execute_query`
Execute a SQL query against BigQuery.

**Parameters**:
- `query` (string, required): SQL query to execute
- `dry_run` (boolean, optional): Validate query without executing

**Example**:
```bash
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "execute_query", "params": {"query": "SELECT COUNT(*) as total FROM `my-project.analytics.events` WHERE DATE(timestamp) = CURRENT_DATE()"}}'
```

#### `get_job_status`
Get status of a BigQuery job.

**Parameters**:
- `job_id` (string, required): BigQuery job ID to check

#### `get_dataset_info`
Get detailed information about a dataset.

**Parameters**:
- `dataset_id` (string, required): Dataset ID to get information for

## Usage Examples

### Basic Queries

```bash
# Count rows in a table
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "SELECT COUNT(*) as row_count FROM `my-project.dataset.table`"
    }
  }'

# Get recent data
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query", 
    "params": {
      "query": "SELECT * FROM `my-project.logs.events` WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) LIMIT 10"
    }
  }'
```

### Schema Exploration

```bash
# List all datasets
curl -X POST http://localhost:7072/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_datasets", "params": {}}'

# Explore table structure
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

### Query Validation

```bash
# Dry run to validate query
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

## Docker Usage

### Using Pre-built Image

```bash
docker run -p 7072:7072 \
  -e GOOGLE_CLOUD_PROJECT=my-project \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account.json \
  -v /path/to/service-account.json:/creds/service-account.json:ro \
  dataeverything/mcp-bigquery:latest
```

### Building Locally

```bash
# Clone and build
git clone https://github.com/Data-Everything/MCP-Platform
cd MCP-Platform/mcp_platform/template/templates/bigquery
docker build -t mcp-bigquery .

# Run with environment variables
docker run -p 7072:7072 \
  -e GOOGLE_CLOUD_PROJECT=my-project \
  -e BIGQUERY_READ_ONLY=true \
  -e BIGQUERY_ALLOWED_DATASETS="analytics_*,public_*" \
  mcp-bigquery
```

## Client Integration

### Python Client

```python
from fastmcp.client import FastMCPClient

# Connect to server
client = FastMCPClient(endpoint='http://localhost:7072')

# List datasets
datasets = client.call('list_datasets')
print(f"Found {len(datasets['datasets'])} datasets")

# Execute query
result = client.call('execute_query', query='SELECT COUNT(*) FROM `my-project.analytics.events`')
print(f"Query returned {result['num_rows']} rows")

# Describe table schema
schema = client.call('describe_table', dataset_id='analytics', table_id='events')
print(f"Table has {len(schema['schema'])} columns")
```

### Claude Desktop Integration

Add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "bigquery": {
      "command": "docker",
      "args": [
        "run",
        "-i", 
        "--rm",
        "-e", "GOOGLE_CLOUD_PROJECT=my-project",
        "-e", "GOOGLE_APPLICATION_CREDENTIALS=/creds/service-account.json",
        "-v", "/path/to/service-account.json:/creds/service-account.json:ro",
        "dataeverything/mcp-bigquery:latest",
        "mcp-server",
        "--transport", "stdio"
      ]
    }
  }
}
```

## Security Best Practices

### 1. Use Read-Only Mode

Always run in read-only mode unless write operations are explicitly required:

```bash
--config read_only=true  # Default and recommended
```

### 2. Restrict Dataset Access

Limit access to only necessary datasets:

```bash
# Specific datasets
--config allowed_datasets="prod_analytics,public_reference"

# Pattern-based
--config allowed_datasets="prod_*,staging_analytics"

# Regex-based (advanced)
--config dataset_regex="^(prod|production)_.*$"
```

### 3. Use Service Accounts

For production, use service accounts with minimal required permissions:

```bash
# Required IAM roles
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

# Optional: for dataset creation (if write mode needed)
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:bigquery-mcp@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

### 4. Set Query Limits

Configure appropriate limits to prevent resource abuse:

```bash
--config query_timeout=300      # 5 minutes max
--config max_results=1000       # Limit result size
```

### 5. Enable Audit Logging

Monitor server access through logs:

```bash
--config log_level=info  # Log all operations
```

## Troubleshooting

### Common Issues

#### Authentication Errors

```bash
# Error: Could not automatically determine credentials
# Solution: Set up authentication
gcloud auth application-default login

# Error: Service account key not found
# Solution: Check file path and permissions
ls -la /path/to/service-account.json
```

#### Permission Errors

```bash
# Error: Access Denied: BigQuery BigQuery: Permission denied
# Solution: Grant required IAM roles
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:your-sa@project.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"
```

#### Dataset Access Issues

```bash
# Error: Access to dataset 'sensitive_data' is not allowed
# Solution: Update allowed_datasets configuration
--config allowed_datasets="analytics_*,sensitive_data"
```

#### Query Timeouts

```bash
# Error: Query timeout exceeded
# Solution: Increase timeout or optimize query
--config query_timeout=600  # 10 minutes
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
--config log_level=debug
```

## Health Check

The server provides a health check endpoint:

```bash
curl http://localhost:7072/health
```

Response includes:
- Server status
- BigQuery connection status  
- Configuration summary
- Authentication method

## Contributing

This template follows the MCP Platform contribution guidelines. See [CONTRIBUTING.md](../../../CONTRIBUTING.md) for details.

## License

This template is part of the MCP Platform and is licensed under the Elastic License 2.0.

## Support

- 📝 [Report Issues](https://github.com/Data-Everything/MCP-Platform/issues)
- 💬 [Discussion Forum](https://github.com/Data-Everything/MCP-Platform/discussions)
- 📚 [Documentation](https://data-everything.github.io/MCP-Platform)