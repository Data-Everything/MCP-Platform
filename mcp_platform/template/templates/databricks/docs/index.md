# Databricks MCP Server

A comprehensive MCP server that enables LLM clients to interact with Databricks SQL warehouses and clusters through the Model Context Protocol (MCP).

## Overview

The Databricks MCP Server provides secure, controlled access to your Databricks workspace, allowing AI applications to:

- Discover and interact with SQL warehouses and compute clusters
- Browse catalogs, databases, schemas, and tables
- Execute read-only SQL queries by default
- Manage access through pattern-based filtering

## Quick Start

### Using the Template System

```bash
# Deploy with Personal Access Token
python -m mcp_platform deploy databricks \
  --config workspace_host=https://dbc-12345.cloud.databricks.com \
  --config access_token=dapi1234567890abcdef

# Deploy with custom settings and filtering
python -m mcp_platform deploy databricks \
  --config workspace_host=https://dbc-12345.cloud.databricks.com \
  --config access_token=dapi1234567890abcdef \
  --config allowed_databases="analytics,reporting" \
  --config read_only=false
```

### Direct Python Usage

```python
import asyncio
from databricks import DatabricksMCPServer

async def main():
    config = {
        "workspace_host": "https://dbc-12345.cloud.databricks.com",
        "access_token": "dapi1234567890abcdef",
        "read_only": True
    }

    server = DatabricksMCPServer(config_dict=config)

    # List available warehouses
    warehouses = await server.list_warehouses()
    print(f"Found {warehouses['count']} warehouses")

    # Execute a query
    result = await server.execute_query(
        query="SELECT * FROM main.default.my_table LIMIT 10"
    )
    print(f"Query returned {result['row_count']} rows")

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration

### Required Settings

```json
{
  "workspace_host": "https://dbc-12345.cloud.databricks.com"
}
```

### Authentication Options

**Personal Access Token Authentication (Recommended):**
```bash
export DATABRICKS_TOKEN="dapi1234567890abcdef"
```

**OAuth Token Authentication:**
```bash
export DATABRICKS_OAUTH_TOKEN="oauth_token_here"
export DATABRICKS_AUTH_METHOD="oauth"
```

**Username/Password Authentication (Legacy):**
```bash
export DATABRICKS_USERNAME="user@example.com"
export DATABRICKS_PASSWORD="password123"
export DATABRICKS_AUTH_METHOD="username_password"
```

### Security Settings

**Read-Only Mode (Recommended):**
```bash
export DATABRICKS_READ_ONLY="true"  # Default
```

**Database/Schema Filtering:**
```bash
# Allow only specific databases
export DATABRICKS_ALLOWED_DATABASES="analytics,reporting,staging"

# Allow databases matching regex patterns
export DATABRICKS_ALLOWED_DATABASES="prod_.*,test_.*"

# Allow specific schemas
export DATABRICKS_ALLOWED_SCHEMAS="default,public,analytics"
```

### Performance Settings

```bash
# Connection timeout (seconds)
export DATABRICKS_TIMEOUT="30"

# Maximum rows per query
export DATABRICKS_MAX_ROWS="1000"

# Enable metadata caching
export DATABRICKS_ENABLE_CACHE="true"
```

## Available Tools

### Cluster Management

**list_clusters**
- Lists all available Databricks clusters
- No parameters required
- Returns cluster details including state, configuration, and resource info

**get_cluster_info**
- Get detailed information about a specific cluster
- Parameters: `cluster_id` (required)
- Returns comprehensive cluster configuration and status

### Warehouse Management

**list_warehouses**
- Lists all available SQL warehouses
- No parameters required
- Returns warehouse details including state, size, and configuration

**get_warehouse_info**
- Get detailed information about a specific warehouse
- Parameters: `warehouse_id` (required)
- Returns comprehensive warehouse configuration and status

### Data Discovery

**list_databases**
- Lists all accessible databases/catalogs
- Parameters: `pattern` (optional) - filter pattern for database names
- Respects `allowed_databases` configuration
- Returns catalog and schema information

**list_tables**
- Lists tables in a specific database/schema
- Parameters: `database` (required), `schema` (optional)
- Supports both `catalog.schema` and legacy `database` formats
- Respects access filtering rules

**describe_table**
- Get detailed schema information for a table
- Parameters: `database` (required), `table` (required), `schema` (optional)
- Returns column details, types, and metadata

### Query Execution

**execute_query**
- Execute SQL queries against warehouses
- Parameters: `query` (required), `warehouse_id` (optional), `limit` (optional)
- Enforces read-only mode by default
- Supports result limiting and pagination

## Security Features

### Read-Only Mode

By default, the server operates in read-only mode, preventing potentially dangerous operations:

- Blocks CREATE, INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE operations
- Can be disabled with `read_only=false` (shows warning)
- Provides query validation before execution

### Access Filtering

Control access to specific databases and schemas:

```bash
# Exact matches
DATABRICKS_ALLOWED_DATABASES="analytics,reporting"

# Regex patterns
DATABRICKS_ALLOWED_DATABASES="prod_.*,staging_.*"

# Multiple patterns
DATABRICKS_ALLOWED_SCHEMAS="default,public,analytics_.*"
```

### Credential Security

- Sensitive fields (tokens, passwords) are masked in logs
- Environment variable mapping for secure credential management
- Support for multiple authentication methods

## Error Handling

The server provides comprehensive error handling:

- Connection validation on startup
- Graceful handling of authentication failures
- Clear error messages for access violations
- Timeout handling for long-running queries

## Monitoring and Health Checks

### Health Check Endpoint

```bash
curl http://localhost:7072/health
```

Returns server status, Databricks connection info, and configuration summary.

### Logging

Configure logging levels:

```bash
export DATABRICKS_LOG_LEVEL="debug"  # debug, info, warning, error
```

## Advanced Configuration

### Performance Tuning

```json
{
  "connection_timeout": 60,
  "max_rows": 5000,
  "enable_cache": true
}
```

### Custom Filtering

```json
{
  "allowed_databases": "analytics.*,reporting.*,dev_team_.*",
  "allowed_schemas": "default,public,staging,prod"
}
```

## Troubleshooting

### Common Issues

**Authentication Failures:**
```bash
# Test credentials manually
curl -X GET https://your-workspace.cloud.databricks.com/api/2.0/clusters/list \
  -H "Authorization: Bearer $DATABRICKS_TOKEN"
```

**Connection Timeouts:**
- Increase `connection_timeout` setting
- Check network connectivity to Databricks
- Verify workspace URL is correct

**Access Denied Errors:**
- Check `allowed_databases` and `allowed_schemas` patterns
- Verify user permissions in Databricks workspace
- Review pattern regex syntax

**Query Execution Issues:**
- Ensure SQL warehouse is running
- Check warehouse permissions
- Verify query syntax and table names

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request