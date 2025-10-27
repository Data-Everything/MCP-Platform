# Databricks MCP Server

A comprehensive MCP server for interacting with Databricks SQL warehouses and clusters using FastMCP.

## Features

- **Multiple Authentication Methods**: PAT, OAuth, Username/Password
- **Read-Only Mode**: Safe operations by default with configurable override
- **Comprehensive Tools**: List clusters/warehouses, browse databases/tables, execute queries
- **Access Control**: Pattern-based filtering for databases and schemas with regex support
- **FastMCP Integration**: Built with FastMCP for optimal performance

## Quick Start

### Using Environment Variables

```bash
export DATABRICKS_HOST="https://dbc-12345678-1234.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
python server.py
```

### Using MCP Platform

```bash
python -m mcp_platform deploy databricks \
  --config workspace_host=https://dbc-12345.cloud.databricks.com \
  --config access_token=dapi1234567890abcdef
```

## Configuration

See [docs/index.md](docs/index.md) for comprehensive configuration documentation.

## Safety Features

- **Read-only by default**: Prevents accidental data modification
- **Pattern-based access control**: Restrict access to specific databases/schemas
- **Query validation**: Checks for write operations in read-only mode
- **Connection testing**: Validates credentials on startup

## Tools Available

- `list_clusters` - List all available Databricks clusters
- `list_warehouses` - List all available SQL warehouses  
- `list_databases` - List accessible databases with filtering
- `list_tables` - List tables in a specific database/schema
- `describe_table` - Get detailed table schema information
- `execute_query` - Execute SQL queries with safety checks
- `get_cluster_info` - Get detailed cluster information
- `get_warehouse_info` - Get detailed warehouse information

## License

This project is licensed under the MIT License.