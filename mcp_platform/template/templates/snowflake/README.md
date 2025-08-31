# Snowflake MCP Server

A comprehensive MCP server for Snowflake data warehouse integration, providing secure read-only access to databases, schemas, tables, and query execution capabilities with multiple authentication methods.

## Overview

This Snowflake MCP server provides:
- **Multiple Authentication Methods**: Username/password, OAuth, key pair, and browser SSO
- **Read-Only Mode**: Enforced by default for security (can be disabled with warnings)
- **Metadata Browsing**: List and explore databases, schemas, and tables
- **Query Execution**: Execute SQL queries with configurable limits
- **Advanced Filtering**: Pattern-based filtering for databases and schemas
- **FastMCP Integration**: HTTP-first approach with stdio fallback
- **Docker Deployment**: Containerized deployment with health checks

## Quick Start

### Deploy with MCP Platform

```bash
# Deploy with basic configuration
python -m mcp_platform deploy snowflake --config snowflake_account="mycompany"

# Deploy with username/password authentication
python -m mcp_platform deploy snowflake \
  --config snowflake_account="mycompany" \
  --config snowflake_user="username" \
  --config snowflake_password="password"

# Check deployment status
python -m mcp_platform status snowflake

# View logs
python -m mcp_platform logs snowflake
```

### Direct Python Usage

```python
from mcp_platform.template.templates.snowflake.server import SnowflakeMCPServer

# Create server with configuration
config = {
    "snowflake_account": "mycompany",
    "snowflake_user": "username",
    "snowflake_password": "password",
    "snowflake_warehouse": "COMPUTE_WH"
}

server = SnowflakeMCPServer(config_dict=config)
server.run()
```

## Authentication Methods

### 1. Username and Password (Default)

```bash
export SNOWFLAKE_ACCOUNT="mycompany"
export SNOWFLAKE_USER="username"
export SNOWFLAKE_PASSWORD="password"
```

### 2. OAuth Token Authentication

```bash
export SNOWFLAKE_ACCOUNT="mycompany"
export SNOWFLAKE_AUTHENTICATOR="oauth"
export SNOWFLAKE_OAUTH_TOKEN="your_oauth_token"
```

### 3. Key Pair Authentication

```bash
export SNOWFLAKE_ACCOUNT="mycompany"
export SNOWFLAKE_USER="username"
export SNOWFLAKE_AUTHENTICATOR="snowflake_jwt"
export SNOWFLAKE_PRIVATE_KEY="/path/to/private_key.pem"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="key_passphrase"
```

### 4. Browser-based SSO

```bash
export SNOWFLAKE_ACCOUNT="mycompany"
export SNOWFLAKE_USER="username"
export SNOWFLAKE_AUTHENTICATOR="externalbrowser"
```

### 5. Okta Authentication

```bash
export SNOWFLAKE_ACCOUNT="mycompany"
export SNOWFLAKE_USER="username"
export SNOWFLAKE_AUTHENTICATOR="https://mycompany.okta.com"
```

## Configuration

### Required Settings

```json
{
  "snowflake_account": "mycompany"
}
```

### Optional Configuration

```bash
# Connection Settings
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"       # Default warehouse
export SNOWFLAKE_DATABASE="MY_DATABASE"       # Default database
export SNOWFLAKE_SCHEMA="PUBLIC"              # Default schema
export SNOWFLAKE_ROLE="MY_ROLE"               # Default role

# Security Settings
export SNOWFLAKE_READ_ONLY=true               # Enable read-only mode (default: true)

# Filtering Settings
export SNOWFLAKE_DATABASE_FILTER="^(PROD|DEV)_.*"  # Database name pattern
export SNOWFLAKE_SCHEMA_FILTER="^(PUBLIC|ANALYTICS)$"  # Schema name pattern

# Timeout Settings
export SNOWFLAKE_CONNECTION_TIMEOUT=60        # Connection timeout (default: 60s)
export SNOWFLAKE_QUERY_TIMEOUT=300           # Query timeout (default: 300s)

# Logging
export MCP_LOG_LEVEL=info                     # Log level (default: info)
```

## Safety Features

### Read-Only Mode

Read-only mode is **enabled by default** for security:

```bash
# Read-only mode enforced (default)
python -m mcp_platform deploy snowflake --config read_only=true

# ⚠️ DANGEROUS: Disable read-only mode (shows warning)
python -m mcp_platform deploy snowflake --config read_only=false
```

When read-only mode is enabled, the following operations are prohibited:
- `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`
- `CREATE`, `ALTER`, `DROP`, `RENAME`
- `GRANT`, `REVOKE`
- `COPY`, `PUT`, `GET`
- `CALL`, `EXECUTE`

### Database and Schema Filtering

Restrict access to specific databases and schemas using regex patterns:

```bash
# Only allow databases starting with PROD_ or DEV_
export SNOWFLAKE_DATABASE_FILTER="^(PROD|DEV)_.*"

# Only allow PUBLIC and ANALYTICS schemas
export SNOWFLAKE_SCHEMA_FILTER="^(PUBLIC|ANALYTICS)$"
```

## Available Tools

### Metadata Browsing

1. **list_databases**: List all accessible databases
   ```python
   client.call("list_databases")
   ```

2. **list_schemas**: List schemas in a database
   ```python
   client.call("list_schemas", database_name="MY_DATABASE")
   ```

3. **list_tables**: List tables in a schema
   ```python
   client.call("list_tables", database_name="MY_DATABASE", schema_name="PUBLIC")
   ```

4. **describe_table**: Get table structure details
   ```python
   client.call("describe_table", 
              database_name="MY_DATABASE", 
              schema_name="PUBLIC", 
              table_name="MY_TABLE")
   ```

### Query Execution

5. **execute_query**: Execute SQL queries
   ```python
   client.call("execute_query", 
              sql_query="SELECT * FROM MY_DATABASE.PUBLIC.MY_TABLE", 
              limit=100)
   ```

### Connection Information

6. **get_connection_info**: Get current connection details
   ```python
   client.call("get_connection_info")
   ```

## Usage Examples

### FastMCP Client Integration

```python
from fastmcp.client import FastMCPClient

# Connect to the server
client = FastMCPClient(endpoint="http://localhost:7071")

# List databases
databases = client.call("list_databases")
print(f"Available databases: {databases['databases']}")

# List schemas in a database
schemas = client.call("list_schemas", database_name="MY_DATABASE")
print(f"Schemas: {schemas['schemas']}")

# Execute a query
result = client.call("execute_query", 
                    sql_query="SELECT COUNT(*) FROM MY_DATABASE.PUBLIC.CUSTOMERS",
                    limit=10)
print(f"Query result: {result['rows']}")
```

### cURL Examples

```bash
# List databases
curl -X POST http://localhost:7071/call \
  -H "Content-Type: application/json" \
  -d '{"method": "list_databases", "params": {}}'

# Execute a query
curl -X POST http://localhost:7071/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query", 
    "params": {
      "sql_query": "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 5",
      "limit": 5
    }
  }'
```

### Claude Desktop Integration

Add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "snowflake": {
      "command": "docker",
      "args": ["exec", "-i", "snowflake", "python", "server.py", "--transport", "stdio"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "mycompany",
        "SNOWFLAKE_USER": "username",
        "SNOWFLAKE_PASSWORD": "password"
      }
    }
  }
}
```

## Development

### Project Structure

```
templates/snowflake/
├── __init__.py          # Package initialization
├── config.py            # Configuration management
├── server.py            # Main server implementation
├── requirements.txt     # Python dependencies
├── Dockerfile           # Container configuration
├── template.json        # Template metadata
├── README.md            # This file
└── tests/               # Test suite
    ├── test_server.py   # Server tests
    ├── test_config.py   # Configuration tests
    ├── test_integration.py  # Integration tests
    └── __init__.py      # Test package init
```

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-mock

# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_server.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

### Local Development Setup

```bash
# Clone the repository
git clone https://github.com/Data-Everything/MCP-Platform.git
cd MCP-Platform/mcp_platform/template/templates/snowflake

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"

# Run the server
python server.py
```

## Security Considerations

### Authentication Security
- **Never commit credentials** to version control
- **Use environment variables** for sensitive configuration
- **Rotate credentials regularly** and use strong passwords
- **Enable MFA** on your Snowflake account when possible

### Network Security
- **Use HTTPS** in production environments
- **Restrict network access** to the MCP server
- **Enable connection encryption** in Snowflake
- **Monitor connection logs** for suspicious activity

### Access Control
- **Use principle of least privilege** for Snowflake roles
- **Enable read-only mode** unless write access is absolutely necessary
- **Use database/schema filtering** to limit accessible data
- **Regular audit** of user permissions and access patterns

## Troubleshooting

### Connection Issues

1. **Authentication failures**:
   ```bash
   # Verify credentials
   python -c "import snowflake.connector; conn = snowflake.connector.connect(
       account='your_account', user='your_user', password='your_password')"
   ```

2. **Network connectivity**:
   ```bash
   # Test basic connectivity
   curl -I https://your_account.snowflakecomputing.com
   ```

3. **Key pair authentication**:
   ```bash
   # Verify private key format
   openssl rsa -in private_key.pem -check
   ```

### Query Issues

1. **Read-only violations**:
   - Check if read-only mode is enabled
   - Verify query doesn't contain DML/DDL operations
   - Use `read_only=false` only if necessary (not recommended)

2. **Timeout issues**:
   - Increase `SNOWFLAKE_QUERY_TIMEOUT` for long-running queries
   - Optimize query performance
   - Use appropriate `LIMIT` clauses

3. **Permission errors**:
   - Verify role has necessary privileges
   - Check database/schema access permissions
   - Ensure warehouse access is granted

### Configuration Issues

1. **Invalid regex patterns**:
   ```python
   import re
   pattern = "^(PROD|DEV)_.*"
   re.compile(pattern)  # Should not raise an exception
   ```

2. **Environment variable precedence**:
   - CLI config overrides environment variables
   - Environment variables override defaults
   - Check with `get_connection_info` tool

## Performance Optimization

### Query Performance
- **Use appropriate LIMIT clauses** for large result sets
- **Leverage Snowflake's query optimization** features
- **Monitor query execution plans** in Snowflake
- **Use result caching** when appropriate

### Connection Management
- **Connection pooling** is handled automatically
- **Monitor connection timeouts** and adjust as needed
- **Use appropriate warehouse sizes** for workload
- **Enable auto-suspend and auto-resume** for warehouses

## Monitoring and Logging

### Health Checks

```bash
# Check server health
curl http://localhost:7071/health

# Check connection status
curl -X POST http://localhost:7071/call \
  -H "Content-Type: application/json" \
  -d '{"method": "get_connection_info", "params": {}}'
```

### Logging Configuration

```bash
# Enable debug logging
export MCP_LOG_LEVEL=debug

# View logs in real-time
python -m mcp_platform logs snowflake --follow
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints for all functions
- Add docstrings for public methods
- Run pre-commit hooks before submitting

## License

This project is licensed under the same terms as the MCP Platform.

## Support

For issues and questions:
- Check the [troubleshooting section](#troubleshooting)
- Review [Snowflake connector documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- Open an issue on the MCP Platform repository