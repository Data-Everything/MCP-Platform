# PostgreSQL MCP Server

A production-ready PostgreSQL MCP (Model Context Protocol) server that provides secure database access with configurable authentication, read-only mode, SSH tunneling, and comprehensive query capabilities.

## Features

- **Secure Database Access**: Multiple authentication methods including password, SSL certificates, and SSH tunneling
- **Read-Only Mode**: Built-in protection against write operations with configurable override
- **Schema Access Control**: Configurable filtering of accessible schemas using patterns or regex
- **SSL/TLS Support**: Full SSL connection support with certificate validation
- **SSH Tunneling**: Secure connections through SSH tunnels for remote databases
- **Query Safety**: SQL parsing and validation to prevent dangerous operations
- **Connection Pooling**: Efficient database connection management
- **Comprehensive Tools**: Schema inspection, query execution, and database introspection

## Quick Start

### Basic Setup

```bash
# Deploy with minimal configuration
python -m mcp_platform deploy postgres \
  --config pg_host='localhost' \
  --config pg_user='postgres' \
  --config pg_password='your_password'
```

### With SSL

```bash
# Deploy with SSL certificate authentication
python -m mcp_platform deploy postgres \
  --config pg_host='secure-db.example.com' \
  --config pg_user='postgres' \
  --config ssl_mode='verify-full' \
  --config ssl_cert='/path/to/client.crt' \
  --config ssl_key='/path/to/client.key' \
  --config ssl_ca='/path/to/ca.crt'
```

### With SSH Tunnel

```bash
# Deploy with SSH tunnel for remote access
python -m mcp_platform deploy postgres \
  --config pg_host='internal-db.company.com' \
  --config pg_user='readonly' \
  --config ssh_tunnel=true \
  --config ssh_host='bastion.company.com' \
  --config ssh_user='admin' \
  --config ssh_key_file='/path/to/ssh_key'
```

## Configuration

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `pg_host` | PostgreSQL server hostname or IP | `localhost`, `db.example.com` |
| `pg_user` | Database username | `postgres`, `readonly_user` |
| `pg_password` | Database password | `secure_password` |

### Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pg_port` | `5432` | PostgreSQL server port |
| `pg_database` | `postgres` | Database name to connect to |
| `connection_timeout` | `10` | Connection timeout in seconds |
| `query_timeout` | `300` | Query execution timeout in seconds |

### SSL Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ssl_mode` | `prefer` | SSL connection mode: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `ssl_cert` | - | Path to SSL client certificate file |
| `ssl_key` | - | Path to SSL client private key file |
| `ssl_ca` | - | Path to SSL Certificate Authority file |

### SSH Tunnel Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ssh_tunnel` | `false` | Enable SSH tunnel for database connection |
| `ssh_host` | - | SSH server hostname or IP address |
| `ssh_port` | `22` | SSH server port number |
| `ssh_user` | - | SSH username for tunnel authentication |
| `ssh_password` | - | SSH password (for password authentication) |
| `ssh_key_file` | - | Path to SSH private key file (for key authentication) |
| `ssh_key_passphrase` | - | Passphrase for SSH private key |
| `ssh_auth_method` | `password` | SSH authentication method: `password`, `key`, `agent` |
| `ssh_local_port` | `0` | Local port for SSH tunnel (0 for auto-assign) |

### Security and Access Control

| Parameter | Default | Description |
|-----------|---------|-------------|
| `read_only` | `true` | Run server in read-only mode (blocks write operations) |
| `allowed_schemas` | `*` | Comma-separated list or regex pattern of allowed schemas |
| `max_results` | `1000` | Maximum number of rows to return from queries |
| `auth_method` | `password` | PostgreSQL authentication method |

### Environment Variables

All configuration parameters can be set via environment variables using the `PG_` prefix:

```bash
export PG_HOST=localhost
export PG_USER=postgres
export PG_PASSWORD=secret
export PG_DATABASE=analytics
export PG_READ_ONLY=false
export PG_MAX_RESULTS=5000
```

## Available Tools

### Schema Discovery

#### `list_schemas`
List all accessible database schemas for a specific database.

```json
{
  "method": "list_schemas",
  "params": {"database": "postgres"}
}
```

#### `list_tables`
List tables in a specific schema.

```json
{
  "method": "list_tables",
  "params": {
    "schema": "public"
  }
}
```

#### `describe_table`
Get detailed schema information for a table.

```json
{
  "method": "describe_table",
  "params": {
    "table": "users",
    "schema": "public"
  }
}
```

#### `list_columns`
List columns in a specific table.

```json
{
  "method": "list_columns",
  "params": {
    "table": "users",
    "schema": "public"
  }
}
```

### Query Execution

#### `execute_query`
Execute a SQL query against PostgreSQL.

```json
{
  "method": "execute_query",
  "params": {
    "query": "SELECT * FROM users WHERE active = true",
    "limit": 100
  }
}
```

#### `explain_query`
Get query execution plan for a SQL query.

```json
{
  "method": "explain_query",
  "params": {
    "query": "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
  }
}
```

### Database Information

#### `get_database_info`
Get information about the PostgreSQL database.

```json
{
  "method": "get_database_info",
  "params": {}
}
```

#### `get_table_stats`
Get statistics for a specific table.

```json
{
  "method": "get_table_stats",
  "params": {
    "table": "users",
    "schema": "public"
  }
}
```

### Indexes and Constraints

#### `list_indexes`
List indexes for a specific table.

```json
{
  "method": "list_indexes",
  "params": {
    "table": "users",
    "schema": "public"
  }
}
```

#### `list_constraints`
List constraints for a specific table.

```json
{
  "method": "list_constraints",
  "params": {
    "table": "users",
    "schema": "public"
  }
}
```

### Connection Management

#### `test_connection`
Test the database connection.

```json
{
  "method": "test_connection",
  "params": {}
}
```

#### `get_connection_info`
Get information about the current database connection.

```json
{
  "method": "get_connection_info",
  "params": {}
}
```

## Security Features

### Read-Only Mode

By default, the server operates in read-only mode, blocking all write operations:

- `INSERT`, `UPDATE`, `DELETE` statements are rejected
- `CREATE`, `DROP`, `ALTER` statements are rejected
- `TRUNCATE` statements are rejected
- Dangerous functions like `NEXTVAL`, `SETVAL` are blocked

To enable write operations:

```bash
python -m mcp_platform deploy postgres \
  --config pg_host='localhost' \
  --config pg_user='admin' \
  --config pg_password='secret' \
  --config read_only=false
```

### Schema Access Control

Control which schemas users can access:

```bash
# Allow only specific schemas
--config allowed_schemas='public,analytics,reporting'

# Use regex pattern
--config allowed_schemas='^(public|test_.*)$'

# Allow all schemas (default)
--config allowed_schemas='*'
```

### SSL/TLS Security

Configure SSL connections for encrypted communication:

```bash
# Basic SSL (server certificate validation)
--config ssl_mode='require'

# Full certificate validation
--config ssl_mode='verify-full' \
--config ssl_ca='/path/to/ca.crt'

# Client certificate authentication
--config ssl_mode='verify-full' \
--config ssl_cert='/path/to/client.crt' \
--config ssl_key='/path/to/client.key' \
--config ssl_ca='/path/to/ca.crt'
```

## SSH Tunneling

For secure connections to remote databases through bastion hosts:

### Password Authentication

```bash
python -m mcp_platform deploy postgres \
  --config pg_host='internal-db.company.com' \
  --config pg_user='postgres' \
  --config ssh_tunnel=true \
  --config ssh_host='bastion.company.com' \
  --config ssh_user='admin' \
  --config ssh_password='ssh_password'
```

### Key-Based Authentication

```bash
python -m mcp_platform deploy postgres \
  --config pg_host='internal-db.company.com' \
  --config pg_user='postgres' \
  --config ssh_tunnel=true \
  --config ssh_host='bastion.company.com' \
  --config ssh_user='admin' \
  --config ssh_auth_method='key' \
  --config ssh_key_file='/home/user/.ssh/id_rsa'
```

## Usage Examples

### Data Analysis

```python
from fastmcp.client import FastMCPClient

client = FastMCPClient(endpoint='http://localhost:7080')

# Explore database structure
schemas = client.call('list_schemas', {'database': 'postgres'})
tables = client.call('list_tables', {'schema': 'public'})

# Analyze data
user_count = client.call('execute_query', {
    'query': 'SELECT COUNT(*) as total_users FROM users'
})

# Get table statistics
stats = client.call('get_table_stats', {
    'table': 'orders',
    'schema': 'public'
})
```

### Schema Inspection

```python
# Get detailed table information
table_info = client.call('describe_table', {
    'table': 'users',
    'schema': 'public'
})

# List all indexes
indexes = client.call('list_indexes', {
    'table': 'users',
    'schema': 'public'
})

# Check constraints
constraints = client.call('list_constraints', {
    'table': 'users',
    'schema': 'public'
})
```

### Query Performance Analysis

```python
# Analyze query performance
query = """
SELECT u.name, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name
ORDER BY order_count DESC
"""

# Get execution plan
plan = client.call('explain_query', {'query': query})

# Execute query
results = client.call('execute_query', {'query': query, 'limit': 100})
```

## Error Handling

The server provides comprehensive error handling:

### Connection Errors

```json
{
  "error": "Failed to connect to database: connection refused"
}
```

### Query Errors

```json
{
  "error": "Query execution failed: syntax error at or near 'SELET'"
}
```

### Access Control Errors

```json
{
  "error": "Access denied to schema 'restricted'"
}
```

### Read-Only Mode Errors

```json
{
  "error": "Query rejected: Write operation 'INSERT' not allowed in read-only mode"
}
```

## Performance Considerations

### Connection Pooling

The server uses SQLAlchemy's connection pooling for efficient database connections:

- Connections are reused across requests
- Automatic connection health checking with `pool_pre_ping=True`
- Connection recycling every hour with `pool_recycle=3600`

### Query Limits

- Default maximum of 1000 rows per query result
- Automatic `LIMIT` clause addition for `SELECT` queries without explicit limits
- Configurable via `max_results` parameter

### Timeouts

- Connection timeout: 10 seconds (configurable)
- Query timeout: 300 seconds (configurable)
- SSH tunnel connection timeout: handled by sshtunnel library

## Troubleshooting

### Common Issues

#### Connection Refused

```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql

# Check if port is accessible
telnet localhost 5432
```

#### SSL Certificate Issues

```bash
# Verify certificate files exist and have correct permissions
ls -la /path/to/ssl/files/
chmod 600 /path/to/client.key
```

#### SSH Tunnel Issues

```bash
# Test SSH connection manually
ssh user@bastion.company.com

# Check SSH key permissions
chmod 600 /path/to/ssh_key
```

#### Permission Denied

```bash
# Check PostgreSQL user permissions
psql -h localhost -U postgres -c "\du"

# Grant necessary permissions
GRANT CONNECT ON DATABASE mydb TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
```

### Debug Mode

Enable debug logging for troubleshooting:

```bash
python -m mcp_platform deploy postgres \
  --config pg_host='localhost' \
  --config pg_user='postgres' \
  --config log_level='debug'
```

### Testing Connection

Use the built-in connection test:

```python
from fastmcp.client import FastMCPClient

client = FastMCPClient(endpoint='http://localhost:7080')
test_result = client.call('test_connection')
print(test_result)
```

## Development

### Running Tests

```bash
# Run unit tests
python -m pytest mcp_platform/template/templates/postgres/tests/test_postgres_config.py -v

# Run integration tests
python -m pytest mcp_platform/template/templates/postgres/tests/test_postgres_integration.py -v -m integration

# Run all tests
python -m pytest mcp_platform/template/templates/postgres/tests/ -v
```

### Custom Extensions

To extend the PostgreSQL server with custom tools:

```python
from mcp_platform.template.templates.postgres import PostgresMCPServer

class CustomPostgresServer(PostgresMCPServer):
    def __init__(self, config_dict=None):
        super().__init__(config_dict)
        self._register_custom_tools()

    def _register_custom_tools(self):
        @self.mcp.tool
        async def custom_analytics_query(self) -> dict:
            """Custom analytics query tool."""
            query = "SELECT DATE(created_at), COUNT(*) FROM users GROUP BY DATE(created_at)"
            return await self.execute_query(query)
```

## Contributing

See the main [CONTRIBUTING.md](../../../../CONTRIBUTING.md) for general contribution guidelines.

### PostgreSQL-Specific Guidelines

- Ensure all SQL queries are parameterized to prevent injection
- Test with multiple PostgreSQL versions (11+)
- Verify SSL and SSH tunnel functionality
- Include performance tests for large datasets
- Document any PostgreSQL-specific features used

## License

This template is part of the MCP Platform and is subject to the same license terms.
