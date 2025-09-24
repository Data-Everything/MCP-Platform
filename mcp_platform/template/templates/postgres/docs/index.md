# PostgreSQL MCP Server

Production-ready PostgreSQL MCP server for secure, controlled access to PostgreSQL databases with comprehensive authentication, SSH tunneling, SSL/TLS support, and advanced querying capabilities.

This template provides a robust interface to PostgreSQL databases, enabling AI assistants and automation tools to query data, explore schemas, and analyze database structures with enterprise-grade security features.

## Overview

The PostgreSQL MCP Server offers a comprehensive interface to PostgreSQL databases, providing secure access with configurable authentication methods, SSH tunneling, and fine-grained access controls. It's designed for production environments where data security and access governance are paramount.

### Key Features

- **🔐 Multiple Authentication Methods**: Password, certificate, peer, LDAP, RADIUS, and more
- **🛡️ Security First**: Read-only mode by default with explicit schema access controls
- **🔒 SSH Tunneling**: Secure connections through SSH tunnels with key or password authentication
- **🔏 SSL/TLS Support**: Full certificate validation, client authentication, and encrypted connections
- **🎯 Schema Access Control**: Configurable schema filtering via patterns and regex
- **📊 Schema Discovery**: Comprehensive table, index, and constraint exploration
- **⚡ Query Execution**: SQL query execution with safety limits and performance monitoring
- **🔍 Database Analytics**: Table statistics, connection monitoring, and performance insights
- **🚀 Enterprise Ready**: Docker support, health checks, and production deployment patterns

## Architecture

The PostgreSQL MCP server follows a secure, modular architecture:

1. **Authentication Layer**: Supports multiple PostgreSQL authentication methods with SSL/TLS
2. **Connection Management**: Connection pooling with SSH tunnel support for remote access
3. **Access Control Engine**: Configurable schema filtering and permission management
4. **Query Engine**: SQL execution with safety controls, read-only enforcement, and result limiting
5. **Schema Browser**: Database exploration with metadata retrieval and statistics
6. **Security Layer**: SSL certificate validation, SSH tunneling, and secure credential handling
7. **Transport Layer**: Supports HTTP and stdio protocols for flexible integration

## Quick Start

### Local Development

Perfect for local PostgreSQL development:

```bash
# Deploy the PostgreSQL MCP server locally
python -m mcp_platform deploy postgres \
  --config pg_host=localhost \
  --config pg_user=postgres \
  --config pg_password=your_password \
  --config pg_database=your_database
```

### Production with SSL

Recommended for production environments:

```bash
# Deploy with SSL and read-only access
python -m mcp_platform deploy postgres \
  --config pg_host=prod-db.company.com \
  --config pg_user=readonly_user \
  --config pg_password=$DB_PASSWORD \
  --config pg_database=analytics \
  --config ssl_mode=require \
  --config read_only=true \
  --config allowed_schemas="public,analytics,reporting"
```

### Secure Remote Access with SSH Tunnel

For accessing databases behind firewalls:

```bash
# Deploy with SSH tunnel and key authentication
python -m mcp_platform deploy postgres \
  --config pg_host=internal-db.company.local \
  --config pg_user=analyst \
  --config pg_password=$DB_PASSWORD \
  --config ssh_tunnel=true \
  --config ssh_host=bastion.company.com \
  --config ssh_user=admin \
  --config ssh_auth_method=key \
  --config ssh_key_file=~/.ssh/company_key
```

## Tool Catalog

### Schema Discovery

#### list_databases
Lists all databases on the PostgreSQL server (excludes template databases).

**Parameters**: None
**Returns**: Array of database names

```python
result = await session.call_tool("list_databases", {})
# Returns: ["postgres", "analytics", "reporting"]
```

#### list_schemas
#### list_schemas
Lists all accessible database schemas for a specified database based on configured access controls.

**Parameters**:
- `database` (string, required): Database name to list schemas from

**Returns**: Array of schema names
**Access Control**: Filtered by `allowed_schemas` configuration

```python
result = await session.call_tool("list_schemas", {"database": "analytics"})
# Returns: ["public", "analytics", "reporting", "staging"]
```

#### list_tables
Lists all tables within a specific schema.

**Parameters**:
- `schema` (string, optional): Target schema name (defaults to "public")

**Returns**: Array of table names
**Access Control**: Subject to schema filtering

```python
result = await session.call_tool("list_tables", {"schema": "analytics"})
# Returns: ["users", "orders", "products", "sessions"]
```

#### list_columns
Lists all columns for a specific table with detailed type information.

**Parameters**:
- `table` (string, required): Target table name
- `schema` (string, optional): Target schema name (defaults to "public")

**Returns**: Array of column objects with name, type, and constraints

```python
result = await session.call_tool("list_columns", {
    "table": "users",
    "schema": "public"
})
# Returns: [
#   {"name": "id", "type": "integer", "nullable": false, "default": "nextval('users_id_seq')"},
#   {"name": "email", "type": "character varying(255)", "nullable": false},
#   {"name": "created_at", "type": "timestamp with time zone", "nullable": false}
# ]
```

### Table Operations

#### describe_table
Provides comprehensive schema information for a specific table including columns, indexes, and constraints.

**Parameters**:
- `table` (string, required): Target table name
- `schema` (string, optional): Target schema name (defaults to "public")

**Returns**: Detailed table schema object with complete metadata

```python
result = await session.call_tool("describe_table", {
    "table": "orders",
    "schema": "analytics"
})
# Returns: {
#   "table_name": "orders",
#   "schema_name": "analytics",
#   "columns": [...],
#   "primary_key": ["id"],
#   "foreign_keys": [...],
#   "indexes": [...],
#   "constraints": [...],
#   "table_size": "145 MB",
#   "row_count": 45231
# }
```

#### get_table_stats
Retrieves detailed statistics for a specific table including size, row count, and performance metrics.

**Parameters**:
- `table` (string, required): Target table name
- `schema` (string, optional): Target schema name (defaults to "public")

**Returns**: Table statistics object with size and performance data

```python
result = await session.call_tool("get_table_stats", {
    "table": "orders",
    "schema": "analytics"
})
# Returns: {
#   "table_name": "orders",
#   "schema_name": "analytics",
#   "row_count": 45231,
#   "total_size": "145 MB",
#   "index_size": "23 MB",
#   "toast_size": "0 bytes",
#   "last_vacuum": "2024-01-15 10:30:00",
#   "last_analyze": "2024-01-15 10:30:00"
# }
```

### Query Execution

#### execute_query
Executes SQL queries against PostgreSQL with comprehensive access control and safety features.

**Parameters**:
- `query` (string, required): SQL query to execute
- `limit` (integer, optional): Maximum number of rows to return

**Returns**: Query results with metadata and execution information
**Access Control**: Subject to read-only mode and schema filtering

```python
# Simple data analysis query
result = await session.call_tool("execute_query", {
    "query": """
        SELECT
            DATE_TRUNC('month', created_at) as month,
            COUNT(*) as user_count,
            COUNT(DISTINCT email_domain) as unique_domains
        FROM (
            SELECT
                created_at,
                SPLIT_PART(email, '@', 2) as email_domain
            FROM users
            WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
        ) monthly_data
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month
    """,
    "limit": 12
})

# Cross-schema analytics query
result = await session.call_tool("execute_query", {
    "query": """
        SELECT
            p.category,
            COUNT(oi.id) as total_orders,
            SUM(oi.quantity * oi.price) as revenue,
            AVG(oi.quantity * oi.price) as avg_order_value
        FROM analytics.order_items oi
        JOIN public.products p ON oi.product_id = p.id
        JOIN analytics.orders o ON oi.order_id = o.id
        WHERE o.created_at >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY p.category
        ORDER BY revenue DESC
    """,
    "limit": 50
})
```

#### explain_query
Analyzes query execution plans for performance optimization.

**Parameters**:
- `query` (string, required): SQL query to analyze

**Returns**: Query execution plan with performance insights

```python
result = await session.call_tool("explain_query", {
    "query": """
        SELECT u.email, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at >= '2024-01-01'
        GROUP BY u.email
        HAVING COUNT(o.id) > 5
    """
})
# Returns: {
#   "execution_plan": [
#     "HashAggregate (cost=1234.56..1345.67 rows=100 width=32)",
#     "  Group Key: u.email",
#     "  Filter: (count(o.id) > 5)",
#     "  -> Hash Left Join (cost=123.45..234.56 rows=1000 width=24)",
#     "       Hash Cond: (u.id = o.user_id)",
#     "       -> Seq Scan on users u (cost=0.00..50.00 rows=500 width=20)",
#     "            Filter: (created_at >= '2024-01-01'::date)",
#     "       -> Hash (cost=100.00..100.00 rows=2000 width=8)",
#     "            -> Seq Scan on orders o (cost=0.00..100.00 rows=2000 width=8)"
#   ],
#   "total_cost": 1345.67,
#   "estimated_rows": 100
# }
```

### Index and Constraint Management

#### list_indexes
Lists all indexes for a specific table with detailed information.

**Parameters**:
- `table` (string, required): Target table name
- `schema` (string, optional): Target schema name (defaults to "public")

**Returns**: Array of index objects with names, columns, and properties

```python
result = await session.call_tool("list_indexes", {
    "table": "users",
    "schema": "public"
})
# Returns: [
#   {
#     "index_name": "users_pkey",
#     "columns": ["id"],
#     "is_unique": true,
#     "is_primary": true,
#     "index_type": "btree",
#     "size": "2048 kB"
#   },
#   {
#     "index_name": "idx_users_email",
#     "columns": ["email"],
#     "is_unique": true,
#     "is_primary": false,
#     "index_type": "btree",
#     "size": "1024 kB"
#   }
# ]
```

#### list_constraints
Lists all constraints for a specific table including foreign keys, check constraints, and unique constraints.

**Parameters**:
- `table` (string, required): Target table name
- `schema` (string, optional): Target schema name (defaults to "public")

**Returns**: Constraint objects organized by type

```python
result = await session.call_tool("list_constraints", {
    "table": "orders",
    "schema": "analytics"
})
# Returns: {
#   "primary_key": {
#     "name": "orders_pkey",
#     "columns": ["id"]
#   },
#   "foreign_keys": [
#     {
#       "name": "fk_orders_user_id",
#       "columns": ["user_id"],
#       "referenced_table": "users",
#       "referenced_columns": ["id"],
#       "on_delete": "CASCADE"
#     }
#   ],
#   "unique_constraints": [],
#   "check_constraints": [
#     {
#       "name": "orders_total_positive",
#       "definition": "CHECK ((total_amount > 0))"
#     }
#   ]
# }
```

### Database Management

#### test_connection
Tests the database connection and returns connection status with performance metrics.

**Parameters**: None
**Returns**: Connection status and performance information

```python
result = await session.call_tool("test_connection", {})
# Returns: {
#   "status": "success",
#   "database": "analytics",
#   "host": "prod-db.company.com",
#   "port": 5432,
#   "ssl_mode": "require",
#   "server_version": "PostgreSQL 15.4",
#   "response_time_ms": 12.5,
#   "active_connections": 5,
#   "ssh_tunnel_active": true
# }
```

#### get_database_info
Retrieves comprehensive information about the PostgreSQL database instance.

**Parameters**: None
**Returns**: Database configuration and version information

```python
result = await session.call_tool("get_database_info", {})
# Returns: {
#   "database_name": "analytics",
#   "server_version": "PostgreSQL 15.4 on x86_64-pc-linux-gnu",
#   "server_encoding": "UTF8",
#   "client_encoding": "UTF8",
#   "timezone": "UTC",
#   "total_databases": 3,
#   "total_schemas": 8,
#   "total_tables": 45,
#   "database_size": "2.3 GB",
#   "uptime": "15 days, 4:23:12"
# }
```

#### get_connection_info
Provides detailed information about the current database connection and connection pool status.

**Parameters**: None
**Returns**: Connection pool and session information

```python
result = await session.call_tool("get_connection_info", {})
# Returns: {
#   "connection_status": "active",
#   "pool_size": 5,
#   "checked_out_connections": 1,
#   "overflow_connections": 0,
#   "connection_timeout": 10,
#   "current_transaction": false,
#   "autocommit": true,
#   "isolation_level": "READ_COMMITTED"
# }
```

## Configuration Reference

### Database Connection

| Parameter | Type | Description | Default | Required |
|-----------|------|-------------|---------|----------|
| `pg_host` | string | PostgreSQL server hostname | - | ✅ |
| `pg_port` | integer | PostgreSQL server port | 5432 | ❌ |
| `pg_user` | string | Database username | - | ✅ |
| `pg_password` | string | Database password | - | ✅* |
| `pg_database` | string | Database name | postgres | ❌ |

*Required unless using certificate authentication

### SSL/TLS Configuration

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `ssl_mode` | enum | SSL connection mode | prefer |
| `ssl_cert` | string | Client certificate file path | - |
| `ssl_key` | string | Client private key file path | - |
| `ssl_ca` | string | Certificate Authority file path | - |

**SSL Modes:**
- `disable`: No SSL connection
- `allow`: Try SSL, fallback to non-SSL
- `prefer`: Try SSL, fallback to non-SSL (default)
- `require`: Require SSL connection
- `verify-ca`: Require SSL with CA verification
- `verify-full`: Require SSL with full certificate verification

### Authentication Methods

| Method | Description | Required Parameters |
|--------|-------------|-------------------|
| `password` | Standard password authentication | `pg_password` |
| `md5` | MD5 password authentication | `pg_password` |
| `scram-sha-256` | SCRAM-SHA-256 authentication | `pg_password` |
| `cert` | SSL certificate authentication | `ssl_cert`, `ssl_key` |
| `peer` | Peer authentication (Unix sockets) | - |
| `ident` | Ident authentication | - |
| `ldap` | LDAP authentication | `pg_password` |
| `radius` | RADIUS authentication | `pg_password` |

### SSH Tunneling

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `ssh_tunnel` | boolean | Enable SSH tunnel | false |
| `ssh_host` | string | SSH server hostname | - |
| `ssh_port` | integer | SSH server port | 22 |
| `ssh_user` | string | SSH username | - |
| `ssh_auth_method` | enum | SSH authentication method | password |
| `ssh_password` | string | SSH password | - |
| `ssh_key_file` | string | SSH private key file path | - |
| `ssh_key_passphrase` | string | SSH key passphrase | - |
| `ssh_local_port` | integer | Local tunnel port (0=auto) | 0 |

### Access Control

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `read_only` | boolean | Enable read-only mode | true |
| `allowed_schemas` | string | Allowed schemas (comma-separated or regex) | * |
| `max_results` | integer | Maximum query result rows | 1000 |
| `query_timeout` | integer | Query timeout in seconds | 300 |
| `connection_timeout` | integer | Connection timeout in seconds | 10 |

## Security Features

### Read-Only Mode

The server runs in read-only mode by default, blocking all write operations:

- `INSERT`, `UPDATE`, `DELETE` statements are blocked
- `CREATE`, `DROP`, `ALTER` statements are blocked
- `TRUNCATE`, `COPY` statements are blocked
- Stored procedure calls that modify data are blocked

```bash
# Enable write operations (use with caution)
--config read_only=false
```

### Schema Access Control

Configure which schemas are accessible using patterns:

```bash
# Allow specific schemas
--config allowed_schemas="public,analytics,reporting"

# Use regex pattern
--config allowed_schemas="^(public|analytics|test_.*)$"

# Allow all schemas
--config allowed_schemas="*"
```

### SSL/TLS Security

Configure comprehensive SSL security:

```bash
# Basic SSL requirement
--config ssl_mode=require

# Full certificate validation
--config ssl_mode=verify-full \
--config ssl_ca=/etc/ssl/certs/ca.crt

# Client certificate authentication
--config ssl_mode=verify-full \
--config ssl_cert=/etc/ssl/certs/client.crt \
--config ssl_key=/etc/ssl/private/client.key \
--config ssl_ca=/etc/ssl/certs/ca.crt
```

### SSH Tunnel Security

Secure remote database access:

```bash
# SSH with key authentication
--config ssh_tunnel=true \
--config ssh_host=bastion.company.com \
--config ssh_user=admin \
--config ssh_auth_method=key \
--config ssh_key_file=~/.ssh/production_key

# SSH with password authentication
--config ssh_tunnel=true \
--config ssh_host=bastion.company.com \
--config ssh_user=admin \
--config ssh_auth_method=password \
--config ssh_password=$SSH_PASSWORD
```

## Production Deployment

### Docker Deployment

```bash
# Build the Docker image
docker build -t my-postgres-mcp .

# Run with environment variables
docker run -d \
  --name postgres-mcp \
  -p 7080:7080 \
  -e PG_HOST=prod-db.company.com \
  -e PG_USER=readonly_user \
  -e PG_PASSWORD=$DB_PASSWORD \
  -e PG_DATABASE=analytics \
  -e PG_SSL_MODE=require \
  -e PG_READ_ONLY=true \
  -e PG_ALLOWED_SCHEMAS="public,analytics" \
  my-postgres-mcp
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres-mcp-server
spec:
  replicas: 2
  selector:
    matchLabels:
      app: postgres-mcp-server
  template:
    metadata:
      labels:
        app: postgres-mcp-server
    spec:
      containers:
      - name: postgres-mcp
        image: dataeverything/mcp-postgres:latest
        ports:
        - containerPort: 7080
        env:
        - name: PG_HOST
          value: "postgres-service"
        - name: PG_USER
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: username
        - name: PG_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: password
        - name: PG_DATABASE
          value: "analytics"
        - name: PG_SSL_MODE
          value: "require"
        - name: PG_READ_ONLY
          value: "true"
        livenessProbe:
          httpGet:
            path: /health
            port: 7080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 7080
          initialDelaySeconds: 5
          periodSeconds: 5
```

## Integration Examples

### FastMCP Python Client

```python
from fastmcp.client import FastMCPClient
import asyncio

async def analyze_user_growth():
    client = FastMCPClient(endpoint='http://localhost:7080')

    # Test connection
    status = await client.call('test_connection')
    print(f"Connection status: {status['status']}")

    # Analyze user growth by month
    result = await client.call('execute_query', {
        'query': '''
            SELECT
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as new_users,
                COUNT(*) FILTER (WHERE email LIKE '%@company.com') as company_users
            FROM users
            WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month
        ''',
        'limit': 12
    })

    for row in result['data']:
        print(f"Month {row['month']}: {row['new_users']} total, {row['company_users']} from company")

# Run the analysis
asyncio.run(analyze_user_growth())
```

### Node.js Integration

```javascript
const axios = require('axios');

class PostgresMCPClient {
    constructor(endpoint = 'http://localhost:7080') {
        this.endpoint = endpoint;
    }

    async call(method, params = {}) {
        const response = await axios.post(`${this.endpoint}/call`, {
            method: method,
            params: params
        });
        return response.data;
    }

  async getSchemaOverview(database = 'postgres') {
    const schemas = await this.call('list_schemas', {database});
    const overview = {};

    for (const schema of schemas.schemas) {
      const tables = await this.call('list_tables', {schema});
      overview[schema] = {
        table_count: tables.tables.length,
        tables: tables.tables
      };
    }

    return overview;
  }
}

// Usage
const client = new PostgresMCPClient();
client.getSchemaOverview()
    .then(overview => console.log('Database overview:', overview))
    .catch(console.error);
```

### cURL Examples

```bash
# Test connection
curl -X POST http://localhost:7080/call \
  -H "Content-Type: application/json" \
  -d '{"method": "test_connection", "params": {}}'

# Execute analytics query
curl -X POST http://localhost:7080/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "execute_query",
    "params": {
      "query": "SELECT COUNT(*) as total_users, MAX(created_at) as latest_signup FROM users",
      "limit": 1
    }
  }'

# Get table schema
curl -X POST http://localhost:7080/call \
  -H "Content-Type: application/json" \
  -d '{
    "method": "describe_table",
    "params": {
      "table": "users",
      "schema": "public"
    }
  }'
```

## Monitoring and Troubleshooting

### Health Checks

The server provides comprehensive health checking:

```bash
# Basic health check
curl http://localhost:7080/health

# Detailed connection info
curl -X POST http://localhost:7080/call \
  -H "Content-Type: application/json" \
  -d '{"method": "get_connection_info", "params": {}}'
```

### Common Issues

#### Connection Failures

1. **Database not accessible**
   - Verify `pg_host` and `pg_port` settings
   - Check firewall rules and network connectivity
   - Ensure PostgreSQL is running and accepting connections

2. **Authentication errors**
   - Verify `pg_user` and `pg_password` credentials
   - Check `auth_method` configuration
   - Ensure user has necessary database permissions

3. **SSL connection issues**
   - Verify SSL certificate paths and permissions
   - Check `ssl_mode` configuration
   - Ensure PostgreSQL supports SSL connections

#### SSH Tunnel Issues

1. **SSH connection failures**
   - Verify SSH host accessibility and credentials
   - Check SSH key file permissions (should be 600)
   - Ensure SSH server allows tunneling (`AllowTcpForwarding yes`)

2. **Tunnel port conflicts**
   - Use `ssh_local_port=0` for automatic port assignment
   - Check for port conflicts with other services

#### Query Execution Issues

1. **Permission denied errors**
   - Verify user has SELECT permissions on target schemas/tables
   - Check `allowed_schemas` configuration
   - Ensure read-only mode settings are appropriate

2. **Query timeouts**
   - Increase `query_timeout` for long-running queries
   - Optimize queries using `explain_query` tool
   - Check database performance and indexing

### Logging

Enable debug logging for detailed troubleshooting:

```bash
--config log_level=debug
```

## Best Practices

1. **Security**
   - Always use read-only mode in production unless write access is specifically required
   - Configure SSL/TLS for all production connections
   - Use SSH tunnels for remote database access
   - Limit schema access using `allowed_schemas`
   - Store credentials securely using environment variables

2. **Performance**
   - Set appropriate `max_results` limits to prevent large result sets
   - Use `query_timeout` to prevent runaway queries
   - Monitor connection pool usage with `get_connection_info`
   - Optimize frequently used queries based on `explain_query` results

3. **Monitoring**
   - Implement health checks in production deployments
   - Monitor connection pool metrics
   - Set up alerts for connection failures
   - Log query execution times for performance analysis

4. **Access Control**
   - Use principle of least privilege for database users
   - Regularly review and audit schema access permissions
   - Implement proper authentication methods for your environment
   - Consider using certificate-based authentication for enhanced security

This PostgreSQL MCP Server provides a robust, secure foundation for database access in AI and automation workflows. For additional support and advanced configurations, refer to the PostgreSQL documentation and the MCP Platform guides.
