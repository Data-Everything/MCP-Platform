# Snowflake MCP Server

The Snowflake MCP Server provides secure, configurable access to Snowflake data warehouses through the MCP (Model Context Protocol) platform.

## Overview

This template delivers a production-ready Snowflake integration with:

- **Multiple Authentication Methods**: Password, Key-Pair, OAuth, SSO, External Browser
- **Advanced Security Controls**: Read-only mode, database/schema filtering, SSL/TLS
- **Comprehensive Data Access**: Schema discovery, query execution, warehouse management
- **Enterprise Features**: Session management, connection pooling, timeout controls

## Quick Start

### Basic Deployment

```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='myuser' \
  --config password='mypassword' \
  --config warehouse='COMPUTE_WH'
```

### Key-Pair Authentication (Production)

1. Generate RSA key pair:
```bash
openssl genrsa -out snowflake_key.pem 2048
openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub
```

2. Register public key in Snowflake:
```sql
ALTER USER myuser SET RSA_PUBLIC_KEY='<public_key_content>';
```

3. Deploy with key authentication:
```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='myuser' \
  --config authenticator='jwt' \
  --config private_key_file='/path/to/snowflake_key.pem'
```

## Authentication Methods

### 1. Password Authentication
- **Use Case**: Development, testing, simple setups
- **Security**: Basic username/password
- **Configuration**: `authenticator=snowflake`, provide password

### 2. Key-Pair Authentication (Recommended)
- **Use Case**: Production environments, automated systems
- **Security**: RSA private/public key cryptography
- **Configuration**: `authenticator=jwt`, provide private key

### 3. OAuth Authentication
- **Use Case**: Applications with existing OAuth flows
- **Security**: OAuth 2.0 tokens
- **Configuration**: `authenticator=oauth`, provide token

### 4. External Browser SSO
- **Use Case**: Interactive user sessions
- **Security**: Browser-based SSO
- **Configuration**: `authenticator=externalbrowser`

### 5. Okta SSO
- **Use Case**: Organizations using Okta
- **Security**: SAML 2.0 through Okta
- **Configuration**: `authenticator=okta_endpoint`, provide Okta URL

## Security Features

### Read-Only Mode
```bash
--config read_only=true  # Default: prevents data modification
```

### Access Control
```bash
# Database filtering
--config allowed_databases="ANALYTICS,REPORTING"

# Schema filtering  
--config allowed_schemas="PUBLIC,METRICS"

# Pattern-based filtering
--config allowed_databases="PROD_*,STAGING_*"
```

### Connection Security
```bash
# SSL settings
--config insecure_mode=false  # Default: require SSL

# Timeouts
--config connection_timeout=60
--config query_timeout=3600
```

## Available Tools

### Database Discovery
- **list_databases**: Browse available databases
- **list_schemas**: Explore schemas within databases
- **list_tables**: View tables in schemas
- **describe_table**: Get detailed table structure
- **list_columns**: List column information

### Query Operations
- **execute_query**: Run SQL queries with safety controls
- **explain_query**: Analyze query execution plans
- **get_table_stats**: Retrieve table metadata and statistics

### Warehouse Management
- **get_warehouse_info**: Current warehouse details
- **list_warehouses**: Available compute resources

### Account Information
- **get_account_info**: Account and region details
- **get_current_role**: Active role information
- **list_roles**: Available roles

### Connection Management
- **test_connection**: Verify connectivity
- **get_connection_info**: Connection status and parameters

## Configuration Examples

### Analytics Workload
```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='analyst' \
  --config password='password' \
  --config database='ANALYTICS' \
  --config warehouse='ANALYTICS_WH' \
  --config read_only=true \
  --config max_results=50000 \
  --config allowed_schemas='PUBLIC,METRICS,REPORTING'
```

### Production Service
```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='service-account' \
  --config authenticator='jwt' \
  --config private_key_file='/etc/snowflake/service.pem' \
  --config allowed_databases='PROD_*' \
  --config client_session_keep_alive=true \
  --config connection_timeout=120
```

### Development Environment
```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='dev@company.com' \
  --config authenticator='externalbrowser' \
  --config database='DEV_SANDBOX' \
  --config read_only=false \
  --config max_results=1000
```

## Best Practices

### 1. Authentication
- Use key-pair authentication for production
- Implement proper key rotation policies  
- Avoid embedding credentials in code

### 2. Access Control
- Enable read-only mode by default
- Use database/schema filtering
- Apply principle of least privilege

### 3. Performance
- Set appropriate query timeouts
- Limit result set sizes
- Use appropriate warehouse sizes

### 4. Monitoring
- Enable session keep-alive for long-running processes
- Monitor connection usage
- Track query performance

## Troubleshooting

### Common Issues

**Authentication Failures**
- Verify account identifier format (`organization-account`)
- Check user exists and has proper permissions
- For key-pair auth, ensure public key is registered

**Connection Timeouts**
- Increase connection_timeout parameter
- Check network connectivity to Snowflake
- Verify firewall allows HTTPS (port 443)

**Query Failures**
- Check read-only mode settings
- Verify table/schema access permissions
- Review query syntax and complexity

**Performance Issues**
- Ensure warehouse is running and appropriately sized
- Add query result limits
- Optimize SQL queries

### Error Messages

| Error | Solution |
|-------|----------|
| "Account not found" | Check account identifier format |
| "User does not exist" | Verify username and permissions |
| "Invalid private key" | Check key format and registration |
| "Warehouse not available" | Start warehouse or adjust permissions |
| "Read-only mode" | Disable read_only for write operations |

## Advanced Configuration

### Session Parameters
```bash
--config session_parameters='{"QUERY_TIMEOUT":"7200","AUTOCOMMIT":"false","TIMEZONE":"UTC"}'
```

### SSL Configuration
```bash
--config insecure_mode=false \
--config ocsp_response_cache_filename='/tmp/ocsp_cache'
```

### Connection Pooling
```bash
--config client_session_keep_alive=true \
--config connection_timeout=180
```

## Integration Examples

### FastMCP Client
```python
from fastmcp.client import FastMCPClient

client = FastMCPClient(endpoint='http://localhost:7081')

# List databases
databases = client.call('list_databases', {})

# Execute query
result = client.call('execute_query', {
    'query': 'SELECT COUNT(*) FROM ANALYTICS.PUBLIC.USERS',
    'limit': 1000
})
```

### REST API
```bash
# Test connection
curl -X POST http://localhost:7081/call \
  -H 'Content-Type: application/json' \
  -d '{"method": "test_connection", "params": {}}'

# Execute query
curl -X POST http://localhost:7081/call \
  -H 'Content-Type: application/json' \
  -d '{"method": "execute_query", "params": {"query": "SELECT 1"}}'
```

## Development

### Local Development
1. Install dependencies: `pip install -r requirements.txt`
2. Set environment variables or create config
3. Run server: `python server.py`

### Testing
```bash
# Unit tests
pytest tests/test_snowflake_config.py

# Integration tests  
pytest tests/test_snowflake_integration.py
```

### Code Quality
```bash
# Format
black . && isort .

# Lint  
flake8 .
```

## Support

For additional help:
- Review [Snowflake Python Connector documentation](https://docs.snowflake.com/en/user-guide/python-connector.html)
- Check [MCP Platform documentation](../../../docs/)
- File issues in the [GitHub repository](https://github.com/Data-Everything/MCP-Platform)