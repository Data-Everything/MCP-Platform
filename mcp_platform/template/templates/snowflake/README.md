# Snowflake MCP Server

A production-ready Snowflake MCP server that provides secure access to Snowflake data warehouses with comprehensive authentication support, configurable security controls, and powerful query capabilities.

## Features

### 🔐 Multiple Authentication Methods
- **Password Authentication**: Traditional username/password
- **Key-Pair Authentication**: RSA private key authentication  
- **OAuth**: OAuth 2.0 token-based authentication
- **SSO Integration**: External browser and Okta SSO support
- **JWT**: JSON Web Token authentication

### 🛡️ Security & Access Control
- **Read-Only Mode**: Prevent accidental data modifications (enabled by default)
- **Database Filtering**: Restrict access to specific databases
- **Schema Filtering**: Control schema-level access
- **Query Validation**: SQL parsing and write operation detection
- **SSL/TLS Support**: Secure connections with certificate validation

### 📊 Comprehensive Data Access
- **Schema Discovery**: Browse databases, schemas, and tables
- **Table Inspection**: Detailed column information and constraints
- **Query Execution**: Run SQL queries with result formatting
- **Query Planning**: Analyze execution plans for optimization
- **Warehouse Management**: Monitor and manage compute resources

### ⚙️ Advanced Configuration
- **Session Parameters**: Custom session-level configurations
- **Connection Pooling**: Efficient connection management
- **Timeout Controls**: Configurable connection and query timeouts
- **Result Limiting**: Control maximum rows returned
- **Keep-Alive Support**: Prevent session timeouts

## Quick Start

### Basic Password Authentication

```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='myuser' \
  --config password='mypassword' \
  --config warehouse='COMPUTE_WH'
```

### Key-Pair Authentication (Recommended for Production)

```bash
# Generate RSA key pair
openssl genrsa -out snowflake_key.pem 2048
openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub

# Configure user with public key in Snowflake
# ALTER USER myuser SET RSA_PUBLIC_KEY='<public_key_content>';

python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='myuser' \
  --config authenticator='jwt' \
  --config private_key_file='/path/to/snowflake_key.pem' \
  --config warehouse='COMPUTE_WH'
```

### SSO with External Browser

```bash
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='myuser' \
  --config authenticator='externalbrowser' \
  --config warehouse='COMPUTE_WH'
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Account identifier | - |
| `SNOWFLAKE_USER` | Username | - |
| `SNOWFLAKE_PASSWORD` | Password (for password auth) | - |
| `SNOWFLAKE_AUTHENTICATOR` | Authentication method | `snowflake` |
| `SNOWFLAKE_PRIVATE_KEY` | Private key content (for JWT) | - |
| `SNOWFLAKE_PRIVATE_KEY_FILE` | Private key file path | - |
| `SNOWFLAKE_OAUTH_TOKEN` | OAuth access token | - |
| `SNOWFLAKE_OKTA_ENDPOINT` | Okta endpoint URL | - |
| `SNOWFLAKE_DATABASE` | Default database | - |
| `SNOWFLAKE_SCHEMA` | Default schema | `PUBLIC` |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse | - |
| `SNOWFLAKE_ROLE` | Role to assume | - |
| `SNOWFLAKE_REGION` | Region (if not in account) | - |
| `SNOWFLAKE_READ_ONLY` | Enable read-only mode | `true` |
| `SNOWFLAKE_MAX_RESULTS` | Max query results | `10000` |
| `SNOWFLAKE_CONNECTION_TIMEOUT` | Connection timeout (seconds) | `60` |
| `SNOWFLAKE_QUERY_TIMEOUT` | Query timeout (seconds) | `3600` |
| `SNOWFLAKE_ALLOWED_DATABASES` | Database access filter | `*` |
| `SNOWFLAKE_ALLOWED_SCHEMAS` | Schema access filter | `*` |

### Authentication Methods

#### 1. Password Authentication (Default)
```json
{
  "account": "myorg-account123",
  "user": "myuser",
  "password": "mypassword",
  "authenticator": "snowflake"
}
```

#### 2. Key-Pair Authentication
```json
{
  "account": "myorg-account123", 
  "user": "myuser",
  "authenticator": "jwt",
  "private_key_file": "/path/to/private_key.pem"
}
```

Or with inline private key:
```json
{
  "account": "myorg-account123",
  "user": "myuser", 
  "authenticator": "jwt",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
}
```

#### 3. OAuth Authentication
```json
{
  "account": "myorg-account123",
  "user": "myuser",
  "authenticator": "oauth", 
  "oauth_token": "your_oauth_token"
}
```

#### 4. External Browser SSO
```json
{
  "account": "myorg-account123",
  "user": "myuser",
  "authenticator": "externalbrowser"
}
```

#### 5. Okta SSO
```json
{
  "account": "myorg-account123",
  "user": "myuser@company.com",
  "authenticator": "okta_endpoint",
  "okta_endpoint": "https://company.okta.com"
}
```

### Access Control Configuration

#### Database Restrictions
```bash
# Allow specific databases
--config allowed_databases="ANALYTICS,REPORTING"

# Allow pattern-based access
--config allowed_databases="PROD_*,STAGING_ANALYTICS"

# Use regex patterns
--config allowed_databases="^(PROD|STAGING)_.*$"
```

#### Schema Restrictions
```bash
# Allow specific schemas
--config allowed_schemas="PUBLIC,ANALYTICS"

# Pattern-based schema access
--config allowed_schemas="*_PUBLIC,SHARED_*"
```

### Session Parameters
```bash
# Configure session parameters
--config session_parameters='{"QUERY_TIMEOUT":"7200","AUTOCOMMIT":"false"}'
```

## Available Tools

### Database Discovery
- `list_databases`: List all accessible databases
- `list_schemas`: List schemas in a database
- `list_tables`: List tables in a schema
- `describe_table`: Get detailed table schema
- `list_columns`: List columns in a table

### Query Operations
- `execute_query`: Run SQL queries (with read-only enforcement)
- `explain_query`: Get query execution plans
- `get_table_stats`: Get table statistics and metadata

### Warehouse Management
- `get_warehouse_info`: Get current warehouse information  
- `list_warehouses`: List available warehouses

### Account & Role Management
- `get_account_info`: Get account information
- `get_current_role`: Get current role
- `list_roles`: List available roles

### Connection Management  
- `test_connection`: Test database connectivity
- `get_connection_info`: Get connection details

## Security Best Practices

### 1. Use Read-Only Mode
Always operate in read-only mode unless write operations are explicitly required:
```bash
--config read_only=true  # Default and recommended
```

### 2. Implement Access Controls
Restrict access to only necessary databases and schemas:
```bash
--config allowed_databases="ANALYTICS_PROD,REPORTING" \
--config allowed_schemas="PUBLIC,METRICS"
```

### 3. Use Key-Pair Authentication
For production environments, use key-pair authentication instead of passwords:
```bash
--config authenticator=jwt \
--config private_key_file=/secure/path/snowflake_key.pem
```

### 4. Set Appropriate Timeouts
Configure reasonable timeouts to prevent resource abuse:
```bash
--config connection_timeout=120 \
--config query_timeout=1800 \
--config max_results=5000
```

### 5. Enable Session Keep-Alive
For long-running applications, enable keep-alive:
```bash
--config client_session_keep_alive=true
```

## Examples

### Data Analysis Workflow
```bash
# Deploy with analytics focus
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='analyst' \
  --config password='password' \
  --config database='ANALYTICS' \
  --config warehouse='ANALYTICS_WH' \
  --config read_only=true \
  --config max_results=50000
```

### Production Data Access
```bash
# Production deployment with key-pair auth
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='service-account' \
  --config authenticator='jwt' \
  --config private_key_file='/etc/snowflake/service.pem' \
  --config allowed_databases='PROD_*' \
  --config read_only=true \
  --config client_session_keep_alive=true
```

### Development Environment
```bash
# Development with external browser SSO
python -m mcp_platform deploy snowflake \
  --config account='myorg-account123' \
  --config user='developer@company.com' \
  --config authenticator='externalbrowser' \
  --config database='DEV_DB' \
  --config read_only=false \
  --config max_results=1000
```

## Troubleshooting

### Authentication Issues

1. **Invalid Account**: Verify account identifier format
   - Use format: `organization-account` or `account.region.cloud`
   - Check with: `SELECT CURRENT_ACCOUNT()`

2. **Key-Pair Authentication**:
   - Verify private key format (PEM)
   - Check public key is registered: `DESC USER myuser`
   - Ensure key matches registered public key

3. **SSO Issues**:
   - Verify Okta endpoint URL
   - Check browser allows popups for external browser auth
   - Confirm user exists in identity provider

### Connection Issues

1. **Network Timeouts**: Increase connection timeout
2. **Firewall**: Ensure outbound HTTPS (443) access
3. **Region Issues**: Specify region if not in account identifier

### Performance Issues

1. **Query Timeouts**: Increase query timeout or optimize queries
2. **Large Results**: Reduce max_results or add LIMIT clauses
3. **Warehouse Issues**: Check warehouse is running and sized appropriately

### Common Error Messages

- `Account not found`: Check account identifier format
- `User does not exist`: Verify username and account
- `Invalid private key`: Check key format and registration
- `Warehouse not available`: Start warehouse or check permissions
- `Read-only mode`: Disable read_only for write operations

## Development

### Running Tests
```bash
# Unit tests
pytest mcp_platform/template/templates/snowflake/tests/test_snowflake_config.py -v

# Integration tests (requires mocking)
pytest mcp_platform/template/templates/snowflake/tests/test_snowflake_integration.py -v
```

### Code Quality
```bash
# Format code
black mcp_platform/template/templates/snowflake/
isort mcp_platform/template/templates/snowflake/

# Lint
flake8 mcp_platform/template/templates/snowflake/
```

## Support

For issues and questions:
- Check the [MCP Platform documentation](../../docs/)
- Review [Snowflake Python Connector docs](https://docs.snowflake.com/en/user-guide/python-connector.html)
- File issues in the [GitHub repository](https://github.com/Data-Everything/MCP-Platform)

## License

This template is part of the MCP Platform and is subject to the same license terms.