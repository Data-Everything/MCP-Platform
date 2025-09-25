# Enhanced MCP Gateway

## What is the Enhanced MCP Gateway?

### ⚡ Performance & Architecture Improvements
- **Pydantic 2.x Migration**: Significant performance improvements with modern validation framework
- **SQLModel Integration**: Type-safe database operations with automatic validation
- **Enhanced MCPClient**: Gateway-aware client with intelligent fallback and connection management
- **Connection Pooling**: Efficient connection management and reuse
- **Load Balancing Strategies**: Round-robin, least connections, weighted, health-based
- **Health Checking**: Configurable health check intervals and strategies
- **Concurrent Operations**: Support for high-concurrency scenarios
- **Async Database Operations**: Full async/await support for optimal performance*Enhanced MCP Gateway** is a production-ready, enterprise-grade unified load balancer and proxy system that provides a single entry point for accessing all MCP (Model Context Protocol) server instances in your deployment. This enhanced version includes comprehensive authentication, database persistence, and a powerful Python SDK for programmatic access.

## Why is the Gateway Needed?

### Problems with Direct MCP Server Access

Before the gateway, clients had to:

1. **Manage Multiple Connections**: Each MCP server template required a separate connection
2. **Handle Load Balancing Manually**: No automatic distribution across multiple instances
3. **Implement Health Checking**: Clients needed to detect and handle server failures
4. **Deal with Service Discovery**: Finding and tracking available server instances
5. **Handle Failover Logic**: Manual switching when servers become unavailable
6. **Implement Authentication**: Each client needed custom authentication logic
7. **Manage State Persistence**: No centralized state management

### Benefits of Using the Gateway

The Enhanced MCP Gateway solves these problems by providing:

✅ **Single Endpoint**: One URL for all MCP server access
✅ **Enhanced Authentication**: JWT tokens and API key authentication with Pydantic 2.x validation
✅ **Database Persistence**: SQLModel/SQLAlchemy with SQLite/PostgreSQL support and async operations
✅ **Enhanced Python SDK**: Comprehensive client library with gateway-aware MCPClient integration
✅ **Automatic Load Balancing**: Intelligent request distribution with multiple strategies
✅ **Health Monitoring**: Continuous health checking with automatic failover
✅ **Modern Data Validation**: Full Pydantic 2.x migration for improved performance and validation
✅ **Flexible Storage**: Multiple database options with seamless migration support
✅ **Service Discovery**: Automatic detection of available instances
✅ **Transport Abstraction**: Unified access to both HTTP and stdio servers
✅ **High Availability**: No single point of failure
✅ **Role-based Access Control**: Admin and user roles with scoped permissions
✅ **Enhanced CLI**: Comprehensive command-line interface for management

### 🔐 Authentication System
- **JWT Token Authentication**: Secure token-based authentication with configurable expiration
- **API Key Management**: Create, manage, and revoke API keys with scoped permissions
- **Role-based Access Control**: Admin and user roles with granular permissions
- **Password Security**: Bcrypt hashing with secure password policies

### 🗄️ Enhanced Database & Storage
- **SQLModel Integration**: Modern type-safe database models with automatic Pydantic validation
- **Multiple Database Support**: SQLite (default), PostgreSQL with seamless switching
- **Async Operations**: Full async/await support for database operations
- **Automatic Migrations**: Database schema management and migrations
- **Connection Pooling**: Efficient database connection management
- **Data Validation**: Comprehensive input/output validation using Pydantic 2.x

### 📦 Enhanced Python SDK & Client
- **GatewayClient**: Comprehensive HTTP client for gateway interaction
- **Enhanced MCPClient**: Gateway-aware client with automatic discovery and fallback
- **Connection Pooling**: Efficient connection management and reuse
- **Batch Operations**: Support for batch tool calls and operations
- **Async Support**: Full async/await support for all operations
- **Intelligent Routing**: Automatic load balancing and health-based routing
- **Failover Management**: Seamless fallback to direct MCP connections when needed

### 🖥️ Enhanced CLI
- **User Management**: Create, list, and manage users
- **API Key Operations**: Generate and manage API keys
- **Interactive Mode**: Enhanced interactive CLI experience
- **Configuration Management**: Centralized configuration system

### ⚡ Performance Improvements
- **Connection Pooling**: Efficient connection management
- **Load Balancing Strategies**: Round-robin, least connections, weighted, health-based
- **Health Checking**: Configurable health check intervals and strategies
- **Concurrent Operations**: Support for high-concurrency scenarios

## How It Works

### Enhanced Architecture

```
┌─────────────┐    ┌─────────────────────────┐    ┌─────────────────┐
│   Clients   │───▶│   Enhanced MCP Gateway  │───▶│  MCP Servers    │
│             │    │                         │    │                 │
│ Python SDK  │    │ • FastAPI Server        │    │ • Instance 1    │
│ HTTP API    │    │ • JWT/API Auth          │    │ • Instance 2    │
│ CLI Tool    │    │ • Database Layer        │    │ • Instance N    │
│             │    │ • Load Balancer         │    │                 │
└─────────────┘    │ • Health Checker        │    └─────────────────┘
                   │ • Enhanced Registry     │
                   └─────────────────────────┘
                            │
                   ┌─────────────────────────┐
                   │     Database Layer      │
                   │                                     │
                   │ • SQLite (default - lightweight)   │
                   │ • PostgreSQL (production-ready)    │
                   │ • Async operations with pooling     │
                   │ • Pydantic 2.x validation          │
                   └─────────────────────────────────────┘
```

### Core Components

1. **Gateway Server**: FastAPI-based server with authentication middleware
2. **Authentication System**: JWT and API key authentication with role-based access
3. **Database Layer**: SQLModel/SQLAlchemy with async operations and Pydantic validation
4. **Enhanced Registry**: State management with database persistence
5. **Load Balancer**: Multiple strategies with health-based routing
6. **Health Checker**: Configurable monitoring with failure detection
7. **Python SDK**: Comprehensive client library for programmatic access
8. **Enhanced MCPClient**: Integrated gateway support with automatic fallback
9. **Enhanced CLI**: Full-featured command-line interface

### Request Flow

1. **Client Request**: Client sends request to `/mcp/{template}/tools/list`
2. **Template Resolution**: Gateway identifies the target template
3. **Instance Selection**: Load balancer selects healthy instance
4. **Request Routing**: Gateway forwards request to selected instance
5. **Response Handling**: Gateway returns response to client

## Gateway Endpoints

### Core MCP Operations

All MCP operations follow the pattern: `/mcp/{template_name}/{operation}`

#### Tool Management
```http
GET  /mcp/{template}/tools/list     # List available tools
POST /mcp/{template}/tools/call     # Call a specific tool
```

#### Resource Management
```http
GET  /mcp/{template}/resources/list # List available resources
POST /mcp/{template}/resources/read # Read a specific resource
```

#### Health Monitoring
```http
GET  /mcp/{template}/health         # Check template health
```

### Gateway Management

#### System Information
```http
GET  /gateway/health                # Gateway health status
GET  /gateway/stats                 # Comprehensive statistics
GET  /gateway/registry              # Registry information
```

#### Instance Management
```http
POST   /gateway/register            # Register new server instance
DELETE /gateway/deregister/{template}/{instance_id}  # Remove instance
```

## API Documentation

### Swagger/OpenAPI Documentation

When the gateway is running, you can access interactive API documentation at:
- **Swagger UI**: `http://localhost:8080/docs`
- **ReDoc**: `http://localhost:8080/redoc`
- **OpenAPI JSON**: `http://localhost:8080/openapi.json`

### Example API Calls

#### List Tools (with Authentication)
```bash
# Using JWT token
curl -X GET http://localhost:8080/mcp/filesystem/tools/list \
  -H "Authorization: Bearer your-jwt-token"

# Using API key
curl -X GET http://localhost:8080/mcp/filesystem/tools/list \
  -H "X-API-Key: your-api-key"
```

Response:
```json
{
  "tools": [
    {
      "name": "read_file",
      "description": "Read the contents of a file",
      "parameters": {
        "type": "object",
        "properties": {
          "path": {"type": "string", "description": "File path"}
        }
      }
    }
  ],
  "count": 1
}
```

#### Call Tool
```bash
curl -X POST http://localhost:8080/mcp/filesystem/tools/call \
  -H "Content-Type: application/json" \
  -d '{
    "name": "read_file",
    "arguments": {"path": "/etc/hosts"}
  }'
```

Response:
```json
{
  "content": [
    {
      "type": "text",
      "text": "127.0.0.1 localhost\n..."
    }
  ],
  "isError": false
}
```

#### Check Health
```bash
curl -X GET http://localhost:8080/mcp/filesystem/health
```

Response:
```json
{
  "template_name": "filesystem",
  "total_instances": 3,
  "healthy_instances": 2,
  "health_percentage": 66.7,
  "instances": {
    "fs-1": {"healthy": true, "status": "healthy"},
    "fs-2": {"healthy": true, "status": "healthy"},
    "fs-3": {"healthy": false, "status": "unhealthy"}
  }
}
```

## Getting Started

### 1. Start the Gateway

```bash
# Basic startup (auto-discovers existing deployments)
mcpp gateway start

# Custom configuration
mcpp gateway start --host 0.0.0.0 --port 8080 --sync

# Background mode
mcpp gateway start --background
```

### 2. Verify Gateway is Running

```bash
# Check health
curl http://localhost:8080/gateway/health

# View registered instances
curl http://localhost:8080/gateway/registry
```

### 3. Authenticate and Create Users

```bash
# Start gateway with authentication enabled
mcpp gateway start --auth

# Create admin user (in another terminal)
mcpp gateway create-user admin --password secure_password --role admin

# Create API key for programmatic access
mcpp gateway create-api-key my-app --role user

# Login to get JWT token
mcpp gateway login admin
```

### 4. Register MCP Servers

```bash
# Register HTTP server
mcpp gateway register mytemplate --endpoint http://localhost:7071

# Register stdio server
mcpp gateway register mytemplate --command "python server.py" --working-dir /app
```

### 5. Use the Gateway with Authentication

```python
import asyncio
from mcp_platform.client import GatewayClient

async def use_gateway():
    # Option 1: Using JWT token
    client = GatewayClient(
        base_url="http://localhost:8080",
        auth_token="your-jwt-token"
    )

    # Option 2: Using API key
    client = GatewayClient(
        base_url="http://localhost:8080",
        api_key="your-api-key"
    )

    async with client:
        # List tools
        tools = await client.list_tools("filesystem")
        print(f"Available tools: {len(tools)}")

        # Call a tool with enhanced validation
        result = await client.call_tool(
            "filesystem",
            "read_file",
            {"path": "/etc/hosts"}
        )
        print(f"Tool result: {result}")

# Enhanced MCPClient with gateway integration
from mcp_platform.client import MCPClient

async def use_enhanced_client():
    # Automatically discovers and uses gateway
    client = MCPClient(gateway_url="http://localhost:8080")

    async with client:
        # Seamlessly works with gateway or direct MCP
        tools = await client.list_tools()
        result = await client.call_tool("read_file", {"path": "/etc/hosts"})
        print(f"Enhanced client result: {result}")

asyncio.run(use_gateway())
asyncio.run(use_enhanced_client())
```

## Load Balancing Strategies

The gateway supports multiple load balancing strategies:

### 1. Round Robin (Default)
Distributes requests evenly across all healthy instances.
```bash
Instance 1 → Instance 2 → Instance 3 → Instance 1 → ...
```

### 2. Least Connections
Routes to the instance with the fewest active connections.
```bash
Request → Instance with 2 connections (vs others with 5, 3, 4)
```

### 3. Weighted Round Robin
Routes based on instance weights (higher weight = more requests).
```bash
Weight 3: Instance A gets 3 requests
Weight 1: Instance B gets 1 request
```

### 4. Health-Based
Prefers instances with better health scores.
```bash
Request → Instance with 100% health (vs others with 80%, 90%)
```

### 5. Random
Randomly selects from healthy instances.
```bash
Request → Random(Instance 1, Instance 2, Instance 3)
```

## Configuration

### Database Configuration

The gateway supports multiple database backends for flexible deployment. See [Database Support](database-support.md) for detailed information on:

- **SQLite** (default): Zero-configuration local database
- **PostgreSQL**: Recommended for production deployments
- **MySQL**: MySQL/MariaDB support
- **Oracle**: Enterprise Oracle Database support
- **SQL Server**: Microsoft SQL Server support

Quick database setup examples:
```bash
# Default SQLite (included)
pip install mcp-platform

# PostgreSQL for production
pip install mcp-platform[postgresql]

# All database support
pip install mcp-platform[all-databases]
```

## Configuration

### Database Configuration

The Enhanced MCP Gateway supports multiple database backends for flexible deployment scenarios. See [Database Support](database-support.md) for detailed configuration options.

**Quick Setup:**
- **SQLite (Default)**: `pip install mcp-platform` - Zero configuration required
- **PostgreSQL**: `pip install mcp-platform[postgresql]` - Production recommended
- **MySQL**: `pip install mcp-platform[mysql]` - MySQL/MariaDB environments
- **All Databases**: `pip install mcp-platform[all-databases]` - Full support

### Template Configuration

Templates can be configured with specific load balancing settings:

```json
{
  "templates": {
    "high-performance": {
      "instances": [...],
      "load_balancer": {
        "strategy": "weighted",
        "health_check_interval": 15,
        "max_retries": 2,
        "timeout": 30
      }
    }
  }
}
```

### Environment Variables

```bash
# Gateway server settings
MCP_GATEWAY_HOST=0.0.0.0
MCP_GATEWAY_PORT=8080
MCP_GATEWAY_REGISTRY_FILE=/path/to/registry.json

# Health checking
MCP_GATEWAY_HEALTH_INTERVAL=30
MCP_GATEWAY_HEALTH_TIMEOUT=10

# Load balancing
MCP_GATEWAY_DEFAULT_STRATEGY=round_robin
MCP_GATEWAY_MAX_RETRIES=3
```

## Monitoring and Observability

### Health Endpoints

```bash
# Gateway health
curl http://localhost:8080/gateway/health

# Template health
curl http://localhost:8080/mcp/filesystem/health

# Detailed statistics
curl http://localhost:8080/gateway/stats | jq
```

### Key Metrics

- **Request Count**: Total requests processed
- **Response Time**: Average response latency
- **Error Rate**: Failed request percentage
- **Instance Health**: Per-instance health status
- **Load Distribution**: Requests per instance

### Logging

The gateway provides structured logging for:
- Request routing decisions
- Health check results
- Load balancer selections
- Error conditions
- Performance metrics

## Troubleshooting

### Common Issues

#### Gateway Not Starting
```bash
# Check port availability
netstat -tlnp | grep 8080

# View detailed logs
mcpp gateway start --log-level debug
```

#### No Instances Available
```bash
# Check registry
curl http://localhost:8080/gateway/registry

# Sync with deployments
mcpp gateway sync

# Manual registration
mcpp gateway register mytemplate --endpoint http://localhost:7071
```

#### Health Check Failures
```bash
# Check instance health
curl http://localhost:8080/mcp/mytemplate/health

# View health checker stats
curl http://localhost:8080/gateway/stats | jq '.health_checker'
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
mcpp gateway start --log-level debug
```

This provides detailed information about:
- Request routing decisions
- Load balancer selections
- Health check processes
- Error handling

## Best Practices

### 1. Health Check Configuration
- Set appropriate health check intervals (30-60 seconds)
- Configure timeouts based on server response times
- Use health-based load balancing for critical services

### 2. Load Balancing Strategy Selection
- **Round Robin**: General purpose, evenly distributed load
- **Least Connections**: Connection-heavy workloads
- **Weighted**: Different instance capabilities
- **Health-Based**: High availability requirements

### 3. Instance Management
- Register instances with meaningful metadata
- Use consistent naming conventions
- Monitor instance health regularly
- Plan for graceful degradation

### 4. Client Integration
- Implement proper error handling
- Use connection pooling
- Cache tool lists when appropriate
- Handle gateway unavailability

## Security Considerations

### 1. Network Security
- Deploy gateway behind load balancer/reverse proxy
- Use HTTPS in production
- Implement network segmentation
- Restrict access to management endpoints

### 2. Authentication & Authorization
- **JWT Token Security**: Rotate tokens regularly and use secure expiration policies
- **API Key Management**: Implement proper API key rotation and scope limitation
- **Role-Based Access**: Assign minimal required roles to users and applications
- **Database Security**: Use encrypted connections and secure credential storage
- **Rate Limiting**: Implement per-user/per-API-key rate limiting
- **Audit Logging**: Log all authentication attempts and security events

### 3. Data Validation & Protection
- **Input Validation**: Leverage Pydantic 2.x for comprehensive request validation
- **Output Sanitization**: Ensure response data is properly validated before transmission
- **Database Security**: Use parameterized queries and connection encryption
- **Backup Strategy**: Implement regular database backups with encryption

### 3. Instance Security
- Validate registered instances
- Implement instance authentication
- Monitor for suspicious activity
- Regular security updates

## Migration Guide

### From Direct MCP Access

1. **Assess Current Setup**: Inventory existing MCP server connections
2. **Deploy Gateway**: Start gateway and sync with existing deployments
3. **Update Clients**: Modify client code to use gateway endpoints
4. **Test and Validate**: Verify functionality with gateway
5. **Monitor and Optimize**: Monitor performance and adjust configuration

### Client Code Changes

Before (Direct Access):
```python
## Migration & Upgrade Guide

### Migrating from Direct MCP Connections

Before (Direct Connections):
```python
# Connect to each server individually with manual authentication
fs_client = MCPClient("http://localhost:7071")
db_client = MCPClient("http://localhost:7072")
```

After (Gateway Access):
```python
# Single gateway connection with authentication
from mcp_platform.client import GatewayClient, MCPClient

# Option 1: Direct gateway client
gateway_client = GatewayClient(
    base_url="http://localhost:8080",
    auth_token="your-jwt-token"
)

# Option 2: Enhanced MCPClient with automatic gateway detection
client = MCPClient(gateway_url="http://localhost:8080")
```

### Upgrading from Previous Versions

1. **Backup existing data**: Export configurations and user data
2. **Update dependencies**: Ensure Pydantic 2.x compatibility in your code
3. **Database migration**: Run automatic schema updates
4. **Authentication setup**: Configure new authentication system
5. **Test enhanced features**: Validate SQLModel integration and improved performance

## Advanced Topics

### Enhanced Features Deep Dive

#### Pydantic 2.x Integration Benefits
- **Performance**: 5-50x faster validation than v1
- **Better Error Messages**: More precise validation feedback
- **Type Safety**: Improved static type checking
- **Memory Efficiency**: Reduced memory usage for large datasets

#### SQLModel Architecture Advantages
- **Type Safety**: Database models with full typing support
- **Automatic Validation**: Built-in Pydantic validation for all database operations
- **Query Builder**: Type-safe database queries with excellent IDE support
- **Migration Support**: Seamless schema changes and database evolution

#### Enhanced MCPClient Features
- **Gateway Discovery**: Automatic detection and connection to available gateways
- **Intelligent Fallback**: Seamless switching between gateway and direct connections
- **Connection Pooling**: Efficient resource management for high-throughput scenarios
- **Load Balancing**: Built-in support for multiple gateway instances

### Custom Health Checks
Implement custom health check logic for specific templates with enhanced validation.

### Circuit Breaker Pattern
Automatic failure isolation and recovery for resilient systems with database-backed state.

### Request Transformation
Modify requests/responses as they pass through the gateway with Pydantic validation.

### Multi-Region Deployment
Deploy gateways across multiple regions with synchronized database state for global availability.
```

After (Gateway Access):
```python
# Single gateway connection
gateway_client = MCPClient("http://localhost:8080")
# Use /mcp/{template}/* endpoints
```

## Advanced Topics

### Custom Health Checks
Implement custom health check logic for specific templates.

### Circuit Breaker Pattern
Automatic failure isolation and recovery for resilient systems.

### Request Transformation
Modify requests/responses as they pass through the gateway.

### Multi-Region Deployment
Deploy gateways across multiple regions for global availability.

## Support and Community

- **Documentation**: [MCP Platform Docs](https://docs.mcpplatform.com)
- **Issues**: [GitHub Issues](https://github.com/Data-Everything/MCP-Platform/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Data-Everything/MCP-Platform/discussions)
- **Examples**: [Gateway Examples](https://github.com/Data-Everything/MCP-Platform/tree/main/examples/gateway)
