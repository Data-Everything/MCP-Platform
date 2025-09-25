"""
MCP Gateway - Unified proxy and load balancer for MCP servers.

This module provides a single HTTP endpoint for accessing all deployed MCP servers
with intelligent routing, load balancing, authentication, and health checking capabilities.

The gateway supports:
- HTTP and stdio MCP server proxying
- Multiple load balancing strategies (round-robin, least-connections, etc.)
- Health monitoring and automatic failover
- Dynamic server registration and discovery
- Authentication via JWT tokens and API keys
- Database persistence with SQLModel
- Integration with existing MCP Platform backends (Docker, Kubernetes)
- Python SDK for programmatic access
"""

from .auth import AuthManager, get_current_user_or_api_key, initialize_auth
from .client import GatewayClient, MCPGatewayConnection, call_tool_simple
from .database import DatabaseManager, initialize_database
from .gateway_server import MCPGatewayServer, create_gateway_server
from .health_checker import HealthChecker
from .load_balancer import LoadBalancer, LoadBalancingStrategy
from .models import (
    APIKey,
    APIKeyCreate,
    AuthConfig,
    DatabaseConfig,
    GatewayConfig,
    ServerInstance,
    ServerTemplate,
    ToolCallRequest,
    ToolCallResponse,
    User,
    UserCreate,
)
from .registry import ServerRegistry

__all__ = [
    # Core components
    "MCPGatewayServer",
    "MCPGatewayServer",
    "create_gateway_server",
    "LoadBalancer",
    "LoadBalancingStrategy",
    "ServerRegistry",
    "ServerRegistry",
    "HealthChecker",
    # Authentication
    "AuthManager",
    "get_current_user_or_api_key",
    "initialize_auth",
    # Database
    "DatabaseManager",
    "initialize_database",
    # Client SDK
    "GatewayClient",
    "MCPGatewayConnection",
    "call_tool_simple",
    # Models
    "APIKey",
    "APIKeyCreate",
    "AuthConfig",
    "DatabaseConfig",
    "GatewayConfig",
    "ServerInstance",
    "ServerTemplate",
    "ToolCallRequest",
    "ToolCallResponse",
    "User",
    "UserCreate",
]
