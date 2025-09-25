"""
Enhanced MCP Gateway Server with authentication, database persistence, and improved features.

Provides a single HTTP endpoint for accessing all deployed MCP servers with intelligent
routing, load balancing, authentication, and comprehensive management capabilities.
"""

import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, Union

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm

from mcp_platform.core.mcp_connection import MCPConnection
from mcp_platform.core.multi_backend_manager import MultiBackendManager

from .auth import (
    AuthManager,
    get_current_user_or_api_key,
    initialize_auth,
    require_gateway_read,
    require_gateway_write,
    require_tools_call,
)
from .database import DatabaseManager, close_database, initialize_database
from .health_checker import HealthChecker
from .load_balancer import LoadBalancer
from .models import (
    APIKey,
    APIKeyCreate,
    APIKeyResponse,
    GatewayConfig,
    GatewayStatsResponse,
    HealthCheckResponse,
    LoadBalancingStrategy,
    ServerInstance,
    ServerInstanceCreate,
    TokenResponse,
    ToolCallRequest,
    ToolCallResponse,
    User,
    UserCreate,
)
from .registry import ServerRegistry

logger = logging.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    logger.info("Starting MCP Gateway server...")
    # Initialize database
    config = app.state.config
    db = await initialize_database(config)
    app.state.db = db

    # Initialize authentication
    auth = initialize_auth(config.auth, db)
    app.state.auth = auth

    # Initialize registry with database
    registry = ServerRegistry(db, fallback_file="registry.json")
    app.state.registry = registry

    # Initialize other components
    app.state.load_balancer = LoadBalancer()
    app.state.health_checker = HealthChecker(registry, 30)
    app.state.backend_manager = MultiBackendManager()

    # Start health checker
    await app.state.health_checker.start()

    # Create default admin user if none exists
    try:
        from .database import UserCRUD

        user_crud = UserCRUD(db)
        admin_user = await user_crud.get_by_username("admin")
        if not admin_user:
            admin_password = os.getenv(
                "GATEWAY_ADMIN_PASSWORD", secrets.token_urlsafe(16)
            )
            admin_user = await auth.create_user(
                username="admin",
                password=admin_password,
                email="admin@localhost",
                is_superuser=True,
            )
    except Exception as e:
        logger.warning("Could not create admin user: %s", e)

    logger.info("MCP Gateway server started successfully")

    yield

    # Shutdown
    logger.info("Shutting down MCP Gateway server...")

    # Stop health checker
    if hasattr(app.state, "health_checker"):
        await app.state.health_checker.stop()

    # Close database
    await close_database()

    logger.info("MCP Gateway server stopped")


class MCPGatewayServer:
    """
    Enhanced MCP Gateway server with authentication, database persistence, and improved features.

    Features:
    - Authentication via JWT tokens and API keys
    - Database persistence with SQLModel
    - Enhanced error handling and validation
    - Comprehensive metrics and monitoring
    - Backward compatibility with existing clients
    """

    def __init__(self, config: Optional[GatewayConfig] = None):
        """
        Initialize enhanced MCP Gateway server.

        Args:
            config: Gateway configuration. Uses defaults if not provided.
        """
        self.config = config or GatewayConfig()

        # Generate secret key if not provided
        if not self.config.auth.secret_key:
            self.config.auth.secret_key = secrets.token_urlsafe(32)
            logger.warning(
                "Generated random secret key. Set GATEWAY_SECRET_KEY for production."
            )

        # FastAPI app with lifespan management
        self.app = FastAPI(
            title="MCP Gateway",
            description="Enhanced unified HTTP gateway for Model Context Protocol servers",
            version="2.0.0",
            lifespan=app_lifespan,
        )

        # Store config in app state
        self.app.state.config = self.config

        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self.config.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Setup routes
        self._setup_routes()

        # Runtime state
        self._request_count = 0
        self._start_time: Optional[float] = None

    def _setup_routes(self):
        """Setup FastAPI routes with authentication and validation."""
        self._setup_auth_routes()
        self._setup_gateway_management_routes()
        self._setup_template_routes()
        self._setup_mcp_server_routes()

    def _setup_auth_routes(self):
        """Setup authentication-related routes."""

        @self.app.post("/auth/login", response_model=TokenResponse)
        async def login(form_data: OAuth2PasswordRequestForm = Depends()):
            """Authenticate user and return JWT token."""
            auth: AuthManager = self.app.state.auth
            user = await auth.authenticate_user(form_data.username, form_data.password)

            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid credentials",
                )

            access_token = auth.create_access_token({"sub": user.username})
            return TokenResponse(
                access_token=access_token,
                expires_in=auth.config.access_token_expire_minutes * 60,
            )

        @self.app.post(
            "/auth/users",
            response_model=User,
            dependencies=[Depends(require_gateway_write)],
        )
        async def create_user(user_data: UserCreate):
            """Create a new user."""
            auth: AuthManager = self.app.state.auth
            return await auth.create_user(**user_data.dict())

        @self.app.post(
            "/auth/api-keys",
            response_model=APIKeyResponse,
            dependencies=[Depends(require_gateway_write)],
        )
        async def create_api_key(
            api_key_data: APIKeyCreate,
            current_user: Union[User, APIKey] = Depends(get_current_user_or_api_key),
        ):
            """Create a new API key."""
            auth: AuthManager = self.app.state.auth

            # Extract user ID
            user_id = api_key_data.user_id
            if isinstance(current_user, User) and not current_user.is_superuser:
                user_id = (
                    current_user.id
                )  # Non-superusers can only create keys for themselves

            api_key_record, api_key = await auth.create_api_key(
                user_id=user_id,
                name=api_key_data.name,
                description=api_key_data.description,
                scopes=api_key_data.scopes,
            )

            response = APIKeyResponse(**api_key_record.dict(), key=api_key)
            return response

    def _setup_gateway_management_routes(self):
        """Setup gateway management routes."""

        @self.app.get("/gateway/health")
        async def gateway_health():
            """Check gateway health status."""
            registry: ServerRegistry = self.app.state.registry
            db: DatabaseManager = self.app.state.db

            # Check database health
            db_healthy = await db.health_check()

            # Get registry stats
            stats = await registry.get_registry_stats()

            return {
                "status": "healthy" if db_healthy else "unhealthy",
                "database": "healthy" if db_healthy else "unhealthy",
                "templates": stats["total_templates"],
                "instances": {
                    "total": stats["total_instances"],
                    "healthy": stats["healthy_instances"],
                    "unhealthy": stats["unhealthy_instances"],
                },
                "uptime": time.time() - self._start_time if self._start_time else 0,
            }

        @self.app.get(
            "/gateway/stats",
            response_model=GatewayStatsResponse,
            dependencies=[Depends(require_gateway_read)],
        )
        async def gateway_stats():
            """Get comprehensive gateway statistics."""
            registry: ServerRegistry = self.app.state.registry
            load_balancer: LoadBalancer = self.app.state.load_balancer
            health_checker: HealthChecker = self.app.state.health_checker

            registry_stats = await registry.get_registry_stats()

            return GatewayStatsResponse(
                total_requests=self._request_count,
                active_connections=len(load_balancer.active_connections),
                templates=registry_stats["templates"],
                load_balancer={
                    "requests_per_instance": dict(load_balancer.request_counts),
                    "active_connections": dict(load_balancer.active_connections),
                },
                health_checker={
                    "running": health_checker.running,
                    "check_interval": health_checker.health_check_interval,
                    "last_check": (
                        health_checker.last_check_time.isoformat()
                        if health_checker.last_check_time
                        else None
                    ),
                },
            )

        @self.app.get("/gateway/registry", dependencies=[Depends(require_gateway_read)])
        async def get_registry():
            """Get server registry information."""
            registry: ServerRegistry = self.app.state.registry
            return await registry.get_registry_stats()

        @self.app.post(
            "/gateway/registry/sync", dependencies=[Depends(require_gateway_write)]
        )
        async def sync_registry():
            """Sync registry with existing deployments."""
            # This would integrate with the deployment discovery system
            return {"message": "Registry sync initiated"}

    def _setup_template_routes(self):
        """Setup template-related routes."""

        @self.app.get("/gateway/templates")
        async def list_templates():
            """List available templates."""
            registry: ServerRegistry = self.app.state.registry
            templates = await registry.list_templates()
            return {"templates": templates}

        @self.app.post(
            "/gateway/templates/{template_name}/instances",
            dependencies=[Depends(require_gateway_write)],
        )
        async def register_instance(
            template_name: str, instance_data: ServerInstanceCreate
        ):
            """Register a new server instance."""
            registry: ServerRegistry = self.app.state.registry
            instance = await registry.register_server(template_name, instance_data)
            return instance.dict()

        @self.app.delete(
            "/gateway/templates/{template_name}/instances/{instance_id}",
            dependencies=[Depends(require_gateway_write)],
        )
        async def deregister_instance(template_name: str, instance_id: str):
            """Deregister a server instance."""
            registry: ServerRegistry = self.app.state.registry
            success = await registry.deregister_server(template_name, instance_id)
            if not success:
                raise HTTPException(status_code=404, detail="Instance not found")
            return {"message": "Instance deregistered successfully"}

    def _setup_mcp_server_routes(self):
        """Setup MCP server routes (with authentication)."""

        @self.app.get("/mcp/{template_name}/tools/list")
        async def list_tools(
            template_name: str,
            _auth: Union[User, APIKey] = Depends(get_current_user_or_api_key),
        ):
            """List tools for a specific template."""
            return await self._handle_mcp_request(template_name, "tools/list", {})

        @self.app.post(
            "/mcp/{template_name}/tools/call", response_model=ToolCallResponse
        )
        async def call_tool(
            template_name: str,
            request: ToolCallRequest,
            _auth: Union[User, APIKey] = Depends(require_tools_call),
        ):
            """Call a tool through the gateway."""
            self._request_count += 1
            return await self._handle_mcp_request(
                template_name,
                "tools/call",
                {"name": request.name, "arguments": request.arguments},
            )

        @self.app.get("/mcp/{template_name}/resources/list")
        async def list_resources(
            template_name: str,
            _auth: Union[User, APIKey] = Depends(get_current_user_or_api_key),
        ):
            """List resources for a specific template."""
            return await self._handle_mcp_request(template_name, "resources/list", {})

        @self.app.post("/mcp/{template_name}/resources/read")
        async def read_resource(
            template_name: str,
            request: Dict[str, Any],
            _auth: Union[User, APIKey] = Depends(get_current_user_or_api_key),
        ):
            """Read a resource through the gateway."""
            return await self._handle_mcp_request(
                template_name, "resources/read", request
            )

        @self.app.get("/mcp/{template_name}/prompts/list")
        async def list_prompts(
            template_name: str,
            _auth: Union[User, APIKey] = Depends(get_current_user_or_api_key),
        ):
            """List prompts for a specific template."""
            return await self._handle_mcp_request(template_name, "prompts/list", {})

        @self.app.post("/mcp/{template_name}/prompts/get")
        async def get_prompt(
            template_name: str,
            request: Dict[str, Any],
            _auth: Union[User, APIKey] = Depends(get_current_user_or_api_key),
        ):
            """Get a prompt through the gateway."""
            return await self._handle_mcp_request(template_name, "prompts/get", request)

        @self.app.get("/mcp/{template_name}/health", response_model=HealthCheckResponse)
        async def template_health(template_name: str):
            """Get health status for a specific template."""
            registry: ServerRegistry = self.app.state.registry

            all_instances = await registry.list_instances(template_name)
            healthy_instances = await registry.get_healthy_instances(template_name)

            if not all_instances:
                raise HTTPException(status_code=404, detail="Template not found")

            return HealthCheckResponse(
                status="healthy" if healthy_instances else "unhealthy",
                healthy_instances=len(healthy_instances),
                total_instances=len(all_instances),
                instances=[
                    {
                        "id": inst.id,
                        "status": inst.status,
                        "endpoint": inst.endpoint,
                        "transport": inst.transport,
                        "last_health_check": (
                            inst.last_health_check.isoformat()
                            if inst.last_health_check
                            else None
                        ),
                    }
                    for inst in all_instances
                ],
            )

    async def _handle_mcp_request(
        self, template_name: str, method: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle MCP request with load balancing and failover."""
        registry: ServerRegistry = self.app.state.registry
        load_balancer: LoadBalancer = self.app.state.load_balancer

        # Get healthy instances
        instances = await registry.get_healthy_instances(template_name)

        # Stdio fallback if no instances
        if not instances:
            logger.info(
                "No healthy instances for %s, trying stdio fallback", template_name
            )
            return await self._try_stdio_fallback(template_name, method, params)

        # Select instance using load balancer
        template = await registry.get_template(template_name)
        strategy = LoadBalancingStrategy.ROUND_ROBIN
        if template and template.load_balancer:
            strategy = LoadBalancingStrategy(template.load_balancer.strategy)

        instance = load_balancer.select_instance(instances, strategy)

        # Route request
        try:
            if instance.transport == "http":
                result = await self._route_http_request(instance, method, params)
            else:
                result = await self._route_stdio_request(instance, method, params)

            # Record success
            load_balancer.record_request_completion(instance, True)
            return result

        except Exception as e:
            logger.error("Request failed for instance %s: %s", instance.id, e)

            # Record failure
            load_balancer.record_request_completion(instance, False)

            # Update health status
            await registry.update_instance_health(template_name, instance.id, False)

            # Try stdio fallback
            return await self._try_stdio_fallback(template_name, method, params)

    async def _route_http_request(
        self, instance: "ServerInstance", method: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Route request to HTTP MCP server."""
        connection = MCPConnection(timeout=60)

        try:
            success = await connection.connect_http_smart(instance.endpoint)
            if not success:
                raise ConnectionError(f"Failed to connect to {instance.endpoint}")

            if method == "tools/list":
                result = await connection.list_tools()
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await connection.call_tool(tool_name, arguments)
            elif method == "resources/list":  # pragma: no cover
                raise NotImplementedError("Resources listing not implemented")
            elif method == "resources/read":  # pragma: no cover
                # uri = params.get("uri")
                raise NotImplementedError("Resources reading not implemented")
            elif method == "prompts/list":  # pragma: no cover
                raise NotImplementedError("Prompts listing not implemented")
                # result = await connection.list_prompts()
            elif method == "prompts/get":  # pragma: no cover
                raise NotImplementedError("Prompts getting not implemented")
                # name = params.get("name")
                # arguments = params.get("arguments", {})
                # result = await connection.get_prompt(name, arguments)
            else:
                raise ValueError(f"Unsupported method: {method}")

            # Add gateway metadata
            if isinstance(result, dict):
                result["_gateway_info"] = {
                    "instance_id": instance.id,
                    "endpoint": instance.endpoint,
                    "transport": "http",
                }

            return result

        finally:
            await connection.disconnect()

    async def _route_stdio_request(
        self, instance: "ServerInstance", method: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Route request to stdio MCP server."""
        connection = MCPConnection(timeout=60)

        try:
            success = await connection.connect_stdio(
                command=instance.command,
                working_dir=instance.working_dir,
                env_vars=instance.env_vars or {},
            )
            if not success:
                raise ConnectionError("Failed to connect to stdio server")

            # Same logic as HTTP but through stdio
            if method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await connection.call_tool(tool_name, arguments)
            else:
                # Handle other methods...
                result = {"error": f"Method {method} not implemented for stdio"}

            # Add gateway metadata
            if isinstance(result, dict):
                result["_gateway_info"] = {
                    "instance_id": instance.id,
                    "command": instance.command,
                    "transport": "stdio",
                }

            return result

        finally:
            await connection.disconnect()

    async def _try_stdio_fallback(
        self, template_name: str, method: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Try stdio fallback using MultiBackendManager."""
        backend_manager = self.app.state.backend_manager

        try:
            if method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = await backend_manager.call_tool(
                    template_name=template_name,
                    tool_name=tool_name,
                    arguments=arguments,
                )
            else:
                raise ValueError(f"Stdio fallback not implemented for method: {method}")

            # Add gateway metadata
            if isinstance(result, dict):
                result["_gateway_info"] = {
                    "used_stdio_fallback": True,
                    "backend_type": "docker",
                    "message": f"No registered instances found for '{template_name}', used stdio fallback",
                }

            return result

        except Exception as e:
            logger.error("Stdio fallback failed for %s: %s", template_name, e)
            raise HTTPException(
                status_code=503,
                detail=f"Service unavailable: {e}",
            ) from e

    def run(self, **kwargs):
        """Run the gateway server."""
        self._start_time = time.time()

        # Merge config with kwargs
        run_config = {
            "host": self.config.host,
            "port": self.config.port,
            "reload": self.config.reload,
            "workers": self.config.workers,
            "log_level": self.config.log_level,
        }
        run_config.update(kwargs)

        logger.info(
            "Starting MCP Gateway on %s: %s", run_config["host"], run_config["port"]
        )
        uvicorn.run(self.app, **run_config)


# Factory function for backward compatibility
def create_gateway_server(
    host: str = "0.0.0.0", port: int = 8080, **kwargs
) -> MCPGatewayServer:
    """Create and configure a gateway server."""
    config = GatewayConfig(host=host, port=port, **kwargs)
    return MCPGatewayServer(config)
