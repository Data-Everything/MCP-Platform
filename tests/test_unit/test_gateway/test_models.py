"""
Unit tests for gateway models.

Tests the SQLModel-based data models including validation, serialization,
and relationships.
"""

from datetime import datetime, timezone

import pytest

from mcp_platform.gateway.models import (
    APIKey,
    APIKeyCreate,
    AuthConfig,
    BackendType,
    DatabaseConfig,
    GatewayConfig,
    LoadBalancerConfig,
    LoadBalancingStrategy,
    ServerInstanceCreate,
    ServerStatus,
    ServerTemplate,
    TransportType,
    UserCreate,
)

pytestmark = pytest.mark.unit


class TestEnums:
    """Test enum values and validation."""

    def test_transport_type_values(self):
        """Test TransportType enum values."""
        assert TransportType.HTTP == "http"
        assert TransportType.STDIO == "stdio"

    def test_server_status_values(self):
        """Test ServerStatus enum values."""
        assert ServerStatus.HEALTHY == "healthy"
        assert ServerStatus.UNHEALTHY == "unhealthy"
        assert ServerStatus.UNKNOWN == "unknown"

    def test_backend_type_values(self):
        """Test BackendType enum values."""
        assert BackendType.DOCKER == "docker"
        assert BackendType.KUBERNETES == "kubernetes"
        assert BackendType.LOCAL == "local"
        assert BackendType.MOCK == "mock"

    def test_load_balancing_strategy_values(self):
        """Test LoadBalancingStrategy enum values."""
        assert LoadBalancingStrategy.ROUND_ROBIN == "round_robin"
        assert LoadBalancingStrategy.LEAST_CONNECTIONS == "least_connections"
        assert LoadBalancingStrategy.WEIGHTED == "weighted"
        assert LoadBalancingStrategy.HEALTH_BASED == "health_based"
        assert LoadBalancingStrategy.RANDOM == "random"


class TestServerInstance:
    """Test ServerInstance model."""

    def test_create_minimal_instance(self):
        """Test creating instance with minimal required fields."""
        instance = ServerInstanceCreate(id="test-1", template_name="demo")
        assert instance.id == "test-1"
        assert instance.template_name == "demo"
        assert instance.transport == TransportType.HTTP
        assert instance.status == ServerStatus.UNKNOWN
        assert instance.backend == BackendType.DOCKER

    def test_create_full_instance(self):
        """Test creating instance with all fields."""
        instance = ServerInstanceCreate(
            id="test-1",
            template_name="demo",
            endpoint="http://localhost:8080",
            transport=TransportType.HTTP,
            status=ServerStatus.HEALTHY,
            backend=BackendType.DOCKER,
            container_id="container123",
            working_dir="/app",
            env_vars={"KEY": "value"},
            instance_metadata={"version": "1.0"},
        )
        assert instance.endpoint == "http://localhost:8080"
        assert instance.container_id == "container123"
        assert instance.env_vars == {"KEY": "value"}
        assert instance.instance_metadata == {"version": "1.0"}

    def test_command_validation(self):
        """Test command field validation."""
        # String command should be converted to list
        instance = ServerInstanceCreate(
            id="test-1", template_name="demo", command="python server.py"
        )
        assert instance.command == ["python server.py"]

        # List command should remain as list
        instance2 = ServerInstanceCreate(
            id="test-2",
            template_name="demo",
            command=["python", "server.py", "--port", "8080"],
        )
        assert instance2.command == ["python", "server.py", "--port", "8080"]

    def test_health_status_methods(self):
        """Test health status methods."""
        instance = ServerInstanceCreate(
            id="test-1", template_name="demo", status=ServerStatus.HEALTHY
        )
        assert instance.is_healthy() is True

        instance.status = ServerStatus.UNHEALTHY
        assert instance.is_healthy() is False

        # Test update_health_status
        instance.update_health_status(True)
        assert instance.status == ServerStatus.HEALTHY
        assert instance.consecutive_failures == 0
        assert instance.last_health_check is not None

        instance.update_health_status(False)
        assert instance.status == ServerStatus.UNHEALTHY
        assert instance.consecutive_failures == 1


class TestServerTemplate:
    """Test ServerTemplate model."""

    def test_create_template(self):
        """Test creating server template."""
        template = ServerTemplate(name="demo")
        assert template.name == "demo"
        assert template.description is None
        assert isinstance(template.created_at, datetime)
        assert isinstance(template.updated_at, datetime)

    def test_template_with_description(self):
        """Test template with description."""
        template = ServerTemplate(name="demo", description="Demo MCP server")
        assert template.description == "Demo MCP server"


class TestLoadBalancerConfig:
    """Test LoadBalancerConfig model."""

    def test_create_config(self):
        """Test creating load balancer config."""
        config = LoadBalancerConfig(template_name="demo")
        assert config.template_name == "demo"
        assert config.strategy == LoadBalancingStrategy.ROUND_ROBIN
        assert config.health_check_interval == 30
        assert config.max_retries == 3
        assert config.pool_size == 3
        assert config.timeout == 60

    def test_config_validation(self):
        """Test config field validation."""
        # Valid ranges should work
        config = LoadBalancerConfig(
            template_name="demo",
            health_check_interval=10,
            max_retries=5,
            pool_size=10,
            timeout=120,
        )
        assert config.health_check_interval == 10
        assert config.max_retries == 5
        assert config.pool_size == 10
        assert config.timeout == 120

        # Test edge cases within valid ranges
        min_config = LoadBalancerConfig(
            template_name="demo",
            health_check_interval=5,
            max_retries=1,
            pool_size=1,
            timeout=5,
        )
        assert min_config.health_check_interval == 5

        max_config = LoadBalancerConfig(
            template_name="demo",
            health_check_interval=300,
            max_retries=10,
            pool_size=20,
            timeout=300,
        )
        assert max_config.health_check_interval == 300


class TestUser:
    """Test User model."""

    def test_create_user(self):
        """Test creating user."""
        user_data = UserCreate(
            username="testuser", email="test@example.com", password="secret123"
        )
        assert user_data.username == "testuser"
        assert user_data.email == "test@example.com"
        assert user_data.password == "secret123"
        assert user_data.is_active is True
        assert user_data.is_superuser is False

    def test_user_defaults(self):
        """Test user default values."""
        user_data = UserCreate(username="testuser", password="secret123")
        assert user_data.email is None
        assert user_data.full_name is None
        assert user_data.is_active is True
        assert user_data.is_superuser is False


class TestAPIKey:
    """Test APIKey model."""

    def test_create_api_key(self):
        """Test creating API key."""
        api_key_data = APIKeyCreate(name="Test Key", user_id=1)
        assert api_key_data.name == "Test Key"
        assert api_key_data.user_id == 1
        assert api_key_data.is_active is True
        assert api_key_data.expires_at is None
        assert api_key_data.scopes == []

    def test_api_key_with_expiration(self):
        """Test API key with expiration."""
        expires_at = datetime.now(timezone.utc)
        api_key_data = APIKeyCreate(name="Test Key", user_id=1, expires_at=expires_at)
        assert api_key_data.expires_at == expires_at

    def test_is_expired_method(self):
        """Test is_expired method."""
        # Create an API key without expiration
        api_key = APIKey(name="Test Key", key_hash="hashed_key", user_id=1)
        assert api_key.is_expired() is False

        # Create an expired API key
        past_time = datetime.now(timezone.utc).replace(year=2020)
        expired_key = APIKey(
            name="Expired Key", key_hash="hashed_key", user_id=1, expires_at=past_time
        )
        assert expired_key.is_expired() is True

        # Create a future-expiring key
        future_time = datetime.now(timezone.utc).replace(year=2030)
        future_key = APIKey(
            name="Future Key", key_hash="hashed_key", user_id=1, expires_at=future_time
        )
        assert future_key.is_expired() is False


class TestConfigModels:
    """Test configuration models."""

    def test_database_config(self):
        """Test DatabaseConfig model."""
        config = DatabaseConfig()
        assert config.url.startswith("sqlite:///")
        assert ".mcp" in config.url
        assert config.echo is False
        assert config.pool_size == 5
        assert config.max_overflow == 10

    def test_auth_config(self):
        """Test AuthConfig model."""
        config = AuthConfig(secret_key="test-secret")
        assert config.secret_key == "test-secret"
        assert config.algorithm == "HS256"
        assert config.access_token_expire_minutes == 30
        assert config.api_key_expire_days == 30

    def test_gateway_config(self):
        """Test GatewayConfig model."""
        config = GatewayConfig()
        assert config.host == "localhost"
        assert config.port == 8080
        assert config.reload is False
        assert config.workers == 1
        assert config.log_level == "info"
        assert config.cors_origins == ["*"]
        assert isinstance(config.database, DatabaseConfig)
        assert config.auth is None

    def test_gateway_config_with_auth(self):
        """Test GatewayConfig with auth configuration."""
        auth_config = AuthConfig(secret_key="test-secret")
        config = GatewayConfig(auth=auth_config)
        assert config.auth is not None
        assert config.auth.secret_key == "test-secret"


class TestModelSerialization:
    """Test model serialization and deserialization."""

    def test_server_instance_serialization(self):
        """Test ServerInstance serialization."""
        instance = ServerInstanceCreate(
            id="test-1",
            template_name="demo",
            endpoint="http://localhost:8080",
            transport=TransportType.HTTP,
            status=ServerStatus.HEALTHY,
            instance_metadata={"key": "value"},
        )

        # Test dict serialization
        data = instance.model_dump()
        assert data["id"] == "test-1"
        assert data["template_name"] == "demo"
        assert data["endpoint"] == "http://localhost:8080"
        assert data["transport"] == "http"
        assert data["status"] == "healthy"
        assert data["instance_metadata"] == {"key": "value"}

        # Test that we can recreate from dict
        instance2 = ServerInstanceCreate(**data)
        assert instance2.id == instance.id
        assert instance2.template_name == instance.template_name
        assert instance2.endpoint == instance.endpoint

    def test_json_serialization(self):
        """Test JSON serialization."""
        instance = ServerInstanceCreate(
            id="test-1", template_name="demo", endpoint="http://localhost:8080"
        )

        json_str = instance.model_dump_json()
        assert '"test-1"' in json_str
        assert '"demo"' in json_str
        assert '"http://localhost:8080"' in json_str
