"""
Unit tests for Trino MCP server configuration and validation.

Tests the Trino template's configuration handling, validation, and
authentication setup using the new Python FastMCP implementation.
"""

import json
import os
# Import the configuration module
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from config import TrinoServerConfig, create_trino_config
except ImportError:
    # Handle import in different environments
    import importlib.util

    config_path = os.path.join(os.path.dirname(__file__), "..", "config.py")
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    TrinoServerConfig = config_module.TrinoServerConfig
    create_trino_config = config_module.create_trino_config


class TestTrinoTemplateConfiguration:
    """Test Trino template configuration validation and processing."""

    def test_template_json_structure(self):
        """Test Trino template.json has required structure for Python implementation."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        # Verify required template fields for new implementation
        assert template_config["name"] == "Trino MCP Server"
        assert template_config["description"]
        assert template_config["version"] == "1.0.0"
        assert template_config["docker_image"] == "dataeverything/mcp-trino"
        assert template_config["docker_tag"] == "latest"
        assert template_config["has_image"] is True
        assert template_config["origin"] == "internal"  # Changed from external
        assert template_config["category"] == "Database"

        # Verify supported transports (now supports HTTP and stdio)
        assert template_config["transport"]["default"] == "http"
        assert "http" in template_config["transport"]["supported"]
        assert "stdio" in template_config["transport"]["supported"]
        assert template_config["transport"]["port"] == 7090

        # Verify tags include new ones
        assert "fastmcp" in template_config["tags"]
        assert "sqlalchemy" in template_config["tags"]

        # Verify configuration schema exists
        assert "config_schema" in template_config
        assert "properties" in template_config["config_schema"]

    def test_basic_configuration_schema(self):
        """Test basic connection configuration options."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        config_schema = template_config["config_schema"]
        properties = config_schema["properties"]

        # Required fields
        assert "trino_host" in properties
        assert "trino_user" in properties
        assert properties["trino_host"]["env_mapping"] == "TRINO_HOST"
        assert properties["trino_user"]["env_mapping"] == "TRINO_USER"

        # Basic connection settings
        assert "trino_port" in properties
        assert properties["trino_port"]["default"] == 8080
        assert properties["trino_port"]["env_mapping"] == "TRINO_PORT"

        assert "trino_password" in properties
        assert properties["trino_password"]["env_mapping"] == "TRINO_PASSWORD"

        assert "trino_scheme" in properties
        assert properties["trino_scheme"]["enum"] == ["http", "https"]
        assert properties["trino_scheme"]["default"] == "https"

    def test_authentication_configuration_schema(self):
        """Test authentication configuration options."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        properties = template_config["config_schema"]["properties"]

        # OAuth configuration
        assert "oauth_enabled" in properties
        assert properties["oauth_enabled"]["default"] is False
        assert properties["oauth_enabled"]["env_mapping"] == "TRINO_OAUTH_ENABLED"

        assert "oauth_provider" in properties
        oauth_providers = properties["oauth_provider"]["enum"]
        assert "hmac" in oauth_providers
        assert "okta" in oauth_providers
        assert "google" in oauth_providers
        assert "azure" in oauth_providers

        # JWT/OIDC fields
        assert "jwt_secret" in properties
        assert properties["jwt_secret"]["env_mapping"] == "TRINO_JWT_SECRET"

        oidc_fields = [
            "oidc_issuer",
            "oidc_audience",
            "oidc_client_id",
            "oidc_client_secret",
        ]
        for field in oidc_fields:
            assert field in properties
            expected_env = f"TRINO_{field.upper()}"
            assert properties[field]["env_mapping"] == expected_env

    def test_security_and_performance_configuration(self):
        """Test security and performance configuration options."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        properties = template_config["config_schema"]["properties"]

        # Read-only mode (now called trino_allow_write_queries with reversed logic)
        assert "trino_allow_write_queries" in properties
        write_queries = properties["trino_allow_write_queries"]
        assert write_queries["default"] is False  # Read-only by default
        assert write_queries["env_mapping"] == "TRINO_ALLOW_WRITE_QUERIES"
        assert "WARNING" in write_queries["description"]

        # Query limits
        assert "trino_max_results" in properties
        max_results = properties["trino_max_results"]
        assert max_results["default"] == 1000
        assert max_results["minimum"] == 1
        assert max_results["maximum"] == 10000
        assert max_results["env_mapping"] == "TRINO_MAX_RESULTS"

        # Query timeout
        assert "trino_query_timeout" in properties
        timeout = properties["trino_query_timeout"]
        assert timeout["default"] == "300"
        assert timeout["env_mapping"] == "TRINO_QUERY_TIMEOUT"

        # SSL settings
        assert "trino_ssl" in properties
        assert "trino_ssl_insecure" in properties

    def test_config_object_initialization(self):
        """Test TrinoServerConfig object initialization."""
        # Test with minimal config
        config = TrinoServerConfig(
            {"trino_host": "localhost", "trino_user": "admin"}, skip_validation=True
        )

        assert config is not None
        template_config = config.get_template_config()
        assert template_config["trino_host"] == "localhost"
        assert template_config["trino_user"] == "admin"

    def test_config_validation_required_fields(self):
        """Test configuration validation for required fields."""
        # Missing trino_host should fail
        with pytest.raises(ValueError, match="trino_host is required"):
            TrinoServerConfig({"trino_user": "admin"})

        # Missing trino_user should fail
        with pytest.raises(ValueError, match="trino_user is required"):
            TrinoServerConfig({"trino_host": "localhost"})

        # Valid minimal config should pass
        config = TrinoServerConfig({"trino_host": "localhost", "trino_user": "admin"})
        assert config is not None

    def test_oauth_validation(self):
        """Test OAuth configuration validation."""
        base_config = {
            "trino_host": "localhost",
            "trino_user": "admin",
            "oauth_enabled": True,
        }

        # Missing oauth_provider should fail
        with pytest.raises(ValueError, match="oauth_provider is required"):
            TrinoServerConfig(base_config)

        # Invalid oauth_provider should fail
        with pytest.raises(ValueError, match="oauth_provider must be one of"):
            TrinoServerConfig({**base_config, "oauth_provider": "invalid"})

        # HMAC provider without jwt_secret should fail
        with pytest.raises(ValueError, match="jwt_secret is required"):
            TrinoServerConfig({**base_config, "oauth_provider": "hmac"})

        # OIDC provider without required fields should fail
        with pytest.raises(ValueError, match="oidc_issuer is required"):
            TrinoServerConfig({**base_config, "oauth_provider": "google"})

        # Valid OAuth config should pass
        config = TrinoServerConfig(
            {
                **base_config,
                "oauth_provider": "google",
                "oidc_issuer": "https://accounts.google.com",
                "oidc_client_id": "client123",
            }
        )
        assert config is not None

    @patch.dict(
        os.environ,
        {
            "TRINO_HOST": "env-host",
            "TRINO_USER": "env-user",
            "TRINO_MAX_RESULTS": "2000",
        },
    )
    def test_environment_variable_precedence(self):
        """Test that config dict takes precedence over environment variables."""
        config = TrinoServerConfig(
            {"trino_host": "config-host", "trino_user": "config-user"}
        )

        template_config = config.get_template_config()

        # Config dict should take precedence
        assert template_config["trino_host"] == "config-host"
        assert template_config["trino_user"] == "config-user"

        # Environment variable should be used when not in config dict
        assert template_config["trino_max_results"] == 2000

    def test_duration_parsing(self):
        """Test duration string parsing for timeouts."""
        config = TrinoServerConfig(
            {"trino_host": "localhost", "trino_user": "admin"}, skip_validation=True
        )

        # Test various duration formats
        assert config._parse_duration("300") == 300
        assert config._parse_duration("300s") == 300
        assert config._parse_duration("5m") == 300
        assert config._parse_duration("1h") == 3600

        # Test invalid format
        with pytest.raises(ValueError):
            config._parse_duration("invalid")

    def test_connection_config_generation(self):
        """Test connection configuration generation."""
        config = TrinoServerConfig(
            {
                "trino_host": "localhost",
                "trino_user": "admin",
                "trino_port": 8080,
                "trino_password": "secret",
                "trino_catalog": "hive",
                "trino_schema": "default",
            }
        )

        conn_config = config.get_connection_config()

        assert conn_config["host"] == "localhost"
        assert conn_config["port"] == 8080
        assert conn_config["user"] == "admin"
        assert conn_config["catalog"] == "hive"
        assert conn_config["schema"] == "default"
        assert conn_config["http_scheme"] == "https"  # default
        assert conn_config["verify"] is False  # ssl_insecure default

    def test_security_config(self):
        """Test security configuration."""
        # Default read-only mode
        config = TrinoServerConfig({"trino_host": "localhost", "trino_user": "admin"})

        security = config.get_security_config()
        assert security["read_only"] is True
        assert config.is_read_only() is True

        # Write mode enabled
        config_write = TrinoServerConfig(
            {
                "trino_host": "localhost",
                "trino_user": "admin",
                "trino_allow_write_queries": True,
            }
        )

        security_write = config_write.get_security_config()
        assert security_write["read_only"] is False
        assert config_write.is_read_only() is False

    def test_query_limits(self):
        """Test query limits configuration."""
        config = TrinoServerConfig(
            {
                "trino_host": "localhost",
                "trino_user": "admin",
                "trino_query_timeout": "10m",
                "trino_max_results": 5000,
            }
        )

        limits = config.get_query_limits()
        assert limits["timeout"] == 600  # 10 minutes in seconds
        assert limits["max_results"] == 5000

    def test_config_summary_logging(self):
        """Test configuration summary logging without sensitive data."""
        config = TrinoServerConfig(
            {
                "trino_host": "localhost",
                "trino_user": "admin",
                "trino_password": "secret123",
                "jwt_secret": "supersecret",
            }
        )

        # Should not raise exception and not log sensitive data
        with patch.object(config.logger, "info") as mock_log:
            config.log_config_summary()

            # Check that logging was called
            assert mock_log.called

            # Check that sensitive data is not in log calls
            log_calls = [str(call) for call in mock_log.call_args_list]
            log_content = " ".join(log_calls)
            assert "secret123" not in log_content
            assert "supersecret" not in log_content

    def test_convenience_function(self):
        """Test convenience function for creating config."""
        config = create_trino_config({"trino_host": "localhost", "trino_user": "admin"})

        assert isinstance(config, TrinoServerConfig)
        assert config.get_template_config()["trino_host"] == "localhost"

    def test_tools_and_capabilities_updated(self):
        """Test that tools and capabilities are properly defined for new implementation."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        # Check capabilities
        capabilities = template_config["capabilities"]
        assert len(capabilities) >= 5

        capability_names = [cap["name"] for cap in capabilities]
        expected_capabilities = [
            "Catalog Discovery",
            "Schema Inspection",
            "Query Execution",
            "Access Control",
            "Multi-Source Support",
        ]

        for expected in expected_capabilities:
            assert expected in capability_names

        # Check tools
        tools = template_config["tools"]
        assert len(tools) >= 8

        tool_names = [tool["name"] for tool in tools]
        expected_tools = [
            "list_catalogs",
            "list_schemas",
            "list_tables",
            "describe_table",
            "execute_query",
            "get_query_status",
            "cancel_query",
            "get_cluster_info",
        ]

        for expected in expected_tools:
            assert expected in tool_names

        # Verify specific tool parameters
        execute_query_tool = next(t for t in tools if t["name"] == "execute_query")
        query_param = next(
            p for p in execute_query_tool["parameters"] if p["name"] == "query"
        )
        assert query_param["required"] is True
        assert query_param["type"] == "string"

        # Check that catalog and schema parameters are optional for execute_query
        optional_params = [
            p["name"]
            for p in execute_query_tool["parameters"]
            if not p.get("required", False)
        ]
        assert "catalog" in optional_params
        assert "schema" in optional_params

    def test_examples_and_integration(self):
        """Test that examples are updated for new HTTP transport."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        examples = template_config.get("examples", {})

        # Check HTTP endpoint example
        assert "http_endpoint" in examples
        assert "7090" in examples["http_endpoint"]

        # Check client integration examples
        client_integration = examples.get("client_integration", {})
        assert "fastmcp" in client_integration
        assert "curl" in client_integration
        assert "7090" in client_integration["curl"]
        assert "list_catalogs" in client_integration["fastmcp"]
