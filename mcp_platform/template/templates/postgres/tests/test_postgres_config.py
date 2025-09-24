"""
Unit tests for PostgreSQL MCP server configuration and validation.

Tests the PostgreSQL template's configuration handling, validation, and
authentication setup using the FastMCP implementation.
"""

import json
import os
import sys
from unittest.mock import patch

import pytest

# Add the template directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from config import PostgresServerConfig
except ImportError:
    # Handle import in different environments
    import importlib.util

    config_path = os.path.join(os.path.dirname(__file__), "..", "config.py")
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    PostgresServerConfig = config_module.PostgresServerConfig


class TestPostgresTemplateConfiguration:
    """Test PostgreSQL template configuration validation and processing."""

    def test_template_json_structure(self):
        """Test PostgreSQL template.json has required structure."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        # Verify required template fields
        assert template_config["name"] == "PostgreSQL MCP Server"
        assert template_config["id"] == "postgres"
        assert template_config["description"]
        assert template_config["version"] == "1.0.0"
        assert template_config["docker_image"] == "dataeverything/mcp-postgres"
        assert template_config["docker_tag"] == "latest"
        assert template_config["has_image"] is True
        assert template_config["origin"] == "internal"
        assert template_config["category"] == "Database"
        assert "postgresql" in template_config["tags"]

        # Verify transport configuration
        assert template_config["transport"]["default"] == "http"
        assert template_config["transport"]["port"] == 7080
        assert "http" in template_config["transport"]["supported"]
        assert "stdio" in template_config["transport"]["supported"]

    def test_config_schema_validation(self):
        """Test configuration schema has all required PostgreSQL fields."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        config_schema = template_config["config_schema"]
        properties = config_schema["properties"]

        # Verify required PostgreSQL connection fields
        assert "pg_host" in properties
        assert "pg_port" in properties
        assert "pg_user" in properties
        assert "pg_password" in properties
        assert "pg_database" in properties

        # Verify SSL configuration fields
        assert "ssl_mode" in properties
        assert "ssl_cert" in properties
        assert "ssl_key" in properties
        assert "ssl_ca" in properties

        # Verify SSH tunnel fields
        assert "ssh_tunnel" in properties
        assert "ssh_host" in properties
        assert "ssh_port" in properties
        assert "ssh_user" in properties
        assert "ssh_auth_method" in properties

        # Verify security and control fields
        assert "read_only" in properties
        assert "allowed_schemas" in properties
        assert "max_results" in properties
        assert "query_timeout" in properties

        # Verify required fields list
        required_fields = config_schema["required"]
        assert "pg_host" in required_fields
        assert "pg_user" in required_fields

    def test_tools_configuration(self):
        """Test tools are properly configured in template."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = template_config["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Verify PostgreSQL-specific tools are present
        expected_tools = [
            "list_schemas",
            "list_tables",
            "describe_table",
            "list_columns",
            "execute_query",
            "explain_query",
            "get_database_info",
            "get_table_stats",
            "list_indexes",
            "list_constraints",
            "test_connection",
            "get_connection_info",
        ]

        for tool in expected_tools:
            assert tool in tool_names, f"Tool {tool} missing from template"

    def test_minimal_config_validation(self):
        """Test configuration with minimal required fields."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
        }

        # Should not raise exception with minimal valid config
        config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)
        template_config = config.get_template_config()

        assert template_config["pg_host"] == "localhost"
        assert template_config["pg_user"] == "postgres"
        assert template_config["pg_password"] == "secret"
        assert template_config["pg_database"] == "postgres"  # default
        assert template_config["pg_port"] == 5432  # default
        assert template_config["ssl_mode"] == "prefer"  # default

    def test_config_validation_missing_required_fields(self):
        """Test configuration validation fails with missing required fields."""
        # Missing pg_host
        config_dict = {"pg_user": "postgres", "pg_password": "secret"}

        with pytest.raises(ValueError, match="pg_host is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

        # Missing pg_user
        config_dict = {"pg_host": "localhost", "pg_password": "secret"}

        with pytest.raises(ValueError, match="pg_user is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

    def test_ssl_mode_validation(self):
        """Test SSL mode validation."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssl_mode": "invalid_mode",
        }

        with pytest.raises(ValueError, match="ssl_mode must be one of"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

    def test_ssh_tunnel_configuration_validation(self):
        """Test SSH tunnel configuration validation."""
        # SSH tunnel enabled but missing required fields
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssh_tunnel": True,
        }

        with pytest.raises(ValueError, match="ssh_host is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

        # SSH tunnel with missing user
        config_dict.update({"ssh_host": "bastion.example.com"})

        with pytest.raises(ValueError, match="ssh_user is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

    def test_ssh_key_authentication_validation(self):
        """Test SSH key authentication validation."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssh_tunnel": True,
            "ssh_host": "bastion.example.com",
            "ssh_user": "admin",
            "ssh_auth_method": "key",
        }

        with pytest.raises(ValueError, match="ssh_key_file is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

    def test_port_validation(self):
        """Test port number validation."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "pg_port": 70000,  # Invalid port
        }

        with pytest.raises(ValueError, match="pg_port must be between 1 and 65535"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

    def test_timeout_validation(self):
        """Test timeout validation."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "query_timeout": -10,  # Invalid timeout
        }

        with pytest.raises(ValueError, match="query_timeout must be positive"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

    def test_connection_string_generation(self):
        """Test PostgreSQL connection string generation."""
        config_dict = {
            "pg_host": "db.example.com",
            "pg_port": 5433,
            "pg_user": "testuser",
            "pg_password": "secret123",
            "pg_database": "testdb",
            "ssl_mode": "require",
        }

        config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)
        connection_string = config.get_connection_string()

        assert "postgresql+psycopg://testuser:" in connection_string
        assert "@db.example.com:5433/testdb" in connection_string
        assert "sslmode=require" in connection_string
        assert "connect_timeout=10" in connection_string
        assert "application_name=mcp-postgres-server" in connection_string

    def test_connection_string_with_ssl_certificates(self):
        """Test connection string generation with SSL certificates."""
        config_dict = {
            "pg_host": "secure-db.example.com",
            "pg_user": "secureuser",
            "pg_password": "secret",
            "ssl_mode": "verify-full",
            "ssl_cert": "/path/to/client.crt",
            "ssl_key": "/path/to/client.key",
            "ssl_ca": "/path/to/ca.crt",
        }

        config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)
        connection_string = config.get_connection_string()

        assert "sslmode=verify-full" in connection_string
        assert "sslcert=/path/to/client.crt" in connection_string
        assert "sslkey=/path/to/client.key" in connection_string
        assert "sslrootcert=/path/to/ca.crt" in connection_string

    def test_ssh_config_generation(self):
        """Test SSH configuration generation."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssh_tunnel": True,
            "ssh_host": "bastion.example.com",
            "ssh_port": 2222,
            "ssh_user": "admin",
            "ssh_password": "sshsecret",
            "ssh_auth_method": "password",
        }

        config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)
        ssh_config = config.get_ssh_config()

        assert ssh_config is not None
        assert ssh_config["ssh_host"] == "bastion.example.com"
        assert ssh_config["ssh_port"] == 2222
        assert ssh_config["ssh_user"] == "admin"
        assert ssh_config["ssh_password"] == "sshsecret"
        assert ssh_config["ssh_auth_method"] == "password"
        assert ssh_config["remote_host"] == "localhost"
        assert ssh_config["remote_port"] == 5432

    def test_ssh_config_disabled(self):
        """Test SSH configuration when tunnel is disabled."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssh_tunnel": False,
        }

        config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)
        ssh_config = config.get_ssh_config()

        assert ssh_config is None

    def test_environment_variable_mapping(self):
        """Test environment variable mapping."""
        with patch.dict(
            os.environ,
            {
                "PG_HOST": "env-host",
                "PG_USER": "env-user",
                "PG_PASSWORD": "env-password",
                "PG_READ_ONLY": "false",
                "PG_MAX_RESULTS": "2000",
            },
        ):
            config = PostgresServerConfig(config_dict={}, skip_validation=True)
            template_config = config.get_template_config()

            assert template_config["pg_host"] == "env-host"
            assert template_config["pg_user"] == "env-user"
            assert template_config["pg_password"] == "env-password"
            assert template_config["read_only"] is False
            assert template_config["max_results"] == 2000

    def test_config_precedence(self):
        """Test configuration precedence (config_dict > env > defaults)."""
        with patch.dict(os.environ, {"PG_HOST": "env-host", "PG_PORT": "3306"}):
            config_dict = {"pg_host": "config-host", "pg_user": "config-user"}

            config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)
            template_config = config.get_template_config()

            # config_dict takes precedence over environment
            assert template_config["pg_host"] == "config-host"
            # environment takes precedence over defaults
            assert template_config["pg_port"] == 3306
            # config_dict value used
            assert template_config["pg_user"] == "config-user"
            # default value for unspecified
            assert template_config["pg_database"] == "postgres"

    def test_boolean_environment_variables(self):
        """Test boolean environment variable parsing."""
        test_cases = [
            ("true", True),
            ("True", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("off", False),
        ]

        for env_value, expected in test_cases:
            with patch.dict(os.environ, {"PG_READ_ONLY": env_value}):
                config = PostgresServerConfig(
                    config_dict={"pg_host": "localhost", "pg_user": "postgres"},
                    skip_validation=True,
                )

                template_config = config.get_template_config()
                assert template_config["read_only"] == expected

    def test_utility_methods(self):
        """Test utility methods."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "read_only": False,
            "allowed_schemas": "public,analytics",
            "query_timeout": 120,
            "max_results": 500,
        }

        config = PostgresServerConfig(config_dict=config_dict, skip_validation=True)

        assert config.is_read_only() is False
        assert config.get_allowed_schemas() == "public,analytics"
        assert config.get_query_timeout() == 120
        assert config.get_max_results() == 500

    @patch("logging.getLogger")
    def test_logging_setup(self, mock_logger):
        """Test logging configuration setup."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "log_level": "debug",
        }

        PostgresServerConfig(config_dict=config_dict, skip_validation=True)

        # Verify logger was configured
        mock_logger.assert_called()

    def test_ssl_certificate_validation(self):
        """Test SSL certificate validation requirements."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssl_mode": "verify-ca",
            # Missing ssl_ca
        }

        with pytest.raises(ValueError, match="ssl_ca is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)

        # Test certificate with key requirement
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "postgres",
            "pg_password": "secret",
            "ssl_cert": "/path/to/cert.pem",
            # Missing ssl_key
        }

        with pytest.raises(ValueError, match="ssl_key is required"):
            PostgresServerConfig(config_dict=config_dict, skip_validation=False)
