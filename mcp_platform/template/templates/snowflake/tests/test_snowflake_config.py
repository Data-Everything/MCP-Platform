"""
Unit tests for Snowflake MCP server configuration.

Tests configuration validation, authentication methods, and parameter handling
for the Snowflake template.
"""

import json
import os
# Add the template directory to the path for imports
import sys
import tempfile
import unittest
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import SnowflakeServerConfig


class TestSnowflakeTemplateConfiguration:
    """Test Snowflake template configuration validation and processing."""

    def test_template_json_structure(self):
        """Test Snowflake template.json has required structure."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        # Verify required template fields
        assert template_config["name"] == "Snowflake MCP Server"
        assert template_config["id"] == "snowflake"
        assert template_config["description"]
        assert template_config["version"] == "1.0.0"
        assert template_config["docker_image"] == "dataeverything/mcp-snowflake"
        assert template_config["docker_tag"] == "latest"
        assert template_config["has_image"] is True
        assert template_config["origin"] == "internal"
        assert template_config["category"] == "Database"
        assert "snowflake" in template_config["tags"]

        # Verify transport configuration
        assert template_config["transport"]["default"] == "http"
        assert "http" in template_config["transport"]["supported"]
        assert "stdio" in template_config["transport"]["supported"]
        assert template_config["transport"]["port"] == 7081

    def test_config_schema_validation(self):
        """Test configuration schema has all required Snowflake fields."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        config_schema = template_config["config_schema"]
        properties = config_schema["properties"]

        # Verify required Snowflake connection fields
        assert "account" in properties
        assert "user" in properties
        assert "password" in properties
        assert "authenticator" in properties

        # Verify authentication method fields
        assert "private_key" in properties
        assert "private_key_file" in properties
        assert "private_key_passphrase" in properties
        assert "oauth_token" in properties
        assert "jwt_token" in properties
        assert "okta_endpoint" in properties

        # Verify Snowflake-specific fields
        assert "database" in properties
        assert "schema" in properties
        assert "warehouse" in properties
        assert "role" in properties
        assert "region" in properties

        # Verify configuration options
        assert "connection_timeout" in properties
        assert "query_timeout" in properties
        assert "max_results" in properties
        assert "read_only" in properties
        assert "allowed_databases" in properties
        assert "allowed_schemas" in properties

        # Verify SSL/security fields
        assert "insecure_mode" in properties
        assert "ocsp_response_cache_filename" in properties
        assert "client_session_keep_alive" in properties

    def test_tools_configuration(self):
        """Test tools are properly configured in template."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = template_config["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Verify essential Snowflake tools
        expected_tools = [
            "list_databases",
            "list_schemas",
            "list_tables",
            "describe_table",
            "list_columns",
            "execute_query",
            "explain_query",
            "get_warehouse_info",
            "list_warehouses",
            "get_account_info",
            "get_current_role",
            "list_roles",
            "get_table_stats",
            "test_connection",
            "get_connection_info",
        ]

        for tool in expected_tools:
            assert tool in tool_names, f"Missing tool: {tool}"

    def test_minimal_config_validation(self):
        """Test configuration with minimal required fields."""
        config_dict = {
            "account": "myorg-account",
            "user": "testuser",
            "password": "testpass",
        }

        config = SnowflakeServerConfig(config_dict=config_dict, skip_validation=True)
        template_config = config.get_template_config()

        assert template_config["account"] == "myorg-account"
        assert template_config["user"] == "testuser"
        assert template_config["password"] == "testpass"
        assert template_config["authenticator"] == "snowflake"  # default

    def test_config_validation_missing_required_fields(self):
        """Test configuration validation fails with missing required fields."""
        # Missing account
        with pytest.raises(ValueError, match="account.*required"):
            SnowflakeServerConfig(config_dict={"user": "test"})

        # Missing user
        with pytest.raises(ValueError, match="user.*required"):
            SnowflakeServerConfig(config_dict={"account": "test-account"})

    def test_authenticator_validation(self):
        """Test authenticator validation."""
        base_config = {"account": "test-account", "user": "testuser"}

        # Valid authenticators
        valid_authenticators = [
            "snowflake",
            "oauth",
            "externalbrowser",
            "okta_endpoint",
            "jwt",
        ]
        for auth in valid_authenticators:
            config_dict = {**base_config, "authenticator": auth}
            if auth == "snowflake":
                config_dict["password"] = "test"
            elif auth == "oauth":
                config_dict["oauth_token"] = "token"
            elif auth == "jwt":
                config_dict["private_key"] = "test-key"
            elif auth == "okta_endpoint":
                config_dict["okta_endpoint"] = "https://test.okta.com"

            # Should not raise for valid authenticators
            try:
                SnowflakeServerConfig(config_dict=config_dict)
            except ValueError as e:
                if "authenticator" in str(e):
                    pytest.fail(f"Valid authenticator {auth} was rejected: {e}")

        # Invalid authenticator
        with pytest.raises(ValueError, match="authenticator must be one of"):
            SnowflakeServerConfig(
                config_dict={**base_config, "authenticator": "invalid"}
            )

    def test_password_authentication_validation(self):
        """Test password authentication validation."""
        base_config = {"account": "test-account", "user": "testuser"}

        # Password required for snowflake authenticator
        with pytest.raises(ValueError, match="password.*required"):
            SnowflakeServerConfig(
                config_dict={**base_config, "authenticator": "snowflake"}
            )

        # Valid password auth
        config = SnowflakeServerConfig(
            config_dict={
                **base_config,
                "authenticator": "snowflake",
                "password": "testpass",
            }
        )
        assert config.get("authenticator") == "snowflake"

    def test_jwt_authentication_validation(self):
        """Test JWT (key-pair) authentication validation."""
        base_config = {
            "account": "test-account",
            "user": "testuser",
            "authenticator": "jwt",
        }

        # Either private_key or private_key_file required
        with pytest.raises(ValueError, match="private_key.*required"):
            SnowflakeServerConfig(config_dict=base_config)

        # Valid with private_key content
        config = SnowflakeServerConfig(
            config_dict={
                **base_config,
                "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",
            },
            skip_validation=True,
        )  # Skip crypto validation for test
        assert config.get("authenticator") == "jwt"

        # Valid with private_key_file (need to create temp file)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
            f.write("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----")
            f.flush()

            config = SnowflakeServerConfig(
                config_dict={**base_config, "private_key_file": f.name},
                skip_validation=True,
            )  # Skip crypto validation for test
            assert config.get("authenticator") == "jwt"

            os.unlink(f.name)

    def test_oauth_authentication_validation(self):
        """Test OAuth authentication validation."""
        base_config = {
            "account": "test-account",
            "user": "testuser",
            "authenticator": "oauth",
        }

        # oauth_token required
        with pytest.raises(ValueError, match="oauth_token.*required"):
            SnowflakeServerConfig(config_dict=base_config)

        # Valid OAuth config
        config = SnowflakeServerConfig(
            config_dict={**base_config, "oauth_token": "oauth-token-123"}
        )
        assert config.get("authenticator") == "oauth"

    def test_okta_authentication_validation(self):
        """Test Okta SSO authentication validation."""
        base_config = {
            "account": "test-account",
            "user": "testuser",
            "authenticator": "okta_endpoint",
        }

        # okta_endpoint required
        with pytest.raises(ValueError, match="okta_endpoint.*required"):
            SnowflakeServerConfig(config_dict=base_config)

        # Valid Okta config
        config = SnowflakeServerConfig(
            config_dict={**base_config, "okta_endpoint": "https://myorg.okta.com"}
        )
        assert config.get("authenticator") == "okta_endpoint"

        # Invalid URL format
        with pytest.raises(ValueError, match="valid HTTP"):
            SnowflakeServerConfig(
                config_dict={**base_config, "okta_endpoint": "invalid-url"}
            )

    def test_externalbrowser_authentication_validation(self):
        """Test external browser authentication validation."""
        config = SnowflakeServerConfig(
            config_dict={
                "account": "test-account",
                "user": "testuser",
                "authenticator": "externalbrowser",
            }
        )
        assert config.get("authenticator") == "externalbrowser"

    def test_account_format_validation(self):
        """Test account identifier format validation."""
        base_config = {"user": "testuser", "password": "testpass"}

        # Valid account formats
        valid_accounts = [
            "myorg-account", "myorg.us-east-1", "AB12345", "test_account"
        ]
        for account in valid_accounts:
            config = SnowflakeServerConfig(
                config_dict={**base_config, "account": account}
            )
            assert config.get("account") == account

        # Invalid account format
        with pytest.raises(ValueError, match="Invalid.*account format"):
            SnowflakeServerConfig(
                config_dict={**base_config, "account": "invalid@account"}
            )

    def test_timeout_validation(self):
        """Test timeout validation."""
        base_config = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        # Valid connection timeout
        config = SnowflakeServerConfig(
            config_dict={**base_config, "connection_timeout": 120}
        )
        assert config.get("connection_timeout") == 120

        # Invalid connection timeout (too low)
        with pytest.raises(ValueError, match="connection_timeout.*between"):
            SnowflakeServerConfig(config_dict={**base_config, "connection_timeout": 5})

        # Invalid connection timeout (too high)
        with pytest.raises(ValueError, match="connection_timeout.*between"):
            SnowflakeServerConfig(
                config_dict={**base_config, "connection_timeout": 1000}
            )

        # Valid query timeout
        config = SnowflakeServerConfig(
            config_dict={**base_config, "query_timeout": 1800}
        )
        assert config.get("query_timeout") == 1800

        # Invalid query timeout (too low)
        with pytest.raises(ValueError, match="query_timeout.*between"):
            SnowflakeServerConfig(config_dict={**base_config, "query_timeout": 30})

        # Invalid query timeout (too high)
        with pytest.raises(ValueError, match="query_timeout.*between"):
            SnowflakeServerConfig(config_dict={**base_config, "query_timeout": 30000})

    def test_max_results_validation(self):
        """Test max_results validation."""
        base_config = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        # Valid max_results
        config = SnowflakeServerConfig(config_dict={**base_config, "max_results": 5000})
        assert config.get("max_results") == 5000

        # Invalid max_results (too low)
        with pytest.raises(ValueError, match="max_results.*between"):
            SnowflakeServerConfig(config_dict={**base_config, "max_results": 0})

        # Invalid max_results (too high)
        with pytest.raises(ValueError, match="max_results.*between"):
            SnowflakeServerConfig(config_dict={**base_config, "max_results": 200000})

    def test_connection_params_generation(self):
        """Test connection parameters generation for different auth methods."""
        # Password authentication
        config = SnowflakeServerConfig(
            config_dict={
                "account": "myorg-account",
                "user": "testuser",
                "password": "testpass",
                "database": "TESTDB",
                "schema": "PUBLIC",
                "warehouse": "COMPUTE_WH",
                "role": "TESTROLE",
            }
        )

        params = config.get_connection_params()
        assert params["account"] == "myorg-account"
        assert params["user"] == "testuser"
        assert params["password"] == "testpass"
        assert params["database"] == "TESTDB"
        assert params["schema"] == "PUBLIC"
        assert params["warehouse"] == "COMPUTE_WH"
        assert params["role"] == "TESTROLE"
        assert params["authenticator"] == "snowflake"

        # JWT authentication
        config = SnowflakeServerConfig(
            config_dict={
                "account": "myorg-account",
                "user": "testuser",
                "authenticator": "jwt",
                "private_key": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",
            },
            skip_validation=True,
        )

        params = config.get_connection_params()
        assert params["authenticator"] == "jwt"
        assert "private_key" in params

    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "env-account",
            "SNOWFLAKE_USER": "env-user",
            "SNOWFLAKE_PASSWORD": "env-password",
            "SNOWFLAKE_DATABASE": "env-db",
            "SNOWFLAKE_WAREHOUSE": "env-wh",
            "SNOWFLAKE_READ_ONLY": "false",
            "SNOWFLAKE_MAX_RESULTS": "5000",
        },
    )
    def test_environment_variable_integration(self):
        """Test integration with environment variables."""
        config = SnowflakeServerConfig(config_dict={}, skip_validation=True)
        template_config = config.get_template_config()

        assert template_config["account"] == "env-account"
        assert template_config["user"] == "env-user"
        assert template_config["password"] == "env-password"
        assert template_config["database"] == "env-db"
        assert template_config["warehouse"] == "env-wh"
        assert template_config["read_only"] is False
        assert template_config["max_results"] == 5000

    def test_session_parameters_handling(self):
        """Test session parameters configuration."""
        config = SnowflakeServerConfig(
            config_dict={
                "account": "test-account",
                "user": "testuser",
                "password": "testpass",
                "session_parameters": {"QUERY_TIMEOUT": "3600", "AUTOCOMMIT": "false"},
            }
        )

        params = config.get_connection_params()
        assert "session_parameters" in params
        assert params["session_parameters"]["QUERY_TIMEOUT"] == "3600"
        assert params["session_parameters"]["AUTOCOMMIT"] == "false"
