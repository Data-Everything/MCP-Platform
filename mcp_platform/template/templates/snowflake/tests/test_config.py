#!/usr/bin/env python3
"""
Unit tests for Snowflake MCP Server configuration.

Tests configuration loading, validation, environment variable mapping,
and double underscore notation processing.
"""

import os

# Import the configuration class
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.append(str(Path(__file__).parent.parent))
from config import SnowflakeServerConfig


class TestSnowflakeServerConfig:
    """Test cases for SnowflakeServerConfig."""

    def test_default_configuration(self):
        """Test default configuration values."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "testaccount"}, clear=True):
            config = SnowflakeServerConfig()

            assert config.get_snowflake_account() == "testaccount"
            assert config.get_snowflake_authenticator() == "snowflake"
            assert config.get_read_only() is True
            assert config.get_connection_timeout() == 60
            assert config.get_query_timeout() == 300

    def test_environment_variable_mapping(self):
        """Test environment variable mapping."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "mycompany",
            "SNOWFLAKE_USER": "testuser",
            "SNOWFLAKE_PASSWORD": "testpass",
            "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
            "SNOWFLAKE_READ_ONLY": "false",
            "SNOWFLAKE_CONNECTION_TIMEOUT": "120",
            "MCP_LOG_LEVEL": "debug",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeServerConfig()

            assert config.get_snowflake_account() == "mycompany"
            assert config.get_snowflake_user() == "testuser"
            assert config.get_snowflake_password() == "testpass"
            assert config.get_snowflake_warehouse() == "COMPUTE_WH"
            assert config.get_read_only() is False
            assert config.get_connection_timeout() == 120

    def test_config_dict_override(self):
        """Test configuration override via config_dict."""
        with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "env_account"}, clear=True):
            config_dict = {
                "snowflake_account": "dict_account",
                "snowflake_user": "dict_user",
                "read_only": False,
            }

            config = SnowflakeServerConfig(config_dict=config_dict)

            # config_dict should override environment variables
            assert config.get_snowflake_account() == "dict_account"
            assert config.get_snowflake_user() == "dict_user"
            assert config.get_read_only() is False

    def test_required_account_validation(self):
        """Test that missing Snowflake account raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Snowflake account is required"):
                SnowflakeServerConfig()

    def test_authentication_validation(self):
        """Test authentication method validation."""
        base_env = {"SNOWFLAKE_ACCOUNT": "testaccount"}

        # Test snowflake auth requires username and password
        with patch.dict(os.environ, base_env, clear=True):
            with pytest.raises(ValueError, match="Username and password are required"):
                SnowflakeServerConfig()

        # Test oauth auth requires token
        oauth_env = {**base_env, "SNOWFLAKE_AUTHENTICATOR": "oauth"}
        with patch.dict(os.environ, oauth_env, clear=True):
            with pytest.raises(ValueError, match="OAuth token is required"):
                SnowflakeServerConfig()

        # Test JWT auth requires username and private key
        jwt_env = {
            **base_env,
            "SNOWFLAKE_AUTHENTICATOR": "snowflake_jwt",
            "SNOWFLAKE_USER": "user",
        }
        with patch.dict(os.environ, jwt_env, clear=True):
            with pytest.raises(
                ValueError, match="Username and private key are required"
            ):
                SnowflakeServerConfig()

    def test_valid_authentication_configurations(self):
        """Test valid authentication configurations."""
        base_env = {"SNOWFLAKE_ACCOUNT": "testaccount"}

        # Valid username/password auth
        valid_env = {**base_env, "SNOWFLAKE_USER": "user", "SNOWFLAKE_PASSWORD": "pass"}
        with patch.dict(os.environ, valid_env, clear=True):
            config = SnowflakeServerConfig()
            assert config.get_snowflake_authenticator() == "snowflake"

        # Valid OAuth auth
        oauth_env = {
            **base_env,
            "SNOWFLAKE_AUTHENTICATOR": "oauth",
            "SNOWFLAKE_OAUTH_TOKEN": "token",
        }
        with patch.dict(os.environ, oauth_env, clear=True):
            config = SnowflakeServerConfig()
            assert config.get_snowflake_authenticator() == "oauth"

        # Valid external browser auth (no additional params needed)
        browser_env = {
            **base_env,
            "SNOWFLAKE_AUTHENTICATOR": "externalbrowser",
            "SNOWFLAKE_USER": "user",
        }
        with patch.dict(os.environ, browser_env, clear=True):
            config = SnowflakeServerConfig()
            assert config.get_snowflake_authenticator() == "externalbrowser"

    def test_filter_pattern_validation(self):
        """Test regex filter pattern validation."""
        base_env = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
        }

        # Valid regex patterns should not raise errors
        valid_env = {
            **base_env,
            "SNOWFLAKE_DATABASE_FILTER": "^(PROD|DEV)_.*",
            "SNOWFLAKE_SCHEMA_FILTER": "^PUBLIC$",
        }
        with patch.dict(os.environ, valid_env, clear=True):
            config = SnowflakeServerConfig()
            assert config.get_database_filter_pattern() == "^(PROD|DEV)_.*"
            assert config.get_schema_filter_pattern() == "^PUBLIC$"

    def test_double_underscore_notation(self):
        """Test double underscore notation processing."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
        }

        config_dict = {
            "snowflake__warehouse": "TEST_WH",
            "snowflake__read_only": "false",
            "tools__0__custom_field": "custom_value",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeServerConfig(config_dict=config_dict)

            # Standard config properties should be processed
            assert config.get_snowflake_warehouse() == "TEST_WH"
            assert config.get_read_only() is False

    def test_type_coercion(self):
        """Test automatic type coercion for configuration values."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
        }

        config_dict = {
            "read_only": "true",  # string -> boolean
            "connection_timeout": "90",  # string -> integer
            "query_timeout": "450",  # string -> integer
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeServerConfig(config_dict=config_dict)

            assert config.get_read_only() is True
            assert config.get_connection_timeout() == 90
            assert config.get_query_timeout() == 450

    def test_override_environment_variables(self):
        """Test override environment variables processing."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
            "OVERRIDE_snowflake_warehouse": "OVERRIDE_WH",
            "MCP_OVERRIDE_read_only": "false",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeServerConfig()

            # Override variables should be processed
            assert config.get_snowflake_warehouse() == "OVERRIDE_WH"
            assert config.get_read_only() is False

    def test_template_data_loading(self):
        """Test template data loading and modification."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeServerConfig()
            template_data = config.get_template_data()

            assert "name" in template_data
            assert "version" in template_data
            assert "config_schema" in template_data
            assert template_data["name"] == "Snowflake MCP Server"

    def test_config_precedence(self):
        """Test configuration value precedence order."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "env_account",
            "SNOWFLAKE_WAREHOUSE": "env_warehouse",
        }

        config_dict = {
            "snowflake_account": "dict_account"
            # Note: snowflake_warehouse not in config_dict
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = SnowflakeServerConfig(config_dict=config_dict)

            # config_dict should override env vars
            assert config.get_snowflake_account() == "dict_account"
            # env vars should be used when not in config_dict
            assert config.get_snowflake_warehouse() == "env_warehouse"

    def test_boolean_conversion_edge_cases(self):
        """Test boolean conversion for various string values."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
        }

        # Test various boolean representations
        test_cases = [
            ("true", True),
            ("TRUE", True),
            ("True", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("FALSE", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("off", False),
        ]

        for bool_str, expected in test_cases:
            config_dict = {"read_only": bool_str}
            with patch.dict(os.environ, env_vars, clear=True):
                config = SnowflakeServerConfig(config_dict=config_dict)
                assert (
                    config.get_read_only() is expected
                ), f"Failed for input: {bool_str}"

    def test_invalid_boolean_conversion(self):
        """Test handling of invalid boolean values."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "testaccount",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "pass",
        }

        config_dict = {"read_only": "invalid_boolean"}

        with patch.dict(os.environ, env_vars, clear=True):
            # Should not raise an exception, but use the original value or default
            SnowflakeServerConfig(config_dict=config_dict)
            # The original implementation might handle this differently
            # This test ensures no crash occurs


if __name__ == "__main__":
    pytest.main([__file__])
