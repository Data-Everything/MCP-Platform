#!/usr/bin/env python3
"""
Unit tests for Databricks MCP Server configuration.

Tests configuration loading, validation, and authentication method handling.
"""

import os
from unittest.mock import patch

import pytest

from mcp_platform.template.templates.databricks.config import \
    DatabricksServerConfig


class TestDatabricksServerConfig:
    """Test the DatabricksServerConfig class."""

    def test_init_with_minimal_config(self):
        """Test initialization with minimal required configuration."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "dapi1234567890abcdef",
        }

        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()

        assert (
            template_config["workspace_host"]
            == "https://dbc-12345.cloud.databricks.com"
        )
        assert template_config["access_token"] == "dapi1234567890abcdef"
        assert template_config["auth_method"] == "pat"  # default
        assert template_config["read_only"] is True  # default

    def test_authentication_methods(self):
        """Test different authentication method configurations."""
        # PAT authentication
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "pat",
            "access_token": "dapi1234567890abcdef",
        }
        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()
        assert template_config["auth_method"] == "pat"
        assert template_config["access_token"] == "dapi1234567890abcdef"

        # OAuth authentication
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "oauth",
            "oauth_token": "oauth_token_123",
        }
        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()
        assert template_config["auth_method"] == "oauth"
        assert template_config["oauth_token"] == "oauth_token_123"

        # Username/password authentication
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "username_password",
            "username": "user@example.com",
            "password": "password123",
        }
        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()
        assert template_config["auth_method"] == "username_password"
        assert template_config["username"] == "user@example.com"
        assert template_config["password"] == "password123"

    def test_read_only_mode_configuration(self):
        """Test read-only mode configuration and validation."""
        # Read-only enabled (default)
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
        }
        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()
        assert template_config["read_only"] is True

        # Read-only explicitly disabled
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "read_only": False,
        }
        # Should log a warning when read_only is disabled
        with patch.object(
            DatabricksServerConfig, "_validate_databricks_config"
        ):
            config = DatabricksServerConfig(config_dict=config_dict)
            template_config = config.get_template_config()
            assert template_config["read_only"] is False

    def test_access_filtering_configuration(self):
        """Test database and schema filtering configuration."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "allowed_databases": "analytics,reporting,dev_.*",
            "allowed_schemas": "default,public,staging",
        }

        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()

        assert template_config["allowed_databases"] == "analytics,reporting,dev_.*"
        assert template_config["allowed_schemas"] == "default,public,staging"

    def test_performance_settings(self):
        """Test performance-related configuration settings."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "connection_timeout": 60,
            "max_rows": 5000,
            "enable_cache": False,
        }

        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()

        assert template_config["connection_timeout"] == 60
        assert template_config["max_rows"] == 5000
        assert template_config["enable_cache"] is False

    def test_environment_variable_mapping(self):
        """Test configuration loading from environment variables."""
        env_vars = {
            "DATABRICKS_HOST": "https://env-workspace.cloud.databricks.com",
            "DATABRICKS_TOKEN": "env_token_123",
            "DATABRICKS_AUTH_METHOD": "pat",
            "DATABRICKS_READ_ONLY": "false",
            "DATABRICKS_MAX_ROWS": "2000",
        }

        with patch.dict(os.environ, env_vars):
            config = DatabricksServerConfig(config_dict={})
            template_config = config.get_template_config()

            assert (
                template_config["workspace_host"]
                == "https://env-workspace.cloud.databricks.com"
            )
            assert template_config["access_token"] == "env_token_123"
            assert template_config["auth_method"] == "pat"
            assert template_config["read_only"] == "false"  # Env vars are strings
            assert template_config["max_rows"] == "2000"  # Env vars are strings

    def test_type_coercion(self):
        """Test type coercion for configuration values."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "connection_timeout": "45",  # string should be converted to int
            "max_rows": "3000",  # string should be converted to int
            "read_only": "true",  # string should be converted to bool
            "enable_cache": "false",  # string should be converted to bool
        }

        config = DatabricksServerConfig(config_dict=config_dict)
        template_config = config.get_template_config()

        assert isinstance(template_config["connection_timeout"], int)
        assert template_config["connection_timeout"] == 45
        assert isinstance(template_config["max_rows"], int)
        assert template_config["max_rows"] == 3000
        assert isinstance(template_config["read_only"], bool)
        assert template_config["read_only"] is True
        assert isinstance(template_config["enable_cache"], bool)
        assert template_config["enable_cache"] is False

    def test_sensitive_field_masking(self):
        """Test that sensitive fields are properly masked."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "dapi1234567890abcdef",
            "oauth_token": "oauth_token_secret",
            "password": "secret_password",
        }

        config = DatabricksServerConfig(config_dict=config_dict)

        # Check sensitive field detection
        assert config.is_sensitive_field("access_token") is True
        assert config.is_sensitive_field("oauth_token") is True
        assert config.is_sensitive_field("password") is True
        assert config.is_sensitive_field("workspace_host") is False

        # Check sanitized config masks sensitive fields
        sanitized = config.get_sanitized_config()
        assert sanitized["access_token"] == "********"
        assert sanitized["oauth_token"] == "********"
        assert sanitized["password"] == "********"
        assert sanitized["workspace_host"] == "https://dbc-12345.cloud.databricks.com"

    def test_double_underscore_notation(self):
        """Test double underscore notation for nested configuration."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "databricks__custom_setting": "custom_value",
            "template__name": "Custom Databricks Server",
        }

        config = DatabricksServerConfig(config_dict=config_dict)
        template_data = config.get_template_data()

        # Check that template overrides are applied
        assert template_data["name"] == "Custom Databricks Server"

    def test_validation_errors(self):
        """Test configuration validation error handling."""
        # Missing required workspace_host
        config_dict = {"access_token": "token123"}

        # Should not raise error during init, but validation should catch it
        DatabricksServerConfig(config_dict=config_dict)
        # The validation happens in _validate_config which is called during init

        # Test invalid auth method
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "invalid_method",
        }

        # This should log warnings during validation
        DatabricksServerConfig(config_dict=config_dict)

    def test_regex_pattern_validation(self):
        """Test validation of regex patterns in filtering."""
        config_dict = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "allowed_databases": "valid_pattern,invalid_[pattern",  # invalid regex
            "allowed_schemas": "valid.*,another_[invalid",  # invalid regex
        }

        # Should not raise error but should log warnings for invalid patterns
        with patch("logging.Logger.warning"):
            DatabricksServerConfig(config_dict=config_dict)
            # Validation should have been called and warnings logged

    def test_template_data_structure(self):
        """Test that template data structure is properly loaded."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        # Check basic template structure
        assert "name" in template_data
        assert "description" in template_data
        assert "version" in template_data
        assert "config_schema" in template_data
        assert "tools" in template_data
        assert "capabilities" in template_data

        # Check config schema structure
        config_schema = template_data["config_schema"]
        assert "properties" in config_schema
        assert "workspace_host" in config_schema["properties"]
        assert "auth_method" in config_schema["properties"]
        assert "read_only" in config_schema["properties"]


if __name__ == "__main__":
    pytest.main([__file__])
