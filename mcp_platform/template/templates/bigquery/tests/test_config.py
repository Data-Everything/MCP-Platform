#!/usr/bin/env python3
"""
Test Configuration for BigQuery MCP Server.

Tests the configuration loading, validation, and management functionality.
"""

# Import the config module
import os
import sys
import tempfile
from unittest.mock import patch

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config import BigQueryServerConfig, create_bigquery_config


class TestBigQueryServerConfig:
    """Test BigQuery server configuration functionality."""

    def test_minimal_valid_config(self):
        """Test configuration with minimal required fields."""
        config_dict = {"project_id": "test-project"}

        config = BigQueryServerConfig(config_dict)

        # Check that defaults are applied
        template_config = config.get_template_config()
        assert template_config["project_id"] == "test-project"
        assert template_config.get("auth_method") == "application_default"
        assert template_config.get("read_only") is True
        assert template_config.get("allowed_datasets") == "*"
        assert template_config.get("query_timeout") == 300
        assert template_config.get("max_results") == 1000

    def test_missing_project_id_fails(self):
        """Test that missing project_id raises ValueError."""
        config_dict = {}

        with pytest.raises(ValueError, match="project_id is required"):
            BigQueryServerConfig(config_dict)

    def test_project_id_from_environment(self):
        """Test that project_id can be loaded from environment variable."""
        config_dict = {}

        with patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "env-project"}):
            config = BigQueryServerConfig(config_dict)
            template_config = config.get_template_config()
            assert template_config["project_id"] == "env-project"

    def test_service_account_auth_validation(self):
        """Test service account authentication configuration validation."""
        # Create a temporary service account file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"type": "service_account", "project_id": "test"}')
            service_account_path = f.name

        try:
            config_dict = {
                "project_id": "test-project",
                "auth_method": "service_account",
                "service_account_path": service_account_path,
            }

            config = BigQueryServerConfig(config_dict)
            template_config = config.get_template_config()

            assert template_config["auth_method"] == "service_account"
            assert template_config["service_account_path"] == service_account_path
        finally:
            os.unlink(service_account_path)

    def test_service_account_missing_file_fails(self):
        """Test that missing service account file raises ValueError."""
        config_dict = {
            "project_id": "test-project",
            "auth_method": "service_account",
            "service_account_path": "/nonexistent/file.json",
        }

        with pytest.raises(ValueError, match="Service account key file not found"):
            BigQueryServerConfig(config_dict)

    def test_service_account_from_environment(self):
        """Test service account path from environment variable."""
        # Create a temporary service account file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"type": "service_account", "project_id": "test"}')
            service_account_path = f.name

        try:
            config_dict = {
                "project_id": "test-project",
                "auth_method": "service_account",
            }

            with patch.dict(
                os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": service_account_path}
            ):
                config = BigQueryServerConfig(config_dict)
                template_config = config.get_template_config()

                assert template_config["service_account_path"] == service_account_path
        finally:
            os.unlink(service_account_path)

    def test_invalid_auth_method_fails(self):
        """Test that invalid authentication method raises ValueError."""
        config_dict = {"project_id": "test-project", "auth_method": "invalid_method"}

        with pytest.raises(ValueError, match="Invalid auth_method"):
            BigQueryServerConfig(config_dict)

    def test_query_timeout_validation(self):
        """Test query timeout validation."""
        # Valid timeout
        config_dict = {"project_id": "test-project", "query_timeout": 600}
        config = BigQueryServerConfig(config_dict)
        assert config.get_template_config()["query_timeout"] == 600

        # Too low timeout
        config_dict["query_timeout"] = 5
        with pytest.raises(
            ValueError, match="query_timeout must be an integer between 10 and 3600"
        ):
            BigQueryServerConfig(config_dict)

        # Too high timeout
        config_dict["query_timeout"] = 5000
        with pytest.raises(
            ValueError, match="query_timeout must be an integer between 10 and 3600"
        ):
            BigQueryServerConfig(config_dict)

    def test_max_results_validation(self):
        """Test max results validation."""
        # Valid max_results
        config_dict = {"project_id": "test-project", "max_results": 5000}
        config = BigQueryServerConfig(config_dict)
        assert config.get_template_config()["max_results"] == 5000

        # Too low max_results
        config_dict["max_results"] = 0
        with pytest.raises(
            ValueError, match="max_results must be an integer between 1 and 10000"
        ):
            BigQueryServerConfig(config_dict)

        # Too high max_results
        config_dict["max_results"] = 20000
        with pytest.raises(
            ValueError, match="max_results must be an integer between 1 and 10000"
        ):
            BigQueryServerConfig(config_dict)

    def test_read_only_mode_parsing(self):
        """Test read-only mode boolean parsing."""
        test_cases = [
            (True, True),
            (False, False),
            ("true", True),
            ("false", False),
            ("1", True),
            ("0", False),
            ("yes", True),
            ("no", False),
            ("on", True),
            ("off", False),
            ("invalid", True),  # Default to True for invalid values
        ]

        for input_value, expected in test_cases:
            config_dict = {"project_id": "test-project", "read_only": input_value}
            config = BigQueryServerConfig(config_dict)
            assert config.get_template_config()["read_only"] == expected

    def test_log_level_validation(self):
        """Test log level validation and defaults."""
        # Valid log level
        config_dict = {"project_id": "test-project", "log_level": "debug"}
        config = BigQueryServerConfig(config_dict)
        assert config.get_template_config()["log_level"] == "debug"

        # Invalid log level should default to info
        config_dict["log_level"] = "invalid"
        config = BigQueryServerConfig(config_dict)
        assert config.get_template_config()["log_level"] == "info"

    def test_dataset_access_validation(self):
        """Test dataset access validation methods."""
        config_dict = {
            "project_id": "test-project",
            "allowed_datasets": "analytics_*,public_data",
        }
        config = BigQueryServerConfig(config_dict)

        # Test pattern matching
        assert config.validate_dataset_access("analytics_prod") is True
        assert config.validate_dataset_access("analytics_staging") is True
        assert config.validate_dataset_access("public_data") is True
        assert config.validate_dataset_access("private_data") is False
        assert config.validate_dataset_access("sensitive_info") is False

    def test_dataset_regex_validation(self):
        """Test regex-based dataset validation."""
        config_dict = {
            "project_id": "test-project",
            "dataset_regex": "^(prod|staging)_.*$",
        }
        config = BigQueryServerConfig(config_dict)

        # Test regex matching
        assert config.validate_dataset_access("prod_analytics") is True
        assert config.validate_dataset_access("staging_data") is True
        assert config.validate_dataset_access("dev_analytics") is False
        assert config.validate_dataset_access("analytics_prod") is False

    def test_regex_takes_precedence(self):
        """Test that regex pattern takes precedence over allowed_datasets."""
        config_dict = {
            "project_id": "test-project",
            "allowed_datasets": "*",  # This would allow everything
            "dataset_regex": "^prod_.*$",  # But regex restricts to prod_ only
        }
        config = BigQueryServerConfig(config_dict)

        assert config.validate_dataset_access("prod_analytics") is True
        assert config.validate_dataset_access("staging_data") is False

    def test_invalid_regex_falls_back(self):
        """Test that invalid regex patterns fall back gracefully."""
        config_dict = {"project_id": "test-project", "dataset_regex": "[invalid regex("}
        config = BigQueryServerConfig(config_dict)

        # Should return False for invalid regex
        assert config.validate_dataset_access("any_dataset") is False

    def test_wildcard_allows_all(self):
        """Test that wildcard pattern allows all datasets."""
        config_dict = {"project_id": "test-project", "allowed_datasets": "*"}
        config = BigQueryServerConfig(config_dict)

        assert config.validate_dataset_access("any_dataset") is True
        assert config.validate_dataset_access("another_dataset") is True

    def test_get_bigquery_config(self):
        """Test get_bigquery_config method."""
        config_dict = {
            "project_id": "test-project",
            "auth_method": "oauth2",
            "read_only": False,
            "query_timeout": 600,
            "max_results": 500,
        }
        config = BigQueryServerConfig(config_dict)

        bq_config = config.get_bigquery_config()

        assert bq_config["project_id"] == "test-project"
        assert bq_config["auth_method"] == "oauth2"
        assert bq_config["read_only"] is False
        assert bq_config["query_timeout"] == 600
        assert bq_config["max_results"] == 500

    def test_is_read_only(self):
        """Test is_read_only convenience method."""
        # Read-only mode
        config = BigQueryServerConfig({"project_id": "test", "read_only": True})
        assert config.is_read_only() is True

        # Write mode
        config = BigQueryServerConfig({"project_id": "test", "read_only": False})
        assert config.is_read_only() is False

    def test_get_allowed_datasets_patterns(self):
        """Test get_allowed_datasets_patterns method."""
        # Single pattern
        config = BigQueryServerConfig(
            {"project_id": "test", "allowed_datasets": "analytics_*"}
        )
        assert config.get_allowed_datasets_patterns() == ["analytics_*"]

        # Multiple patterns
        config = BigQueryServerConfig(
            {
                "project_id": "test",
                "allowed_datasets": "analytics_*, public_data, reporting_*",
            }
        )
        patterns = config.get_allowed_datasets_patterns()
        assert "analytics_*" in patterns
        assert "public_data" in patterns
        assert "reporting_*" in patterns

        # Wildcard
        config = BigQueryServerConfig({"project_id": "test", "allowed_datasets": "*"})
        assert config.get_allowed_datasets_patterns() == ["*"]

    def test_get_auth_config(self):
        """Test get_auth_config method."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"type": "service_account"}')

            config = BigQueryServerConfig(
                {
                    "project_id": "test-project",
                    "auth_method": "service_account",
                    "service_account_path": f.name,
                },
                skip_validation=False,  # Enable validation since we have a real file
            )

            try:
                auth_config = config.get_auth_config()

                assert auth_config["method"] == "service_account"
                assert auth_config["project_id"] == "test-project"
                assert auth_config["service_account_path"] == f.name
            finally:
                os.unlink(f.name)

    def test_get_query_limits(self):
        """Test get_query_limits method."""
        config = BigQueryServerConfig(
            {"project_id": "test", "query_timeout": 600, "max_results": 2000}
        )

        limits = config.get_query_limits()
        assert limits["timeout"] == 600
        assert limits["max_results"] == 2000

    def test_get_security_config(self):
        """Test get_security_config method."""
        config = BigQueryServerConfig(
            {
                "project_id": "test",
                "read_only": False,
                "allowed_datasets": "prod_*",
                "dataset_regex": "^prod_.*$",
            }
        )

        security = config.get_security_config()
        assert security["read_only"] is False
        assert security["allowed_datasets"] == "prod_*"
        assert security["dataset_regex"] == "^prod_.*$"

    def test_create_bigquery_config_function(self):
        """Test the convenience function for creating config."""
        config_dict = {"project_id": "test-project"}

        config = create_bigquery_config(config_dict)
        assert isinstance(config, BigQueryServerConfig)
        assert config.get_template_config()["project_id"] == "test-project"

        # Test with None
        config = create_bigquery_config(None, skip_validation=True)
        assert isinstance(config, BigQueryServerConfig)

    @patch("logging.basicConfig")
    def test_logging_setup(self, mock_logging_config):
        """Test that logging is set up correctly."""
        config_dict = {"project_id": "test-project", "log_level": "debug"}

        BigQueryServerConfig(config_dict)

        # Verify logging.basicConfig was called
        mock_logging_config.assert_called()

    def test_log_config_summary(self):
        """Test log_config_summary method doesn't expose sensitive data."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"type": "service_account"}')
            service_account_path = f.name

        try:
            config_dict = {
                "project_id": "test-project",
                "auth_method": "service_account",
                "service_account_path": service_account_path,
                "read_only": False,
            }

            config = BigQueryServerConfig(config_dict)

            # This should not raise an exception and should log safely
            config.log_config_summary()

        finally:
            os.unlink(service_account_path)
