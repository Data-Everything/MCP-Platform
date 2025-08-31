#!/usr/bin/env python3
"""
BigQuery MCP Server Configuration Handler.

This module handles configuration loading, validation, and management for the
BigQuery MCP server template.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Import base configuration from MCP Platform
try:
    from mcp_platform.template.config.base import ServerConfig
except ImportError:
    # Fallback for Docker execution or direct script runs
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    try:
        from mcp_platform.template.config.base import ServerConfig
    except ImportError:
        # Create a minimal fallback if base config is not available
        class ServerConfig:
            def __init__(self, config_dict: dict = None):
                self.config_dict = config_dict or {}
                self.logger = logging.getLogger(__name__)

            def get_template_config(self):
                return self.config_dict

            def get_template_data(self):
                # Try to load template.json for fallback
                try:
                    template_path = Path(__file__).parent / "template.json"
                    with open(template_path, mode="r", encoding="utf-8") as f:
                        template_data = json.load(f)
                    # Merge with config overrides
                    template_data.update(self.config_dict)
                    return template_data
                except (FileNotFoundError, json.JSONDecodeError):
                    return self.config_dict


class BigQueryServerConfig(ServerConfig):
    """
    BigQuery-specific configuration handler.

    Extends the base ServerConfig to provide BigQuery-specific configuration
    validation and defaults.
    """

    def __init__(self, config_dict: dict = None, skip_validation: bool = False):
        """Initialize BigQuery server configuration."""
        super().__init__(config_dict or {})
        self.logger = logging.getLogger(__name__)

        # Load template data
        self.template_data = self._load_template()

        # Validate required configuration (skip for testing)
        if not skip_validation:
            self._validate_config()

        # Set up logging based on config
        self._setup_logging()

    def _validate_config(self):
        """Validate BigQuery-specific configuration requirements."""
        config = self.get_template_config()

        # Check required fields
        project_id = config.get("project_id")
        if not project_id:
            # Try environment variable fallback
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
            if not project_id:
                # Allow test project IDs that start with "test-"
                if not (hasattr(self, "_is_test_mode") and self._is_test_mode):
                    raise ValueError(
                        "project_id is required. Set it in config or "
                        "GOOGLE_CLOUD_PROJECT environment variable."
                    )
                # Use test default
                project_id = "test-project"
            config["project_id"] = project_id

        # Validate auth method and set default
        auth_method = config.get("auth_method", "application_default")
        config["auth_method"] = auth_method
        valid_auth_methods = ["service_account", "oauth2", "application_default"]
        if auth_method not in valid_auth_methods:
            raise ValueError(
                f"Invalid auth_method '{auth_method}'. Must be one of: {valid_auth_methods}"
            )

        # Validate service account path if using service account auth
        if auth_method == "service_account":
            service_account_path = config.get("service_account_path")
            if not service_account_path:
                # Try environment variable fallback
                service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if not service_account_path:
                    raise ValueError(
                        "service_account_path is required when auth_method is 'service_account'. "
                        "Set it in config or GOOGLE_APPLICATION_CREDENTIALS environment variable."
                    )
                config["service_account_path"] = service_account_path

            # In test mode, skip file existence check
            if not (
                hasattr(self, "_is_test_mode") and self._is_test_mode
            ) and not os.path.exists(service_account_path):
                raise ValueError(
                    f"Service account key file not found: {service_account_path}"
                )

        # Validate numeric ranges and set defaults
        query_timeout = config.get("query_timeout", 300)
        config["query_timeout"] = query_timeout
        if (
            not isinstance(query_timeout, int)
            or query_timeout < 10
            or query_timeout > 3600
        ):
            raise ValueError(
                "query_timeout must be an integer between 10 and 3600 seconds"
            )

        max_results = config.get("max_results", 1000)
        config["max_results"] = max_results
        if not isinstance(max_results, int) or max_results < 1 or max_results > 10000:
            raise ValueError("max_results must be an integer between 1 and 10000")

        # Validate read_only mode and set defaults
        read_only = config.get("read_only", True)
        if not isinstance(read_only, bool):
            # Try to parse string values
            if isinstance(read_only, str):
                lower_val = read_only.lower()
                if lower_val in ("true", "1", "yes", "on"):
                    read_only = True
                elif lower_val in ("false", "0", "no", "off"):
                    read_only = False
                else:
                    # Default to True for invalid values
                    read_only = True
            else:
                read_only = True
        config["read_only"] = read_only

        # Validate dataset filters and set defaults
        allowed_datasets = config.get("allowed_datasets", "*")
        config["allowed_datasets"] = allowed_datasets
        if not isinstance(allowed_datasets, str):
            raise ValueError("allowed_datasets must be a string")

        dataset_regex = config.get("dataset_regex")
        if dataset_regex and not isinstance(dataset_regex, str):
            raise ValueError("dataset_regex must be a string")

        # Validate log level and set defaults
        log_level = config.get("log_level", "info")
        config["log_level"] = log_level
        valid_log_levels = ["debug", "info", "warning", "error"]
        if log_level not in valid_log_levels:
            self.logger.warning(
                f"Invalid log_level '{log_level}', defaulting to 'info'"
            )
            config["log_level"] = "info"

    def _setup_logging(self):
        """Set up logging based on configuration."""
        config = self.get_template_config()
        log_level = config.get("log_level", "info").upper()

        # Map string levels to logging constants
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }

        logging_level = level_map.get(log_level, logging.INFO)

        # Configure root logger
        logging.basicConfig(
            level=logging_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        # Update our logger level
        self.logger.setLevel(logging_level)

    def get_bigquery_config(self) -> Dict[str, Any]:
        """Get BigQuery-specific configuration values."""
        config = self.get_template_config()

        return {
            "project_id": config.get("project_id"),
            "auth_method": config.get("auth_method", "application_default"),
            "service_account_path": config.get("service_account_path"),
            "read_only": config.get("read_only", True),
            "allowed_datasets": config.get("allowed_datasets", "*"),
            "dataset_regex": config.get("dataset_regex"),
            "query_timeout": config.get("query_timeout", 300),
            "max_results": config.get("max_results", 1000),
            "log_level": config.get("log_level", "info"),
        }

    def is_read_only(self) -> bool:
        """Check if server is in read-only mode."""
        return self.get_template_config().get("read_only", True)

    def get_allowed_datasets_patterns(self) -> list:
        """Get list of allowed dataset patterns."""
        allowed_datasets = self.get_template_config().get("allowed_datasets", "*")
        if allowed_datasets == "*":
            return ["*"]
        return [pattern.strip() for pattern in allowed_datasets.split(",")]

    def get_auth_config(self) -> Dict[str, Any]:
        """Get authentication configuration."""
        config = self.get_template_config()
        return {
            "method": config.get("auth_method", "application_default"),
            "project_id": config.get("project_id"),
            "service_account_path": config.get("service_account_path"),
        }

    def get_query_limits(self) -> Dict[str, int]:
        """Get query execution limits."""
        config = self.get_template_config()
        return {
            "timeout": config.get("query_timeout", 300),
            "max_results": config.get("max_results", 1000),
        }

    def get_security_config(self) -> Dict[str, Any]:
        """Get security-related configuration."""
        config = self.get_template_config()
        return {
            "read_only": config.get("read_only", True),
            "allowed_datasets": config.get("allowed_datasets", "*"),
            "dataset_regex": config.get("dataset_regex"),
        }

    def validate_dataset_access(self, dataset_id: str) -> bool:
        """
        Validate if access to a dataset is allowed based on configuration.

        Args:
            dataset_id: The BigQuery dataset ID to check

        Returns:
            bool: True if access is allowed, False otherwise
        """
        import fnmatch
        import re

        security_config = self.get_security_config()

        # Check regex filter first (takes precedence)
        dataset_regex = security_config.get("dataset_regex")
        if dataset_regex:
            try:
                return bool(re.match(dataset_regex, dataset_id))
            except re.error as e:
                self.logger.warning(f"Invalid regex pattern '{dataset_regex}': {e}")
                return False

        # Check allowed_datasets patterns
        allowed_datasets = security_config.get("allowed_datasets", "*")
        if allowed_datasets == "*":
            return True

        patterns = [pattern.strip() for pattern in allowed_datasets.split(",")]
        return any(fnmatch.fnmatch(dataset_id, pattern) for pattern in patterns)

    def log_config_summary(self):
        """Log a summary of the current configuration (without sensitive data)."""
        config = self.get_bigquery_config()

        # Remove sensitive information for logging
        safe_config = {
            "project_id": config["project_id"],
            "auth_method": config["auth_method"],
            "read_only": config["read_only"],
            "allowed_datasets": config["allowed_datasets"],
            "query_timeout": config["query_timeout"],
            "max_results": config["max_results"],
            "log_level": config["log_level"],
        }

        if config.get("dataset_regex"):
            safe_config["dataset_regex"] = config["dataset_regex"]

        if config.get("service_account_path"):
            # Only log the filename, not the full path
            path = config["service_account_path"]
            safe_config["service_account_file"] = os.path.basename(path)

        self.logger.info("BigQuery MCP Server Configuration:")
        for key, value in safe_config.items():
            self.logger.info(f"  {key}: {value}")

    def _load_template(self, template_path: str = None) -> Dict[str, Any]:
        """
        Load template data from template.json file.

        Args:
            template_path: Path to the template JSON file

        Returns:
            Parsed template data as dictionary
        """
        if not template_path:
            template_path = Path(__file__).parent / "template.json"

        try:
            with open(template_path, mode="r", encoding="utf-8") as template_file:
                return json.load(template_file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.warning(
                f"Failed to load template data from {template_path}: {e}"
            )
            # Return minimal template data as fallback
            return {
                "name": "BigQuery MCP Server",
                "version": "1.0.0",
                "description": "BigQuery MCP Server",
                "transport": {"port": 7072},
            }

    def get_template_data(self) -> Dict[str, Any]:
        """
        Get the full template data, potentially modified by configuration overrides.

        Returns:
            Template data dictionary with any configuration overrides applied
        """
        # Start with base template data
        template_data = self.template_data.copy()

        # Apply any template-level overrides from config_dict
        template_config_keys = set(self.get_template_config().keys())
        for key, value in self.config_dict.items():
            if key.lower() not in template_config_keys:
                # Direct template-level override (not in config_schema)
                template_key = key.lower()
                template_data[template_key] = value
                self.logger.debug(
                    "Applied template override: %s = %s", template_key, value
                )

        return template_data

        # Get current configuration for logging
        current_config = self.get_template_config()

        # Log security warnings
        if not current_config["read_only"]:
            self.logger.warning(
                "Server is running in WRITE mode - this allows data modifications!"
            )

        if current_config["allowed_datasets"] == "*":
            self.logger.warning(
                "All datasets are accessible - consider restricting access for production use"
            )


# Convenience function for creating configuration
def create_bigquery_config(
    config_dict: Optional[Dict[str, Any]] = None,
    skip_validation: bool = False,
) -> BigQueryServerConfig:
    """
    Create a BigQuery server configuration instance.

    Args:
        config_dict: Optional configuration dictionary
        skip_validation: Skip validation for testing

    Returns:
        BigQueryServerConfig: Configured instance
    """
    return BigQueryServerConfig(config_dict, skip_validation=skip_validation)
