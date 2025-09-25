#!/usr/bin/env python3
"""
Snowflake MCP Server Configuration Handler.

This module handles configuration loading, validation, and management for the
Snowflake MCP server template.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from cryptography.hazmat.primitives import serialization

    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False


class SnowflakeServerConfig:
    """
    Snowflake-specific configuration handler.

    Provides Snowflake-specific configuration validation and defaults.
    """

    def __init__(self, config_dict: dict = None, skip_validation: bool = False):
        """Initialize Snowflake server configuration."""
        self.logger = logging.getLogger(__name__)
        self._skip_validation = skip_validation
        self.config_dict = config_dict or {}

        # Load template data
        self.template_data = self._load_template()

        # Setup logging
        self._setup_logging()

        # Validate configuration
        if not skip_validation:
            self._validate_config()

    def _get_config(
        self, key: str, env_var: str, default: Any, cast_to: Optional[type] = str
    ) -> Any:
        """
        Get configuration value from dict, environment, or default.

        Args:
            key: Configuration key
            env_var: Environment variable name
            default: Default value
            cast_to: Type to cast the value to

        Returns:
            Configuration value
        """
        # First check config dict
        if key in self.config_dict and self.config_dict[key] is not None:
            value = self.config_dict[key]
        # Then check environment
        elif env_var and env_var in os.environ:
            value = os.environ[env_var]
        # Finally use default
        else:
            value = default

        # Cast to appropriate type
        if value is not None and cast_to and cast_to != str:
            try:
                if cast_to == bool:
                    # Handle boolean conversion properly
                    if isinstance(value, str):
                        return value.lower() in ("true", "1", "yes", "on")
                    return bool(value)
                elif cast_to == dict and isinstance(value, str):
                    # Handle JSON string conversion for session parameters
                    return json.loads(value)
                else:
                    return cast_to(value)
            except (ValueError, TypeError, json.JSONDecodeError) as e:
                self.logger.warning(
                    "Failed to cast %s to %s: %s. Using default.",
                    key,
                    cast_to.__name__,
                    e,
                )
                return default

        return value

    def get_template_config(self) -> Dict[str, Any]:
        """
        Get configuration properties from the template.

        Returns:
            Dictionary containing template configuration properties
        """
        properties_dict = {}
        properties = self.template_data.get("config_schema", {}).get("properties", {})

        for key, value in properties.items():
            # Load default values from environment or template
            env_var = value.get("env_mapping", key.upper())
            default_value = value.get("default", None)
            data_type = value.get("type", "string")

            if data_type == "integer":
                cast_to = int
            elif data_type == "number":
                cast_to = float
            elif data_type == "boolean":
                cast_to = bool
            elif data_type == "object":
                cast_to = dict
            else:
                cast_to = str

            properties_dict[key] = self._get_config(
                key, env_var, default_value, cast_to
            )

        return properties_dict

    def get_template_data(self) -> Dict[str, Any]:
        """
        Get template metadata.

        Returns:
            Dictionary containing template metadata
        """
        return self.template_data

    def _validate_config(self):
        """Validate Snowflake-specific configuration requirements."""
        config = self.get_template_config()

        # Validate required fields
        if not config.get("account"):
            raise ValueError("Snowflake account identifier is required")

        if not config.get("user"):
            raise ValueError("Snowflake user is required")

        # Validate account format
        account = config.get("account", "")
        if not re.match(r"^[a-zA-Z0-9._-]+$", account):
            raise ValueError("Invalid Snowflake account format")

        # Validate authenticator
        authenticator = config.get("authenticator", "snowflake")
        valid_authenticators = [
            "snowflake",
            "oauth",
            "externalbrowser",
            "okta_endpoint",
            "jwt",
        ]
        if authenticator not in valid_authenticators:
            raise ValueError(f"authenticator must be one of: {valid_authenticators}")

        # Validate authentication method requirements
        if authenticator == "snowflake" and not config.get("password"):
            raise ValueError("password is required when authenticator is 'snowflake'")

        if authenticator == "jwt":
            if not config.get("private_key") and not config.get("private_key_file"):
                raise ValueError(
                    "Either private_key or private_key_file is required "
                    "when authenticator is 'jwt'"
                )
            # Validate private key if provided
            if config.get("private_key"):
                self._validate_private_key_content(config["private_key"])
            elif config.get("private_key_file"):
                self._validate_private_key_file(config["private_key_file"])

        if authenticator == "oauth" and not config.get("oauth_token"):
            raise ValueError("oauth_token is required when authenticator is 'oauth'")

        if authenticator == "okta_endpoint" and not config.get("okta_endpoint"):
            raise ValueError(
                "okta_endpoint is required when authenticator is 'okta_endpoint'"
            )

        # Validate timeout ranges
        connection_timeout = config.get("connection_timeout", 60)
        if (
            not isinstance(connection_timeout, int)
            or connection_timeout < 10
            or connection_timeout > 900
        ):
            raise ValueError("connection_timeout must be between 10 and 900 seconds")

        query_timeout = config.get("query_timeout", 3600)
        if (
            not isinstance(query_timeout, int)
            or query_timeout < 60
            or query_timeout > 21600
        ):
            raise ValueError("query_timeout must be between 60 and 21600 seconds")

        # Validate max_results
        max_results = config.get("max_results", 10000)
        if not isinstance(max_results, int) or max_results < 1 or max_results > 100000:
            raise ValueError("max_results must be between 1 and 100000")

        # Validate URI format for okta_endpoint
        okta_endpoint = config.get("okta_endpoint")
        if okta_endpoint and not re.match(r"^https?://", okta_endpoint):
            raise ValueError("okta_endpoint must be a valid HTTP/HTTPS URL")

    def _validate_private_key_content(self, private_key_content: str):
        """Validate private key content format."""
        if not CRYPTOGRAPHY_AVAILABLE:
            self.logger.warning(
                "cryptography package not available, skipping private key validation"
            )
            return

        try:
            # Try to load the private key to validate format
            serialization.load_pem_private_key(
                private_key_content.encode(), password=None
            )
        except Exception as e:
            raise ValueError(f"Invalid private key format: {e}")

    def _validate_private_key_file(self, private_key_file: str):
        """Validate private key file exists and has valid format."""
        if not os.path.exists(private_key_file):
            raise ValueError(f"Private key file not found: {private_key_file}")

        if not CRYPTOGRAPHY_AVAILABLE:
            self.logger.warning(
                "cryptography package not available, skipping private key validation"
            )
            return

        try:
            with open(private_key_file, "rb") as f:
                private_key_data = f.read()

            # Try to load the private key to validate format
            serialization.load_pem_private_key(private_key_data, password=None)
        except Exception as e:
            raise ValueError(f"Invalid private key file format: {e}")

    def _setup_logging(self):
        """Set up logging based on configuration."""
        log_level = self._get_config("log_level", "MCP_LOG_LEVEL", "info", str).upper()

        # Convert string to logging level
        numeric_level = getattr(logging, log_level, logging.INFO)

        # Configure logger
        logger = logging.getLogger()
        logger.setLevel(numeric_level)

        # Add console handler if not already present
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    def _load_template(self) -> Dict[str, Any]:
        """Load template configuration from template.json."""
        try:
            template_path = Path(__file__).parent / "template.json"
            with open(template_path, mode="r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.warning("Could not load template.json: %s", e)
            return {}

    def get_connection_params(self) -> Dict[str, Any]:
        """
        Get Snowflake connection parameters.

        Returns:
            Dictionary containing connection parameters for Snowflake connector
        """
        config = self.get_template_config()

        # Base connection parameters
        params = {
            "account": config["account"],
            "user": config["user"],
        }

        # Add optional connection parameters
        for param in ["database", "schema", "warehouse", "role", "region"]:
            if config.get(param):
                params[param] = config[param]

        # Add authentication parameters based on method
        authenticator = config.get("authenticator", "snowflake")
        params["authenticator"] = authenticator

        if authenticator == "snowflake":
            params["password"] = config["password"]
        elif authenticator == "jwt":
            if config.get("private_key"):
                params["private_key"] = config["private_key"]
            elif config.get("private_key_file"):
                # Load private key from file
                with open(config["private_key_file"], "r") as f:
                    params["private_key"] = f.read()
            if config.get("private_key_passphrase"):
                params["private_key_passphrase"] = config["private_key_passphrase"]
        elif authenticator == "oauth":
            params["token"] = config["oauth_token"]
        elif authenticator == "okta_endpoint":
            params["okta_endpoint_url"] = config["okta_endpoint"]

        # Add connection settings
        params["login_timeout"] = config.get("connection_timeout", 60)
        params["network_timeout"] = config.get("query_timeout", 3600)

        # Add SSL/security settings
        if config.get("insecure_mode"):
            params["insecure_mode"] = True

        if config.get("ocsp_response_cache_filename"):
            params["ocsp_response_cache_filename"] = config[
                "ocsp_response_cache_filename"
            ]

        # Add session parameters
        if config.get("session_parameters"):
            params["session_parameters"] = config["session_parameters"]

        # Add client session keep alive
        if config.get("client_session_keep_alive"):
            params["client_session_keep_alive"] = True

        return params

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        config = self.get_template_config()
        return config.get(key, default)
