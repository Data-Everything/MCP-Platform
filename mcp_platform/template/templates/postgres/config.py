#!/usr/bin/env python3
"""
PostgreSQL MCP Server Configuration Handler.

This module handles configuration loading, validation, and management for the
PostgreSQL MCP server template.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote_plus


class PostgresServerConfig:
    """
    PostgreSQL-specific configuration handler.

    Provides PostgreSQL-specific configuration validation and defaults.
    """

    def __init__(self, config_dict: dict = None, skip_validation: bool = False):
        """Initialize PostgreSQL server configuration."""
        self.config_dict = config_dict or {}
        self.logger = logging.getLogger(__name__)

        # Load template data
        self.template_data = self._load_template()

        # Validate required configuration (skip for testing)
        if not skip_validation:
            self._validate_config()

        # Set up logging based on config
        self._setup_logging()

    def _get_config(
        self, key: str, env_var: str, default: Any, cast_to: Optional[type] = str
    ) -> Any:
        """
        Get configuration value with precedence handling.

        Args:
            key: Configuration key in config_dict
            env_var: Environment variable name
            default: Default value if not found
            cast_to: Cast environment variable value to this type

        Returns:
            Configuration value
        """
        # Check config_dict first
        if key in self.config_dict:
            self.logger.debug(
                "Using config_dict value for '%s': %s", key, self.config_dict[key]
            )
            return self.config_dict[key]

        # Check environment variable
        env_value = os.getenv(env_var)
        if env_value is not None:
            self.logger.debug("Using environment variable '%s': %s", env_var, env_value)
            try:
                if cast_to == bool:
                    # Handle boolean environment variables
                    return env_value.lower() in ("true", "1", "yes", "on")
                return cast_to(env_value)
            except (ValueError, TypeError) as e:
                self.logger.error(
                    "Error casting environment variable '%s': %s", env_var, e
                )
                return env_value

        # Return default
        self.logger.debug("Using default value for '%s': %s", key, default)
        return default

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
            else:
                cast_to = str

            properties_dict[key] = self._get_config(
                key, env_var, default_value, cast_to
            )

        return properties_dict

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

    def _validate_config(self):
        """Validate PostgreSQL-specific configuration requirements."""
        config = self.get_template_config()

        # Check required fields
        pg_host = config.get("pg_host")
        if not pg_host:
            raise ValueError(
                "pg_host is required. Set it in config or PG_HOST environment variable."
            )

        pg_user = config.get("pg_user")
        if not pg_user:
            raise ValueError(
                "pg_user is required. Set it in config or PG_USER environment variable."
            )

        # Validate port range
        pg_port = config.get("pg_port", 5432)
        try:
            pg_port = int(pg_port)
        except (ValueError, TypeError):
            raise ValueError("pg_port must be a valid integer")
        if not (1 <= pg_port <= 65535):
            raise ValueError("pg_port must be between 1 and 65535")

        # Validate SSL mode
        ssl_mode = config.get("ssl_mode", "prefer")
        valid_ssl_modes = [
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ]
        if ssl_mode not in valid_ssl_modes:
            raise ValueError(f"ssl_mode must be one of: {valid_ssl_modes}")

        # Validate auth method
        auth_method = config.get("auth_method", "password")
        valid_auth_methods = [
            "password",
            "md5",
            "scram-sha-256",
            "gss",
            "sspi",
            "ident",
            "peer",
            "ldap",
            "radius",
            "cert",
            "pam",
        ]
        if auth_method not in valid_auth_methods:
            raise ValueError(f"auth_method must be one of: {valid_auth_methods}")

        # Validate certificate files: if ssl_cert provided, require ssl_key.
        # Additionally enforce ssl_ca when ssl_mode requires it.
        ssl_cert = config.get("ssl_cert")
        ssl_key = config.get("ssl_key")
        ssl_ca = config.get("ssl_ca")

        if ssl_cert and not ssl_key:
            # Tests expect a message containing 'ssl_key is required'
            raise ValueError("ssl_key is required")

        # Enforce CA certificate when ssl_mode requests verification
        if ssl_mode in ["verify-ca", "verify-full"]:
            if not ssl_ca:
                raise ValueError(
                    "ssl_ca is required when ssl_mode is 'verify-ca' or 'verify-full'"
                )

        # Validate SSH tunnel configuration
        ssh_tunnel = config.get("ssh_tunnel", False)
        if ssh_tunnel:
            ssh_host = config.get("ssh_host")
            ssh_user = config.get("ssh_user")

            if not ssh_host:
                raise ValueError("ssh_host is required when ssh_tunnel is enabled")
            if not ssh_user:
                raise ValueError("ssh_user is required when ssh_tunnel is enabled")

            # Validate SSH auth method
            ssh_auth_method = config.get("ssh_auth_method", "password")
            valid_ssh_auth = ["password", "key", "agent"]
            if ssh_auth_method not in valid_ssh_auth:
                raise ValueError(f"ssh_auth_method must be one of: {valid_ssh_auth}")

            # Validate SSH key if key auth is used
            if ssh_auth_method == "key":
                ssh_key_file = config.get("ssh_key_file")
                if not ssh_key_file:
                    raise ValueError(
                        "ssh_key_file is required when ssh_auth_method is 'key'"
                    )

        # Validate timeout values
        query_timeout = config.get("query_timeout", 300)
        try:
            query_timeout = int(query_timeout)
        except (ValueError, TypeError):
            raise ValueError("query_timeout must be a positive integer")
        if query_timeout <= 0:
            raise ValueError("query_timeout must be positive")

        connection_timeout = config.get("connection_timeout", 10)
        try:
            connection_timeout = int(connection_timeout)
        except (ValueError, TypeError):
            raise ValueError("connection_timeout must be a positive integer")
        if connection_timeout <= 0:
            raise ValueError("connection_timeout must be positive")

        # Validate max results
        max_results = config.get("max_results", 1000)
        try:
            max_results = int(max_results)
        except (ValueError, TypeError):
            raise ValueError("max_results must be a positive integer")
        if max_results <= 0:
            raise ValueError("max_results must be positive")

        # Validate schemas access control
        allowed_schemas = config.get("allowed_schemas", "*")
        if allowed_schemas and allowed_schemas != "*":
            try:
                # Test if it's a valid regex
                re.compile(allowed_schemas)
            except re.error as e:
                self.logger.warning(
                    "allowed_schemas appears to be invalid regex: %s", e
                )

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
            "CRITICAL": logging.CRITICAL,
        }

        level = level_map.get(log_level, logging.INFO)
        logging.getLogger().setLevel(level)

        # Set up formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Update existing handlers
        for handler in logging.getLogger().handlers:
            handler.setLevel(level)
            handler.setFormatter(formatter)

    def _load_template(self) -> Dict[str, Any]:
        """Load template configuration from template.json."""
        try:
            template_path = Path(__file__).parent / "template.json"
            with open(template_path, mode="r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.warning("Could not load template.json: %s", e)
            return {}

    def get_connection_string(self, database_override: str = None) -> str:
        """
        Build PostgreSQL connection string from configuration.

        Returns:
            SQLAlchemy-compatible connection string
        """
        config = self.get_template_config()

        # Basic connection parameters
        host = config.get("pg_host")
        port = config.get("pg_port", 5432)
        database = database_override or config.get("pg_database", "postgres")
        user = config.get("pg_user")
        password = config.get("pg_password", "")

        # URL encode password if it contains special characters
        if password:
            password = quote_plus(password)

        # Build base connection string
        if password:
            conn_str = (
                f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
            )
        else:
            conn_str = f"postgresql+psycopg://{user}@{host}:{port}/{database}"

        # Add SSL parameters
        ssl_params = []
        ssl_mode = config.get("ssl_mode", "prefer")
        if ssl_mode != "disable":
            ssl_params.append(f"sslmode={ssl_mode}")

            ssl_cert = config.get("ssl_cert")
            if ssl_cert:
                ssl_params.append(f"sslcert={ssl_cert}")

            ssl_key = config.get("ssl_key")
            if ssl_key:
                ssl_params.append(f"sslkey={ssl_key}")

            ssl_ca = config.get("ssl_ca")
            if ssl_ca:
                ssl_params.append(f"sslrootcert={ssl_ca}")

        # Add connection timeout
        connection_timeout = config.get("connection_timeout", 10)
        ssl_params.append(f"connect_timeout={connection_timeout}")

        # Add application name for connection tracking
        ssl_params.append("application_name=mcp-postgres-server")

        if ssl_params:
            conn_str += "?" + "&".join(ssl_params)

        return conn_str

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-like getter to ease test compatibility.

        Tests expect the config object to support `.get(key, default)`, so
        provide a thin helper that proxies to the template config values.
        """
        return self.get_template_config().get(key, default)

    def get_ssh_config(self) -> Optional[Dict[str, Any]]:
        """
        Get SSH tunnel configuration if enabled.

        Returns:
            SSH configuration dictionary or None if disabled
        """
        config = self.get_template_config()

        if not config.get("ssh_tunnel", False):
            return None

        ssh_config = {
            "ssh_host": config.get("ssh_host"),
            "ssh_port": config.get("ssh_port", 22),
            "ssh_user": config.get("ssh_user"),
            "ssh_auth_method": config.get("ssh_auth_method", "password"),
            "local_port": config.get("ssh_local_port", 0),  # 0 = auto-assign
            "remote_host": config.get("pg_host", "localhost"),
            "remote_port": config.get("pg_port", 5432),
        }

        # Add authentication details
        if ssh_config["ssh_auth_method"] == "password":
            ssh_config["ssh_password"] = config.get("ssh_password")
        elif ssh_config["ssh_auth_method"] == "key":
            ssh_config["ssh_key_file"] = config.get("ssh_key_file")
            ssh_config["ssh_key_passphrase"] = config.get("ssh_key_passphrase")

        return ssh_config

    def is_read_only(self) -> bool:
        """Check if server is configured in read-only mode."""
        config = self.get_template_config()
        return config.get("read_only", True)

    def get_allowed_schemas(self) -> str:
        """Get allowed schemas pattern."""
        config = self.get_template_config()
        return config.get("allowed_schemas", "*")

    def get_query_timeout(self) -> int:
        """Get query timeout in seconds."""
        config = self.get_template_config()
        return config.get("query_timeout", 300)

    def get_max_results(self) -> int:
        """Get maximum number of results to return."""
        config = self.get_template_config()
        return config.get("max_results", 1000)
