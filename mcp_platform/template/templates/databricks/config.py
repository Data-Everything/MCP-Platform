#!/usr/bin/env python3
"""
Configuration module for the Databricks MCP Server.

This module provides configuration management for the Databricks template,
including environment variable mapping, validation, and support for
double underscore notation from CLI arguments.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


class DatabricksServerConfig:
    """
    Configuration class for the Databricks MCP Server.

    Handles configuration loading from environment variables,
    provides defaults, validates settings, and supports double underscore
    notation for nested configuration override.
    """

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        """
        Initialize Databricks server configuration.

        Args:
            config_dict: Optional configuration dictionary to override defaults
        """

        self.config_dict = config_dict or {}
        self.log_level = None
        self.logger = self._setup_logger()

        # Load template data first so we can use it for type coercion
        self.template_data = self._load_template()
        self.logger.debug("Template data loaded")

        # Load override environment variables from deployer
        self._load_override_env_vars()

        # Process any double underscore configurations passed from CLI
        self._process_nested_config()

        # Validate configuration
        self._validate_config()
        self.logger.info("Databricks server configuration loaded")

    def _load_override_env_vars(self) -> None:
        """
        Load override environment variables set by the deployer.

        Environment variables with OVERRIDE_ prefix are processed as template overrides.
        These allow the deployer to pass override values without parsing template.json.
        """
        override_dict = {}

        # Scan environment for OVERRIDE_ variables
        for env_var, env_value in os.environ.items():
            override_key = None

            if env_var.startswith("OVERRIDE_"):
                # Remove OVERRIDE_ prefix to get the original key
                override_key = env_var[9:]
            elif env_var.startswith("MCP_OVERRIDE_"):
                # Handle case where deployment pipeline adds MCP_ prefix
                override_key = env_var[13:]

            if override_key:
                override_dict[override_key] = env_value
                self.logger.debug(
                    "Found override environment variable: %s = %s",
                    override_key,
                    env_value,
                )

        # Add override values to config_dict so they get processed by _process_nested_config
        if override_dict:
            self.config_dict.update(override_dict)
            self.logger.info(
                "Loaded %d override environment variables", len(override_dict)
            )

    def _validate_config(self) -> None:
        """Validate configuration values."""
        config_schema = self.template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})
        required = config_schema.get("required", [])

        # Check required fields only if we have any config (not just loading template)
        if self.config_dict:
            for field in required:
                if field not in self.config_dict and not os.getenv(f"DATABRICKS_{field.upper()}"):
                    self.logger.warning("Required configuration field '%s' is missing", field)

        # Validate field types and values
        for key, value in self.config_dict.items():
            if key in properties:
                schema = properties[key]
                self._validate_field(key, value, schema)

        # Validate Databricks-specific requirements
        self._validate_databricks_config()

    def _validate_field(self, key: str, value: Any, schema: Dict[str, Any]) -> None:
        """Validate a single configuration field."""
        field_type = schema.get("type", "string")
        
        if field_type == "boolean":
            if not isinstance(value, bool):
                try:
                    self.config_dict[key] = self._convert_to_boolean(value)
                except ValueError:
                    self.logger.warning("Invalid boolean value for '%s': %s", key, value)
        elif field_type == "integer":
            if not isinstance(value, int):
                try:
                    self.config_dict[key] = int(value)
                except ValueError:
                    self.logger.warning("Invalid integer value for '%s': %s", key, value)
        elif field_type == "string":
            if value is not None:
                self.config_dict[key] = str(value)

        # Validate enum values
        if "enum" in schema and value not in schema["enum"]:
            self.logger.warning(
                "Invalid value for '%s': %s. Valid values: %s", 
                key, value, schema["enum"]
            )

    def _validate_databricks_config(self) -> None:
        """Validate Databricks-specific configuration requirements."""
        # Validate workspace_host format
        workspace_host = self.config_dict.get("workspace_host")
        if workspace_host and not workspace_host.startswith(("http://", "https://")):
            self.logger.warning(
                "Workspace host should include protocol (http:// or https://): %s",
                workspace_host
            )

        # Validate authentication method and required credentials
        auth_method = self.config_dict.get("auth_method", "pat")
        
        if auth_method == "pat":
            if not self.config_dict.get("access_token"):
                self.logger.warning(
                    "PAT authentication requires access_token to be set"
                )
        elif auth_method == "oauth":
            if not self.config_dict.get("oauth_token"):
                self.logger.warning(
                    "OAuth authentication requires oauth_token to be set"
                )
        elif auth_method == "username_password":
            if not self.config_dict.get("username") or not self.config_dict.get("password"):
                self.logger.warning(
                    "Username/password authentication requires both username and password"
                )

        # Validate read-only mode warning
        if not self.config_dict.get("read_only", True):
            self.logger.warning(
                "⚠️  READ-ONLY MODE DISABLED: This allows write operations which can "
                "modify or delete data in your Databricks workspace. Use with extreme caution!"
            )

        # Validate allowed databases/schemas patterns
        self._validate_access_patterns()

    def _validate_access_patterns(self) -> None:
        """Validate database and schema access patterns."""
        allowed_databases = self.config_dict.get("allowed_databases", "*")
        allowed_schemas = self.config_dict.get("allowed_schemas", "*")

        for pattern_name, pattern in [
            ("allowed_databases", allowed_databases),
            ("allowed_schemas", allowed_schemas)
        ]:
            if pattern and pattern != "*":
                # Try to validate regex patterns
                patterns = [p.strip() for p in pattern.split(",")]
                for p in patterns:
                    try:
                        re.compile(p)
                    except re.error as e:
                        self.logger.warning(
                            "Invalid regex pattern in %s: '%s' - %s",
                            pattern_name, p, str(e)
                        )

    def _convert_to_boolean(self, value: Any) -> bool:
        """Convert value to boolean."""
        if isinstance(value, str):
            lower_value = value.lower()
            if lower_value in ("true", "1", "yes", "on"):
                return True
            elif lower_value in ("false", "0", "no", "off"):
                return False
            else:
                raise ValueError(f"Invalid boolean value: {value}")
        return bool(value)

    def _process_nested_config(self) -> None:
        """
        Process double underscore notation in configuration.

        Recursively processes nested keys using double underscore notation and
        attempts to cast values using the original data type from template.json.
        """
        processed_config = {}

        for key, value in self.config_dict.items():
            if "__" in key:
                processed_key, processed_value = self._process_double_underscore_key(
                    key, value
                )
                if processed_key:
                    # Attempt type coercion based on template.json schema
                    coerced_value = self._coerce_value_type(
                        processed_key, processed_value
                    )
                    processed_config[processed_key] = coerced_value
            else:
                # Keep non-nested configurations as-is, but still attempt type coercion
                coerced_value = self._coerce_value_type(key, value)
                processed_config[key] = coerced_value

        # Update config_dict with processed configurations
        self.config_dict.update(processed_config)

    def _process_double_underscore_key(
        self, key: str, value: Any
    ) -> tuple[Optional[str], Any]:
        """
        Process a single double underscore key.

        Returns:
            Tuple of (processed_key, value) or (None, value) if no processing needed
        """
        parts = key.split("__")

        if len(parts) == 2:
            return self._handle_two_part_key(parts, value)
        elif len(parts) > 2:
            return self._handle_multi_part_key(parts, value)

        return key, value

    def _handle_two_part_key(self, parts: list[str], value: Any) -> tuple[str, Any]:
        """Handle two-part keys like databricks__workspace_host."""
        prefix, config_key = parts

        # Check if the final key is a known config property
        if self._is_config_property(config_key):
            self.logger.debug("Processed config property: %s = %s", config_key, value)
            return config_key, value

        # Handle template-level overrides (legacy for backward compatibility)
        elif prefix.lower() in ["databricks", "template"]:
            self.logger.debug("Processed template override: %s = %s", config_key, value)
            return config_key, value

        # Handle nested configuration for custom properties
        else:
            nested_key = f"{prefix}_{config_key}"
            self.logger.debug("Processed nested config: %s = %s", nested_key, value)
            return nested_key, value

    def _handle_multi_part_key(self, parts: list[str], value: Any) -> tuple[str, Any]:
        """Handle multi-part keys like category__subcategory__property."""
        # For config properties, check if the final part is a known config property
        final_key = parts[-1]
        if self._is_config_property(final_key):
            self.logger.debug("Processed config property: %s = %s", final_key, value)
            return final_key, value
        else:
            # For non-config properties, create nested structure
            nested_key = "_".join(parts)
            self.logger.debug("Processed nested structure: %s = %s", nested_key, value)
            return nested_key, value

    def _is_config_property(self, key: str) -> bool:
        """Check if a key is a known configuration property."""
        if not hasattr(self, "template_data") or not self.template_data:
            return False

        config_schema = self.template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})

        # Check if it's a direct property
        if key in properties:
            return True

        # Check if it matches any env_mapping
        for prop_data in properties.values():
            if prop_data.get("env_mapping") == key:
                return True

        return False

    def _coerce_value_type(self, key: str, value: Any) -> Any:
        """
        Attempt to coerce value to the appropriate type based on template.json schema.

        Args:
            key: Configuration key
            value: String value to coerce

        Returns:
            Coerced value or original value if coercion fails
        """
        if not hasattr(self, "template_data") or not self.template_data:
            return value

        prop_config = self._find_property_config(key)
        if not prop_config:
            return value

        try:
            prop_type = prop_config.get("type", "string")
            return self._convert_value_by_type(value, prop_type, prop_config)

        except (ValueError, json.JSONDecodeError) as e:
            self.logger.warning(
                "Failed to coerce value '%s' for key '%s' to type '%s': %s. Using original value.",
                value,
                key,
                prop_config.get("type", "unknown"),
                str(e),
            )
            return value

    def _find_property_config(self, key: str) -> Optional[Dict[str, Any]]:
        """Find property configuration for a given key."""
        config_schema = self.template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})

        # Direct key lookup
        prop_config = properties.get(key)
        if prop_config:
            return prop_config

        # Try to find by env_mapping as fallback
        for prop_data in properties.values():
            if prop_data.get("env_mapping") == key:
                return prop_data

        return None

    def _convert_value_by_type(
        self, value: Any, prop_type: str, prop_config: Dict[str, Any]
    ) -> Any:
        """Convert value based on property type."""
        if prop_type == "boolean":
            return self._convert_to_boolean(value)
        elif prop_type == "integer":
            return int(value)
        elif prop_type == "number":
            return float(value)
        elif prop_type == "array":
            return self._convert_to_array(value, prop_config)
        elif prop_type == "object":
            return self._convert_to_object(value)
        else:  # string or unknown type
            return str(value)

    def _convert_to_array(self, value: Any, prop_config: Dict[str, Any]) -> List[Any]:
        """Convert value to array."""
        if isinstance(value, str):
            # Handle JSON array strings
            if value.startswith("["):
                if not value.endswith("]"):
                    raise ValueError(f"Malformed JSON array: {value}")
                try:
                    return json.loads(value)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON array: {value}") from e
            else:
                # Handle comma-separated values
                separator = prop_config.get("env_separator", ",")
                return [item.strip() for item in value.split(separator)]
        elif isinstance(value, list):
            return value
        else:
            return [value]

    def _convert_to_object(self, value: Any) -> Any:
        """Convert value to object."""
        if isinstance(value, str) and value.startswith("{"):
            return json.loads(value)
        return value

    def _setup_logger(self) -> logging.Logger:
        """Setup logger for configuration."""
        logger = logging.getLogger(__name__)

        # Set initial log level from environment or default
        initial_log_level = os.getenv("DATABRICKS_LOG_LEVEL", "info").upper()
        logger.setLevel(getattr(logging, initial_log_level, logging.INFO))
        self.log_level = initial_log_level.lower()

        return logger

    def _get_config(self, key: str, env_var: str, default: Any) -> Any:
        """
        Get configuration value with precedence handling.

        Args:
            key: Configuration key in config_dict
            env_var: Environment variable name
            default: Default value if not found

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
            return env_value

        # Return default
        self.logger.debug("Using default value for '%s': %s", key, default)
        return default

    def _load_template(self, template_path: str = None) -> Dict[str, Any]:
        """
        Load template data from a JSON file.

        Args:
            template_path: Path to the template JSON file

        Returns:
            Parsed template data as dictionary
        """

        if not template_path:
            template_path = Path(__file__).parent / "template.json"

        with open(template_path, mode="r", encoding="utf-8") as template_file:
            return json.load(template_file)

    def get_template_config(self, template_path: str = None) -> Dict[str, Any]:
        """
        Get configuration properties from the template.

        Args:
            template_path: Path to the template JSON file

        Returns:
            Dictionary containing template configuration properties
        """

        if template_path:
            template_data = self._load_template(template_path)
        else:
            template_data = self._load_template()

        properties_dict = {}
        properties = template_data.get("config_schema", {}).get("properties", {})
        for key, value in properties.items():
            # Load default values from environment or template
            env_var = value.get("env_mapping", key.upper())
            default_value = value.get("default", None)
            properties_dict[key] = self._get_config(key, env_var, default_value)

        return properties_dict

    def get_template_data(self) -> Dict[str, Any]:
        """
        Get the full template data, potentially modified by double underscore notation.

        This allows double underscore CLI arguments to override ANY part of the
        template structure, not just config_schema values.

        Returns:
            Template data dictionary with any double underscore overrides applied
        """
        # Start with base template data
        template_data = self.template_data.copy()

        # Apply any template-level overrides from double underscore notation
        template_config_keys = set(self.get_template_config().keys())
        for key, value in self.config_dict.items():
            if "__" in key:
                # Apply nested override to template data
                self._apply_nested_override(template_data, key, value)
            elif key.lower() not in template_config_keys:
                # Direct template-level override (not in config_schema)
                # Convert key to lowercase to match template.json keys
                template_key = key.lower()
                template_data[template_key] = value
                self.logger.debug(
                    "Applied template override: %s = %s", template_key, value
                )

        return template_data

    def _apply_nested_override(
        self, data: Dict[str, Any], key: str, value: Any
    ) -> None:
        """
        Apply a nested override using double underscore notation.

        Args:
            data: Dictionary to modify
            key: Double underscore key
            value: Value to set
        """
        parts = key.split("__")
        current = data

        # Navigate to the nested location, creating structure as needed
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]

        # Set the final value
        final_key = parts[-1]
        current[final_key] = value
        self.logger.debug("Applied nested override: %s = %s", key, value)

    def is_sensitive_field(self, field_name: str) -> bool:
        """Check if a field contains sensitive information."""
        config_schema = self.template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})
        field_schema = properties.get(field_name, {})
        return field_schema.get("sensitive", False)

    def get_sanitized_config(self) -> Dict[str, Any]:
        """Get configuration with sensitive fields masked."""
        config = self.get_template_config()
        sanitized = {}

        for key, value in config.items():
            if self.is_sensitive_field(key) and value:
                sanitized[key] = "*" * 8
            else:
                sanitized[key] = value

        return sanitized