"""
Configuration processing utility for MCP templates.

This module provides a unified way to process configuration from multiple sources
and handle special properties like volume mounts and command arguments.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

logger = logging.getLogger(__name__)


RESERVED_ENV_VARS = {
    "transport": "MCP_TRANSPORT",
    "log_level": "MCP_LOG_LEVEL",
    "read_only_mode": "MCP_READ_ONLY_MODE",
    "port": "MCP_PORT",
    "host": "MCP_HOST",
}


class ValidationResult:
    """Result of configuration validation."""

    def __init__(
        self,
        valid: bool = True,
        errors: List[str] = None,
        warnings: List[str] = None,
        missing_required: Optional[List[str]] = None,
        conditional_issues: Optional[List[Dict[str, Any]]] = None,
        suggestions: Optional[List[str]] = None,
    ):
        self.valid = valid
        self.errors = errors or []
        self.warnings = warnings or []
        # Fields to support conditional validator output
        self.missing_required = missing_required or []
        self.conditional_issues = conditional_issues or []
        self.suggestions = suggestions or []

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "missing_required": self.missing_required,
            "conditional_issues": self.conditional_issues,
            "suggestions": self.suggestions,
        }


class ConfigProcessor:
    """Unified configuration processor for MCP templates."""

    def __init__(self):
        """Initialize the configuration processor."""
        pass

    def _convert_overrides_to_env_vars(
        self, override_values: Dict[str, str]
    ) -> Dict[str, str]:
        """
        Convert override values to environment variables with OVERRIDE_ prefix.

        This allows the template's config.py to handle the override processing
        instead of the deployer trying to modify template.json directly.

        Args:
            override_values: Override values from CLI (e.g., {'capabilities__0__name': 'Hello Tool'})

        Returns:
            Dict with OVERRIDE_ prefixed environment variables
        """
        override_env_vars = {}

        for key, value in override_values.items():
            # Convert to environment variable with OVERRIDE_ prefix
            env_var_name = f"OVERRIDE_{key}"
            override_env_vars[env_var_name] = str(value)

        return override_env_vars

    def _apply_template_overrides(
        self, template_data: Dict[str, Any], override_values: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Apply template data overrides using double underscore notation."""
        import copy

        if not override_values:
            return copy.deepcopy(template_data)

        template_copy = copy.deepcopy(template_data)

        for key, value in override_values.items():
            # Split on double underscores to create nested path
            key_parts = key.split("__")

            # Navigate/create the nested structure
            current = template_copy
            for i, part in enumerate(key_parts[:-1]):
                # Check if this part is a numeric index for array access
                if part.isdigit():
                    index = int(part)
                    if isinstance(current, list):
                        # Extend list if needed
                        while len(current) <= index:
                            current.append({})
                        if i == len(key_parts) - 2:  # This is the parent of final key
                            if not isinstance(current[index], dict):
                                current[index] = {}
                        current = current[index]
                    else:
                        # DEBUG: Add more details about the failing field
                        logger.warning(
                            "Cannot index non-list field with %s (field type: %s, key_parts so far: %s)",
                            part,
                            type(current),
                            key_parts[: i + 1],
                        )
                        break
                else:
                    # Regular dictionary key access
                    if part not in current:
                        # Check if next part is a digit - if so, create array
                        if i + 1 < len(key_parts) and key_parts[i + 1].isdigit():
                            current[part] = []
                        else:
                            current[part] = {}
                    elif not isinstance(current[part], (dict, list)):
                        # Can't override non-dict/list with nested structure
                        logger.warning(
                            "Cannot override non-dict field %s with nested structure",
                            ".".join(key_parts[: i + 1]),
                        )
                        break
                    current = current[part]
            else:
                # Set the final value, converting types appropriately
                final_key = key_parts[-1]
                converted_value = self._convert_override_value(value)

                # Handle final array index
                if final_key.isdigit() and isinstance(current, list):
                    index = int(final_key)
                    while len(current) <= index:
                        current.append(None)
                    current[index] = converted_value
                else:
                    current[final_key] = converted_value

        return template_copy

    def _extract_config_overrides(
        self, override_values: Dict[str, str], template_data: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Extract config-related overrides that should be converted to environment variables.

        Args:
            override_values: Raw override values from CLI
            template_data: Template data with config_schema

        Returns:
            Dictionary of config overrides to be converted to env vars
        """
        config_overrides = {}
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})

        for key, value in override_values.items():
            # Check if this override key corresponds to a config schema property
            parts = key.split("__")

            # Direct property match
            if key in properties:
                config_overrides[key] = value
                continue

            # Check for nested property matches
            for prop_name, prop_config in properties.items():
                env_mapping = prop_config.get("env_mapping", "")

                # Match by environment mapping
                if env_mapping and (
                    key == env_mapping
                    or key.upper() == env_mapping.upper()
                    or key.replace("__", "_").upper() == env_mapping.upper()
                ):
                    config_overrides[prop_name] = value
                    break

                # Match nested structure patterns
                if len(parts) >= 2:
                    potential_matches = [
                        "_".join(parts),
                        "_".join(parts[1:]),
                        parts[-1],
                    ]

                    if prop_name in potential_matches:
                        config_overrides[prop_name] = value
                        break

        return config_overrides

    def _convert_override_value(self, value: str) -> Any:
        """Convert string override value to appropriate type."""
        # Handle boolean values
        if value.lower() in ("true", "false"):
            return value.lower() == "true"

        # Handle numeric values
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        # Handle JSON structures
        if value.startswith(("{", "[")):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass

        # Default to string
        return value

    def prepare_configuration(
        self,
        template: Dict[str, Any],
        env_vars: Optional[Dict[str, str]] = None,
        config_file: Optional[str] = None,
        config_values: Optional[Dict[str, str]] = None,
        session_config: Optional[Dict[str, Any]] = None,
        inline_config: Optional[List[str]] = None,
        env_var_list: Optional[List[str]] = None,
        override_values: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Prepare configuration from multiple sources with proper type conversion.

        Priority order (highest to lowest):
        1. env_vars (Dict)
        2. inline_config (List of KEY=VALUE)
        3. env_var_list (List of KEY=VALUE)
        4. config_values (Dict)
        5. config_file (JSON/YAML file)
        6. session_config (persistent session config)
        7. template defaults

        Args:
            template: Template configuration
            env_vars: Environment variables as dict (highest priority)
            config_file: Path to JSON/YAML configuration file
            config_values: Configuration values as dict
            session_config: Base configuration from interactive session
            inline_config: List of KEY=VALUE inline config pairs
            env_var_list: List of KEY=VALUE environment variable pairs

        Returns:
            Processed configuration dictionary
        """

        config = {}

        # Start with template defaults
        template_env = template.get("env_vars", {})
        for key, value in template_env.items():
            config[key] = value

        # Apply session config if provided
        if session_config:
            config.update(session_config)

        # Load from config file if provided
        if config_file:
            config.update(self._load_config_file(config_file, template))

        # Apply CLI config values with type conversion
        if config_values:
            config.update(self._convert_config_values(config_values, template))

        # Apply environment variable list (medium priority)
        if env_var_list:
            for pair in env_var_list:
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    config[k] = v

        # Apply inline config list (higher priority)
        if inline_config:
            for pair in inline_config:
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    config[k] = v

        # Apply environment variables dict (highest priority)
        if env_vars:
            config.update(env_vars)

        for reserved_key, reserved_value in RESERVED_ENV_VARS.items():
            if reserved_key in config:
                config[reserved_value] = config.pop(reserved_key)

        if override_values:
            override_env_vars = self._convert_overrides_to_env_vars(override_values)
            config.update(override_env_vars)

        return config

    def validate_config(
        self,
        config_or_template: Dict[str, Any],
        schema: Optional[Dict[str, Any]] = None,
        *,
        env_vars: Optional[Dict[str, str]] = None,
        config_file: Optional[str] = None,
        config_values: Optional[Dict[str, str]] = None,
        session_config: Optional[Dict[str, Any]] = None,
        inline_config: Optional[List[str]] = None,
        env_var_list: Optional[List[str]] = None,
        override_values: Optional[Dict[str, str]] = None,
    ) -> ValidationResult:
        """
        Validate configuration.

        Two modes supported:
        1. Classic: validate_config(config_dict, schema_dict) - validates a property-keyed
           `config_dict` against `schema_dict` and returns a ValidationResult.
        2. Template-normalization: validate_config(template_info, schema=None, **sources)
           When `schema` is None and `config_or_template` contains a `config_schema`, the
           method will normalize configuration sources (env vars, config file, overrides,
           etc.) via `prepare_configuration`, map env-vars back to property names, and
           perform the full validation including conditional requirements.

        Returns a ValidationResult which is valid only when all checks pass.
        """
        try:
            # Mode 2: template-normalization mode
            if (
                schema is None
                and isinstance(config_or_template, dict)
                and "config_schema" in config_or_template
            ):
                template_info = config_or_template
                config_schema = template_info.get("config_schema", {})

                # session_config is expected to be property-keyed config provided by caller
                session_env_config: Dict[str, Any] = {}
                if session_config:
                    try:
                        session_env_config = self._convert_config_values(
                            session_config, template_info
                        )
                    except Exception:
                        session_env_config = dict(session_config)

                prepared_env = self.prepare_configuration(
                    template_info,
                    env_vars=env_vars,
                    config_file=config_file,
                    config_values=config_values,
                    session_config=session_env_config,
                    inline_config=inline_config,
                    env_var_list=env_var_list,
                    override_values=override_values,
                )

                # Map prepared env-var keys back to property names for schema validation
                properties = config_schema.get("properties", {})
                effective_config: Dict[str, Any] = {}
                for prop_name, prop_info in properties.items():
                    env_mapping = prop_info.get("env_mapping", prop_name.upper())
                    if env_mapping in prepared_env:
                        raw_val = prepared_env[env_mapping]
                        # If the prepared env value is a string, try to coerce it
                        # to a native Python type so conditional checks compare
                        # booleans/numbers correctly against schema defaults.
                        if isinstance(raw_val, str):
                            coerced = ConfigProcessor()._convert_override_value(raw_val)
                        else:
                            coerced = raw_val
                        effective_config[prop_name] = coerced

                # Run plain validation (which will also check required fields)
                schema_result = ConfigProcessor.validate_config_schema(
                    config_schema, effective_config
                )

                # Merge with basic validation (existing checks)
                basic_result = self.validate_config(
                    effective_config, config_schema
                )  # classic mode

                # Combine results
                errors = basic_result.errors + schema_result.get("errors", [])
                warnings = basic_result.warnings

                valid = basic_result.valid and schema_result.get("valid", True)

                return ValidationResult(
                    valid=valid,
                    errors=errors,
                    warnings=warnings,
                    missing_required=schema_result.get("missing_required", []),
                    conditional_issues=schema_result.get("conditional_issues", []),
                    suggestions=schema_result.get("suggestions", []),
                )

            # Classic mode: config_or_template is a config dict and schema is provided
            config = config_or_template
            if not schema:
                return ValidationResult(valid=True)

            errors = []
            warnings = []

            # Check required fields
            required_fields = schema.get("required", [])
            for field in required_fields:
                field_env_var = (
                    schema.get("properties", {})
                    .get(field, {})
                    .get("env_mapping", field.upper())
                )
                if not (field in config or field_env_var in config):
                    errors.append(
                        f"Required field '{field}' (ENV VAR: {field_env_var}) is missing"
                    )

            # Check for unknown fields
            properties = schema.get("properties", {})
            if schema.get("additionalProperties", True) is False:
                for field in config:
                    if field not in properties:
                        warnings.append(f"Unknown field '{field}' in configuration")

            return ValidationResult(
                valid=len(errors) == 0, errors=errors, warnings=warnings
            )

        except Exception as e:
            logger.error("Config validation failed: %s", e)
            return ValidationResult(valid=False, errors=[f"Validation error: {str(e)}"])

    def handle_volume_and_args_config_properties(
        self,
        template: Dict[str, Any],
        config: Dict[str, Any],
        additional_volumes: Union[Dict[str, str], List, None] = None,
        default_mount_path: str = "/mnt",
    ) -> Dict[str, Any]:
        """
        Process properties that have volume_mount or command_arg set to True.
        These should not be treated as environment variables but as Docker volumes/commands.

        Args:
            template: Template configuration
            config: Configuration dictionary
            default_mount_path: Default mount path for volumes
            additional_volumes: Volumes provided by users in the command (if any)

        Returns:
            Dictionary with updated template and config
        """
        config_properties = template.get("config_schema", {}).get("properties", {})
        command = []
        volumes = {}

        # Make a copy to avoid modifying during iteration
        config_copy = config.copy()
        for prop_key, prop_value in config_properties.items():
            delete_key = False
            updated_config_container_path = ""
            env_var_name = prop_value.get("env_mapping", prop_key.upper())
            container_mount_path = None
            host_path = None
            final_container_paths = []

            # Check if this property is a volume mount
            if (
                env_var_name in config_copy
                and prop_value.get("volume_mount", False) is True
            ):
                config_value = config_copy[env_var_name]

                # Clean up the value - remove Docker command artifacts and split by space
                # Handle cases where users accidentally include Docker syntax
                cleaned_value = config_value.strip()

                # Remove common Docker command artifacts more carefully
                docker_artifacts = ["--volume ", "-v ", "--env ", "-e "]
                for artifact in docker_artifacts:
                    cleaned_value = cleaned_value.replace(artifact, " ")

                # Also handle cases where artifacts are at the end
                end_artifacts = ["--volume", "-v", "--env", "-e"]
                for artifact in end_artifacts:
                    if cleaned_value.endswith(artifact):
                        cleaned_value = cleaned_value[: -len(artifact)]

                # Split by space to handle multiple paths, then filter out empty strings
                path_parts = [
                    part.strip() for part in cleaned_value.split() if part.strip()
                ]

                for path_part in path_parts:
                    if not path_part:
                        continue

                    mount_value = path_part.split(":")
                    container_mount_path = None
                    host_path = None

                    # In most cases, it would be only the host path
                    if len(mount_value) == 1:
                        container_mount_path = None
                        host_path = mount_value[0]
                    elif len(mount_value) == 2:
                        # Assume format is host_path:container_path
                        container_mount_path = mount_value[1]
                        host_path = mount_value[0]
                    else:
                        logger.warning("Invalid volume mount format: %s", path_part)
                        continue  # Skip this path and continue with others

                    if host_path:
                        if container_mount_path:
                            volumes[host_path] = container_mount_path
                            final_container_paths.append(container_mount_path)
                        else:
                            container_mount_path = (
                                f"{default_mount_path}/{host_path.lstrip('/')}"
                            )
                            volumes[host_path] = container_mount_path
                            final_container_paths.append(container_mount_path)

                    # If we are to pass this to container then it should be mounted path
                    updated_config_container_path = container_mount_path

            # Check if this property is a command argument
            if (
                env_var_name in config_copy
                and prop_value.get("command_arg", False) is True
            ):
                # If this property is both volume_mount and command_arg, use container paths
                if final_container_paths:
                    # Use the container paths for command arguments since the container
                    # needs to access the mounted paths, not the original host paths
                    command.extend(final_container_paths)
                else:
                    # If not a volume mount, use the original value as-is
                    config_value = config_copy[env_var_name]

                    # Clean up the value for command arguments (remove Docker artifacts if any)
                    cleaned_value = config_value.strip()
                    docker_artifacts = ["--volume ", "-v ", "--env ", "-e "]
                    for artifact in docker_artifacts:
                        cleaned_value = cleaned_value.replace(artifact, " ")

                    end_artifacts = ["--volume", "-v", "--env", "-e"]
                    for artifact in end_artifacts:
                        if cleaned_value.endswith(artifact):
                            cleaned_value = cleaned_value[: -len(artifact)]

                    # For space-separated paths in command args, split and add each
                    path_parts = [
                        part.strip() for part in cleaned_value.split() if part.strip()
                    ]
                    command.extend(path_parts)

                delete_key = True

            if delete_key:
                # Remove the key from config to avoid duplication
                config.pop(env_var_name, None)
            elif updated_config_container_path:
                config[env_var_name] = updated_config_container_path

        # Update template with volumes and commands
        if "volumes" not in template or template["volumes"] is None:
            template["volumes"] = {}

        if isinstance(additional_volumes, dict):
            # Key is local path and value is host path
            volumes.update(additional_volumes)
        elif isinstance(additional_volumes, list):
            additional_volumes = {
                vol: f"{default_mount_path}/{vol.lstrip('/')}"
                for vol in additional_volumes
            }
            volumes.update(additional_volumes)

        template["volumes"].update(volumes)

        if "command" not in template or template["command"] is None:
            template["command"] = []
        template["command"].extend(command)

        return {
            "template": template,
            "config": config,
        }

    def _load_json_yaml_config_file(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON/YAML file and map to environment variables."""
        file_config = {}

        try:
            config_path = Path(config_file)
            if not config_path.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")

            # Load based on extension
            if config_path.suffix.lower() in [".yaml", ".yml"]:
                with open(config_path, "r", encoding="utf-8") as f:
                    file_config = yaml.safe_load(f)
            else:
                with open(config_path, "r", encoding="utf-8") as f:
                    file_config = json.load(f)

        except Exception as e:
            logger.error("Failed to load config file %s: %s", config_file, e)
            raise

        return file_config

    def _load_config_file(
        self, config_file: str, template: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Load configuration from JSON/YAML file and map to environment variables."""
        try:
            file_config = self._load_json_yaml_config_file(config_file)
            return self._map_file_config_to_env(file_config, template)

        except Exception as e:
            logger.error("Failed to load config file %s: %s", config_file, e)
            raise

    def _map_file_config_to_env(
        self, file_config: Dict[str, Any], template: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Map config file values to environment variables based on template schema."""
        env_config = {}

        # Get the config schema from template
        config_schema = template.get("config_schema", {})
        properties = config_schema.get("properties", {})

        # Generic mapping: try to map config values directly to properties
        # First, try direct property name mapping
        for prop_name, prop_config in properties.items():
            env_mapping = prop_config.get("env_mapping", prop_name.upper())

            # Try direct property name mapping
            if prop_name in file_config:
                env_config[env_mapping] = self._convert_value_to_env_string(
                    file_config[prop_name], prop_config
                )

            # Try nested mapping patterns
            else:
                nested_value = self._find_nested_config_value(
                    file_config, prop_name, prop_config
                )
                if nested_value is not None:
                    env_config[env_mapping] = self._convert_value_to_env_string(
                        nested_value, prop_config
                    )

        return env_config

    def _find_nested_config_value(
        self, file_config: Dict[str, Any], prop_name: str, prop_config: Dict[str, Any]
    ) -> Any:
        """Find config value using common nested patterns."""
        # Check if property config has a file_mapping hint
        if "file_mapping" in prop_config:
            return self._get_nested_value(file_config, prop_config["file_mapping"])

        # Try common nested patterns based on property name
        common_patterns = self._generate_common_patterns(prop_name)
        for pattern in common_patterns:
            try:
                value = self._get_nested_value(file_config, pattern)
                if value is not None:
                    return value
            except (KeyError, AttributeError):
                continue

        return None

    def _generate_common_patterns(self, prop_name: str) -> List[str]:
        """Generate common nested configuration patterns for a property."""
        patterns = []

        # Common category mappings
        category_mappings = {
            "log_level": ["logging.level", "log.level"],
            "enable_audit_logging": [
                "logging.enableAudit",
                "logging.audit",
                "log.audit",
            ],
            "read_only_mode": ["security.readOnly", "security.readonly", "readonly"],
            "max_file_size": [
                "security.maxFileSize",
                "limits.maxFileSize",
                "performance.maxFileSize",
            ],
            "allowed_directories": [
                "security.allowedDirs",
                "security.directories",
                "paths.allowed",
            ],
            "exclude_patterns": [
                "security.excludePatterns",
                "security.exclude",
                "filters.exclude",
            ],
            "max_concurrent_operations": [
                "performance.maxConcurrentOperations",
                "limits.concurrent",
            ],
            "timeout_ms": [
                "performance.timeoutMs",
                "performance.timeout",
                "limits.timeout",
            ],
        }

        if prop_name in category_mappings:
            patterns.extend(category_mappings[prop_name])

        # Generate generic patterns
        camel_name = self._snake_to_camel(prop_name)
        patterns.extend(
            [
                f"config.{prop_name}",
                f"settings.{prop_name}",
                f"options.{prop_name}",
                f"config.{camel_name}",
                f"settings.{camel_name}",
                f"options.{camel_name}",
            ]
        )

        return patterns

    def _snake_to_camel(self, snake_str: str) -> str:
        """Convert snake_case to camelCase."""
        components = snake_str.split("_")
        return components[0] + "".join(word.capitalize() for word in components[1:])

    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from dictionary using dot notation."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                raise KeyError(f"Key '{key}' not found in path '{path}'")
        return value

    def _convert_value_to_env_string(
        self, value: Any, prop_config: Dict[str, Any]
    ) -> str:
        """Convert a value to environment variable string format."""
        if isinstance(value, list):
            return ",".join(str(item) for item in value)
        elif isinstance(value, bool):
            return "true" if value else "false"
        else:
            return str(value)

    def _convert_config_values(
        self, config_values: Dict[str, str], template: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert CLI config values to proper types based on template schema."""
        converted_config = {}
        # Get the config schema from template
        config_schema = template.get("config_schema", {})
        properties = config_schema.get("properties", {})

        for key, value in config_values.items():
            # Special handling for VOLUMES key from CLI --volumes parameter
            if key == "VOLUMES":
                # Keep VOLUMES as-is (dict) for later processing
                converted_config[key] = value
                continue

            # Handle nested CLI config using double underscore notation
            if "__" in key:
                nested_key = self._handle_nested_cli_config(key, value, properties)
                if nested_key:
                    key = nested_key

            # Find the property config for type conversion
            prop_config = None
            env_mapping = None

            # Try to find the property by name or env_mapping
            for prop_name, prop_cfg in properties.items():
                prop_env_mapping = prop_cfg.get("env_mapping", prop_name.upper())
                if key == prop_name or key == prop_env_mapping:
                    prop_config = prop_cfg
                    env_mapping = prop_env_mapping
                    break

            # Convert value based on property type
            if prop_config:
                prop_type = prop_config.get("type", "string")
                try:
                    if prop_type == "boolean":
                        converted_config[env_mapping] = str(value).lower() in (
                            "true",
                            "1",
                            "yes",
                        )
                    elif prop_type == "integer":
                        converted_config[env_mapping] = int(value)
                    elif prop_type == "number":
                        converted_config[env_mapping] = float(value)
                    elif prop_type == "array":
                        # Handle comma-separated values
                        if isinstance(value, str):
                            converted_config[env_mapping] = value
                        else:
                            converted_config[env_mapping] = ",".join(
                                str(v) for v in value
                            )
                    else:
                        converted_config[env_mapping] = str(value)
                except (ValueError, TypeError) as e:
                    logger.warning(
                        "Failed to convert %s=%s to %s: %s",
                        key,
                        value,
                        prop_type,
                        e,
                    )
                    converted_config[env_mapping] = str(value)
            else:
                # No property config found, use the key as-is
                converted_config[key] = str(value)

        return converted_config

    def _handle_nested_cli_config(
        self, nested_key: str, value: str, properties: Dict[str, Any]
    ) -> Optional[str]:
        """Handle nested CLI configuration using double underscore notation."""
        # Convert security__read_only to find read_only_mode in properties
        # Also supports template__property notation for template-level overrides
        parts = nested_key.split("__")
        if len(parts) < 2:
            return None

        # Handle template-level overrides (template_name__property)
        if len(parts) == 2:
            category, prop = parts
            # Try to find property with this category prefix
            for prop_name in properties:
                if prop_name.startswith(category.lower()) or prop_name == prop:
                    return prop_name
        elif len(parts) == 3:
            # Handle three-part notation: category__subcategory__property
            category, subcategory, prop = parts
            search_patterns = [
                f"{category}_{subcategory}_{prop}",
                f"{category}_{prop}",
                prop,
            ]
            for pattern in search_patterns:
                if pattern in properties:
                    return pattern

        return None

    # -------------------------------
    # Conditional validation methods
    # These were previously part of ConditionalConfigValidator. They are
    # incorporated here to keep configuration processing and validation
    # responsibilities in a single class.
    # -------------------------------

    @staticmethod
    def validate_config_schema(
        config_schema: Dict[str, Any], config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate config against JSON Schema with support for conditional requirements.
        Returns validation result with missing_required, conditional_issues, and suggestions.
        """
        result = {
            "valid": True,
            "missing_required": [],
            "conditional_issues": [],
            "suggestions": [],
        }

        properties = config_schema.get("properties", {})

        # Check basic required fields first
        basic_required = config_schema.get("required", [])
        for prop in basic_required:
            if prop not in config or config[prop] is None:
                result["missing_required"].append(prop)
                result["valid"] = False

        # Process inline conditional rule list pattern: anyOf used as collection of independent if/then rules
        # or traditional anyOf (at least one satisfied). We first scan for if/then usage.
        if "anyOf" in config_schema:
            anyof_items = config_schema["anyOf"]
            has_inline_if = any(
                isinstance(item, dict) and "if" in item and "then" in item
                for item in anyof_items
            )
            if has_inline_if:
                # Treat each element as independent rule; all rules that trigger must have their 'then' requirements satisfied
                for i, rule in enumerate(anyof_items):
                    if not isinstance(rule, dict) or "if" not in rule:
                        continue
                    if_cond = rule.get("if", {})
                    then_cond = rule.get("then", {})
                    if_result = ConfigProcessor._check_condition(
                        if_cond, config, properties
                    )
                    if if_result["satisfied"]:
                        # Enforce then.required
                        missing_then = []
                        for req in then_cond.get("required", []):
                            if req not in config or config.get(req) in (None, ""):
                                missing_then.append(req)
                        if missing_then:
                            result["valid"] = False
                            result["conditional_issues"].append(
                                {
                                    "rule_index": i,
                                    "type": "if/then",
                                    "triggered": True,
                                    "missing": missing_then,
                                }
                            )
                # no traditional anyOf semantics when inline if/then pattern detected
            else:
                any_of_satisfied = False
                conditional_errors = []
                for i, condition in enumerate(anyof_items):
                    condition_result = ConfigProcessor._check_condition(
                        condition, config, properties
                    )
                    if condition_result["satisfied"]:
                        any_of_satisfied = True
                        break
                    else:
                        conditional_errors.append(
                            {
                                "condition_index": i,
                                "condition": condition,
                                "errors": condition_result["errors"],
                                "missing": condition_result["missing"],
                            }
                        )
                if not any_of_satisfied:
                    result["valid"] = False
                    result["conditional_issues"].extend(conditional_errors)
                    suggestions = ConfigProcessor._generate_config_suggestions(
                        config_schema, config
                    )
                    result["suggestions"] = suggestions

        if "oneOf" in config_schema:
            satisfied_conditions = 0
            one_of_errors = []

            for i, condition in enumerate(config_schema["oneOf"]):
                condition_result = ConfigProcessor._check_condition(
                    condition, config, properties
                )
                if condition_result["satisfied"]:
                    satisfied_conditions += 1
                else:
                    one_of_errors.append(
                        {
                            "condition_index": i,
                            "condition": condition,
                            "errors": condition_result["errors"],
                            "missing": condition_result["missing"],
                        }
                    )

            if satisfied_conditions != 1:
                result["valid"] = False
                if satisfied_conditions == 0:
                    result["conditional_issues"].extend(one_of_errors)
                else:
                    result["conditional_issues"].append(
                        {
                            "error": f"Multiple conditions satisfied in oneOf (found {satisfied_conditions})"
                        }
                    )

        # Support top-level if/then/else conditional blocks
        if "if" in config_schema:
            try:
                if_condition = config_schema["if"]
                if_result = ConfigProcessor._check_condition(
                    if_condition, config, properties
                )

                # Decide which branch to validate
                branch = "then" if if_result["satisfied"] else "else"
                branch_schema = config_schema.get(branch, {})

                branch_missing = []
                branch_errors = []

                # Check required in the branch, honoring schema defaults when the
                # property is not present in the provided config.
                for prop in branch_schema.get("required", []):
                    prop_default = properties.get(prop, {}).get("default")
                    current_value = config.get(prop, prop_default)
                    if current_value is None:
                        branch_missing.append(prop)

                # Also check any property const/enum constraints inside branch.properties
                for prop, constraint in branch_schema.get("properties", {}).items():
                    prop_default = properties.get(prop, {}).get("default")
                    current_value = config.get(prop, prop_default)
                    if "const" in constraint and current_value != constraint["const"]:
                        branch_errors.append(f"{prop} must be '{constraint['const']}'")
                    if "enum" in constraint and current_value not in constraint["enum"]:
                        branch_errors.append(
                            f"{prop} must be one of {constraint['enum']}"
                        )

                if branch_missing or branch_errors:
                    result["valid"] = False
                    result["conditional_issues"].append(
                        {
                            "branch": branch,
                            "missing": branch_missing,
                            "errors": branch_errors,
                        }
                    )

                    # Generate suggestions when possible
                    suggestions = ConfigProcessor._generate_config_suggestions(
                        config_schema, config
                    )
                    if suggestions:
                        result["suggestions"].extend(suggestions)

            except Exception as e:
                result["valid"] = False
                result["conditional_issues"].append({"error": str(e)})

        # Support allOf (every condition MUST be satisfied). Supports regular property-based conditions
        # and mixed with inline if/then semantics inside each element.
        if "allOf" in config_schema:
            for i, condition in enumerate(config_schema["allOf"]):
                if (
                    isinstance(condition, dict)
                    and "if" in condition
                    and "then" in condition
                ):
                    # Inline rule style inside allOf
                    if_res = ConfigProcessor._check_condition(
                        condition["if"], config, properties
                    )
                    if if_res["satisfied"]:
                        missing_then = [
                            r
                            for r in condition["then"].get("required", [])
                            if r not in config or config.get(r) in (None, "")
                        ]
                        if missing_then:
                            result["valid"] = False
                            result["conditional_issues"].append(
                                {
                                    "clause": "allOf",
                                    "index": i,
                                    "missing": missing_then,
                                }
                            )
                    continue
                # Traditional condition object
                cond_result = ConfigProcessor._check_condition(
                    condition, config, properties
                )
                if not cond_result["satisfied"]:
                    result["valid"] = False
                    result["conditional_issues"].append(
                        {
                            "clause": "allOf",
                            "index": i,
                            "errors": cond_result["errors"],
                            "missing": cond_result["missing"],
                        }
                    )

        return result

    @staticmethod
    def _check_condition(
        condition: Dict[str, Any], config: Dict[str, Any], properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check if a single condition (from anyOf/oneOf) is satisfied."""
        result = {"satisfied": True, "errors": [], "missing": []}

        # Check property constraints
        if "properties" in condition:
            for prop, constraint in condition["properties"].items():
                # Use the property's default from schema if not present in config
                prop_default = properties.get(prop, {}).get("default")
                current_value = config.get(prop, prop_default)

                if "const" in constraint:
                    if current_value != constraint["const"]:
                        result["satisfied"] = False
                        result["errors"].append(
                            f"{prop} must be '{constraint['const']}'"
                        )
                elif "enum" in constraint:
                    if current_value not in constraint["enum"]:
                        result["satisfied"] = False
                        result["errors"].append(
                            f"{prop} must be one of {constraint['enum']}"
                        )

        # Check required fields for this condition
        if "required" in condition:
            for prop in condition["required"]:
                prop_default = properties.get(prop, {}).get("default")
                if prop not in config and prop_default is None:
                    result["satisfied"] = False
                    result["missing"].append(prop)

        # Handle nested oneOf within conditions
        if "oneOf" in condition:
            nested_satisfied = 0
            for nested_condition in condition["oneOf"]:
                nested_result = ConfigProcessor._check_condition(
                    nested_condition, config, properties
                )
                if nested_result["satisfied"]:
                    nested_satisfied += 1

            if nested_satisfied != 1:
                result["satisfied"] = False
                if nested_satisfied == 0:
                    result["errors"].append(
                        "None of the authentication methods are satisfied"
                    )
                else:
                    result["errors"].append(
                        "Multiple authentication methods provided (only one allowed)"
                    )

        return result

    @staticmethod
    def _generate_config_suggestions(
        config_schema: Dict[str, Any], config: Dict[str, Any]
    ) -> List[str]:
        """Generate helpful suggestions based on conditional validation failures."""
        suggestions: List[str] = []
        properties = config_schema.get("properties", {})

        # Generic suggestions based on anyOf/oneOf structure
        if "anyOf" in config_schema:
            for i, condition in enumerate(config_schema["anyOf"]):
                if "properties" in condition:
                    for prop, constraint in condition["properties"].items():
                        if "const" in constraint:
                            prop_info = properties.get(prop, {})
                            prop_title = prop_info.get("title", prop)
                            current_value = config.get(prop)
                            if current_value != constraint["const"]:
                                suggestions.append(
                                    f"Option {i+1}: Set '{prop_title}' to '{constraint['const']}'"
                                )
                                if "required" in condition:
                                    required_props = [
                                        properties.get(p, {}).get("title", p)
                                        for p in condition["required"]
                                    ]
                                    suggestions.append(
                                        f"  Then configure: {', '.join(required_props)}"
                                    )

        # Elasticsearch/OpenSearch specific suggestions (backwards compatibility)
        current_engine = config.get(
            "engine_type", properties.get("engine_type", {}).get("default")
        )

        if current_engine == "elasticsearch":
            # Check if elasticsearch_hosts is missing
            if "elasticsearch_hosts" not in config:
                suggestions.append(
                    "For Elasticsearch: set 'elasticsearch_hosts' to your cluster URL"
                )

            # Check authentication
            has_api_key = "elasticsearch_api_key" in config
            has_basic_auth = (
                "elasticsearch_username" in config
                and "elasticsearch_password" in config
            )

            if not has_api_key and not has_basic_auth:
                suggestions.append(
                    "For Elasticsearch: choose either API key authentication (elasticsearch_api_key) OR basic authentication (elasticsearch_username + elasticsearch_password)"
                )

        elif current_engine == "opensearch":
            missing_opensearch = []
            if "opensearch_hosts" not in config:
                missing_opensearch.append("opensearch_hosts")
            if "opensearch_username" not in config:
                missing_opensearch.append("opensearch_username")
            if "opensearch_password" not in config:
                missing_opensearch.append("opensearch_password")

            if missing_opensearch:
                suggestions.append(
                    f"For OpenSearch: set {', '.join(missing_opensearch)}"
                )

        elif "engine_type" in properties:
            # Only suggest engine_type if it's actually a property in this template
            suggestions.append(
                "Set 'engine_type' to either 'elasticsearch' or 'opensearch'"
            )

        return suggestions

    @staticmethod
    def is_conditionally_required(
        prop_name: str, config_schema: Dict[str, Any], current_config: Dict[str, Any]
    ) -> bool:
        """Check if a property is conditionally required based on current config."""
        # Check anyOf conditions
        if "anyOf" in config_schema:
            for condition in config_schema["anyOf"]:
                if ConfigProcessor._prop_required_in_condition(
                    prop_name, condition, current_config
                ):
                    return True

        # Check oneOf conditions
        if "oneOf" in config_schema:
            for condition in config_schema["oneOf"]:
                if ConfigProcessor._prop_required_in_condition(
                    prop_name, condition, current_config
                ):
                    return True

        return False

    @staticmethod
    def _prop_required_in_condition(
        prop_name: str, condition: Dict[str, Any], current_config: Dict[str, Any]
    ) -> bool:
        """Check if a property is required in a specific condition."""
        # First check if the condition applies to current config
        if "properties" in condition:
            for constraint_prop, constraint in condition["properties"].items():
                if "const" in constraint:
                    if current_config.get(constraint_prop) != constraint["const"]:
                        return False  # This condition doesn't apply

        # If condition applies, check if prop is required
        if "required" in condition and prop_name in condition["required"]:
            return True

        # Check nested oneOf conditions
        if "oneOf" in condition:
            for nested_condition in condition["oneOf"]:
                if ConfigProcessor._prop_required_in_condition(
                    prop_name, nested_condition, current_config
                ):
                    return True

        return False

    def check_missing_config(
        self,
        template_info: Dict[str, Any],
        config: Dict[str, Any],
        env_vars: Dict[str, str],
        config_file: Optional[str] = None,
        config_values: Optional[Dict[str, str]] = None,
        inline_config: Optional[List[str]] = None,
        env_var_list: Optional[List[str]] = None,
        override_values: Optional[Dict[str, str]] = None,
    ) -> ValidationResult:
        """Check for missing required configuration with conditional requirements support.

        This method accepts alternate configuration inputs (config file, CLI
        config values, inline config and env var lists, and override values).
        It delegates normalization of these sources to :meth:`prepare_configuration`
        so all inputs are converted into a single env-var-keyed mapping. The
        conditional validator then maps those env-var keys back to property
        names (using `env_mapping` from the template's config schema) and runs
        conditional validation over the consolidated view.
        """
        config_schema = template_info.get("config_schema", {})

        # The incoming `config` is property-keyed (e.g., {'api_key': 'x'}). The
        # prepare_configuration expects session_config to be env-var keyed.
        session_env_config: Dict[str, Any] = {}
        if config:
            try:
                session_env_config = self._convert_config_values(config, template_info)
            except Exception:
                session_env_config = dict(config)

        prepared_env = self.prepare_configuration(
            template_info,
            env_vars=env_vars,
            config_file=config_file,
            config_values=config_values,
            session_config=session_env_config,
            inline_config=inline_config,
            env_var_list=env_var_list,
            override_values=override_values,
        )

        # Map prepared env-var keys back to property names for schema validation
        properties = config_schema.get("properties", {})
        effective_config: Dict[str, Any] = {}
        for prop_name, prop_info in properties.items():
            env_mapping = prop_info.get("env_mapping", prop_name.upper())
            if env_mapping in prepared_env:
                effective_config[prop_name] = prepared_env[env_mapping]

        # Run conditional validation on the property-keyed effective config
        schema_result = ConfigProcessor.validate_config_schema(
            config_schema, effective_config
        )

        # Build a ValidationResult to return richer information
        return ValidationResult(
            valid=schema_result.get("valid", True),
            errors=schema_result.get("errors", []),
            warnings=schema_result.get("warnings", []),
            missing_required=schema_result.get("missing_required", []),
            conditional_issues=schema_result.get("conditional_issues", []),
            suggestions=schema_result.get("suggestions", []),
        )
