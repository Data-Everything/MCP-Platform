"""
Unit tests for the ConfigProcessor utility.

This module tests the unified configuration processing functionality that handles
merging configuration from multiple sources and processing volume mounts and command arguments.
"""

import json
from unittest.mock import mock_open, patch

import pytest
import yaml

from mcp_platform.core.config_processor import ConfigProcessor, ValidationResult


@pytest.mark.unit
class TestConfigProcessor:
    """Test ConfigProcessor class for unified configuration processing."""


@pytest.mark.unit
class TestConfigProcessorPrepareConfiguration:
    """Test ConfigProcessor.prepare_configuration method."""

    def test_prepare_configuration_template_defaults_only(
        self, config_processor, sample_template
    ):
        """Test configuration preparation with only template defaults."""
        result = config_processor.prepare_configuration(template=sample_template)

        assert result["LOG_LEVEL"] == "INFO"

    def test_prepare_configuration_with_session_config(
        self, config_processor, sample_template
    ):
        """Test configuration preparation with session config."""
        session_config = {"LOG_LEVEL": "DEBUG", "CUSTOM_VAR": "session_value"}

        result = config_processor.prepare_configuration(
            template=sample_template, session_config=session_config
        )

        assert result["LOG_LEVEL"] == "DEBUG"
        assert result["CUSTOM_VAR"] == "session_value"

    def test_prepare_configuration_with_config_values(
        self, config_processor, sample_template
    ):
        """Test configuration preparation with config values."""
        config_values = {"log_level": "WARNING", "allowed_dirs": "/tmp"}

        result = config_processor.prepare_configuration(
            template=sample_template, config_values=config_values
        )

        assert result["LOG_LEVEL"] == "WARNING"
        assert result["ALLOWED_DIRS"] == "/tmp"

    def test_prepare_configuration_with_env_vars(
        self, config_processor, sample_template
    ):
        """Test configuration preparation with environment variables."""
        env_vars = {"LOG_LEVEL": "ERROR", "CUSTOM_ENV": "env_value"}

        result = config_processor.prepare_configuration(
            template=sample_template, env_vars=env_vars
        )

        assert result["LOG_LEVEL"] == "ERROR"
        assert result["CUSTOM_ENV"] == "env_value"

    def test_prepare_configuration_priority_order(
        self, config_processor, sample_template
    ):
        """Test that configuration sources are merged in correct priority order."""
        session_config = {"LOG_LEVEL": "session", "session_only": "session"}
        config_values = {"log_level": "config", "config_only": "config"}
        env_vars = {"LOG_LEVEL": "env", "env_only": "env"}

        result = config_processor.prepare_configuration(
            template=sample_template,
            session_config=session_config,
            config_values=config_values,
            env_vars=env_vars,
        )

        # env_vars should have highest priority
        assert result["LOG_LEVEL"] == "env"
        assert result["session_only"] == "session"
        assert result["config_only"] == "config"
        assert result["env_only"] == "env"

    def test_prepare_configuration_with_config_file_json(self, config_processor):
        """Test configuration preparation with JSON config file."""
        template = {
            "config_schema": {
                "properties": {
                    "log_level": {"env_mapping": "LOG_LEVEL"},
                    "port": {"env_mapping": "PORT", "type": "integer"},
                }
            },
            "env_vars": {},
        }

        file_content = {"log_level": "DEBUG", "port": 9000}

        with patch("builtins.open", mock_open(read_data=json.dumps(file_content))):
            with patch("pathlib.Path.exists", return_value=True):
                result = config_processor.prepare_configuration(
                    template=template, config_file="config.json"
                )

                assert result["LOG_LEVEL"] == "DEBUG"
                assert result["PORT"] == "9000"  # Config file values become strings

    def test_prepare_configuration_with_config_file_yaml(self, config_processor):
        """Test configuration preparation with YAML config file."""
        template = {
            "config_schema": {
                "properties": {
                    "log_level": {"env_mapping": "LOG_LEVEL"},
                    "enable_feature": {
                        "env_mapping": "ENABLE_FEATURE",
                        "type": "boolean",
                    },
                }
            },
            "env_vars": {},
        }

        file_content = {"log_level": "WARNING", "enable_feature": True}

        with patch("builtins.open", mock_open(read_data=yaml.dump(file_content))):
            with patch("pathlib.Path.exists", return_value=True):
                with patch("yaml.safe_load", return_value=file_content):
                    result = config_processor.prepare_configuration(
                        template=template, config_file="config.yaml"
                    )

                    assert result["LOG_LEVEL"] == "WARNING"
                    assert (
                        result["ENABLE_FEATURE"] == "true"
                    )  # Config file values become strings

    def test_prepare_configuration_config_file_not_found(self, config_processor):
        """Test configuration preparation with non-existent config file."""
        template = {"config_schema": {"properties": {}}, "env_vars": {}}

        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError):
                config_processor.prepare_configuration(
                    template=template, config_file="nonexistent.json"
                )


@pytest.mark.unit
class TestConfigProcessorConditionals:
    """Tests for conditional validation (if/then/else, anyOf, oneOf)."""

    def test_if_then_requires_password_when_oauth_disabled(self, config_processor):
        """When oauth_enabled is false, trino_password should be required."""
        template = {
            "config_schema": {
                "properties": {
                    "oauth_enabled": {
                        "type": "boolean",
                        "env_mapping": "OAUTH_ENABLED",
                    },
                    "trino_password": {
                        "type": "string",
                        "env_mapping": "TRINO_PASSWORD",
                    },
                },
                "required": ["trino_host", "trino_user"],
                "if": {"properties": {"oauth_enabled": {"const": False}}},
                "then": {"required": ["trino_password"]},
                "else": {"required": ["oauth_provider"]},
            }
        }

        # Simulate prepared effective config where oauth_enabled is false and password missing
        effective_config = {
            "trino_host": "h",
            "trino_user": "u",
            "oauth_enabled": False,
        }

        res = ConfigProcessor.validate_config_schema(
            template["config_schema"], effective_config
        )
        assert res["valid"] is False
        assert (
            any(
                "trino_password" in (issue.get("missing") or [])
                for issue in res["conditional_issues"]
            )
            or "trino_password" in res["missing_required"]
        )

    def test_if_else_requires_oauth_provider_when_enabled(self, config_processor):
        """When oauth_enabled is true, oauth_provider should be required."""
        template = {
            "config_schema": {
                "properties": {
                    "oauth_enabled": {
                        "type": "boolean",
                        "env_mapping": "OAUTH_ENABLED",
                    },
                    "oauth_provider": {
                        "type": "string",
                        "env_mapping": "OAUTH_PROVIDER",
                    },
                },
                "required": ["trino_host", "trino_user"],
                "if": {"properties": {"oauth_enabled": {"const": False}}},
                "then": {"required": ["trino_password"]},
                "else": {"required": ["oauth_provider"]},
            }
        }

        effective_config = {"trino_host": "h", "trino_user": "u", "oauth_enabled": True}

        res = ConfigProcessor.validate_config_schema(
            template["config_schema"], effective_config
        )
        assert res["valid"] is False
        assert (
            any(
                "oauth_provider" in (issue.get("missing") or [])
                for issue in res["conditional_issues"]
            )
            or "oauth_provider" in res["missing_required"]
        )


@pytest.mark.unit
class TestConfigProcessorNestedAndSuggestions:
    """Tests for nested oneOf and suggestion generation."""

    def test_nested_oneof_auth_methods_rejects_multiple(self):
        """If nested oneOf lists multiple auth methods provided, validation should fail."""
        schema = {
            "properties": {
                "basic_user": {"type": "string", "title": "Basic User"},
                "basic_password": {"type": "string", "title": "Basic Password"},
                "oauth_provider": {"type": "string", "title": "OAuth Provider"},
                "jwt_secret": {"type": "string", "title": "JWT Secret"},
            },
            "oneOf": [
                {"required": ["basic_user", "basic_password"]},
                {"required": ["oauth_provider", "jwt_secret"]},
            ],
        }

        # Provide both basic and oauth fields to simulate conflicting methods
        config = {
            "basic_user": "u",
            "basic_password": "p",
            "oauth_provider": "google",
            "jwt_secret": "s",
        }

        res = ConfigProcessor.validate_config_schema(schema, config)
        assert res["valid"] is False
        # Should report multiple satisfied conditions in oneOf
        assert any(
            isinstance(issue, dict)
            and "Multiple conditions satisfied" in issue.get("error", "")
            for issue in res["conditional_issues"]
        )

    def test_anyof_suggestion_generation(self):
        """When anyOf fails, generate helpful suggestions pointing to required settings."""
        schema = {
            "properties": {
                "oauth_enabled": {"type": "boolean", "title": "Enable OAuth"},
                "trino_password": {"type": "string", "title": "Trino Password"},
                "oauth_provider": {"type": "string", "title": "OAuth Provider"},
            },
            "anyOf": [
                {
                    "properties": {"oauth_enabled": {"const": False}},
                    "required": ["trino_password"],
                },
                {
                    "properties": {"oauth_enabled": {"const": True}},
                    "required": ["oauth_provider"],
                },
            ],
        }

        # Config that satisfies neither option (oauth_enabled is missing)
        config = {"trino_host": "h", "trino_user": "u"}

        res = ConfigProcessor.validate_config_schema(schema, config)
        assert res["valid"] is False
        # Suggestions should be non-empty and mention setting Enable OAuth or provider/password
        assert res["suggestions"]
        assert any(
            "Enable OAuth" in s or "OAuth Provider" in s or "Trino Password" in s
            for s in res["suggestions"]
        )


@pytest.mark.unit
class TestConfigProcessorVolumeAndCommandArgs:
    """Test ConfigProcessor.handle_volume_and_args_config_properties method."""

    def test_handle_volume_mount_property(self, config_processor):
        """Test handling of volume mount properties."""
        template = {
            "config_schema": {
                "properties": {
                    "data_dir": {
                        "env_mapping": "DATA_DIR",
                        "volume_mount": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        config = {"DATA_DIR": "/host/data"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should create volume mount
        assert "/host/data" in result["template"]["volumes"]
        assert result["template"]["volumes"]["/host/data"] == "/mnt/host/data"

    def test_handle_command_arg_property(self, config_processor):
        """Test handling of command argument properties."""
        template = {
            "config_schema": {
                "properties": {
                    "server_args": {
                        "env_mapping": "SERVER_ARGS",
                        "command_arg": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        config = {"SERVER_ARGS": "--verbose --debug"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should add to command (split into individual arguments)
        assert "--verbose" in result["template"]["command"]
        assert "--debug" in result["template"]["command"]

        # Should remove from config
        assert "SERVER_ARGS" not in result["config"]

    def test_handle_combined_volume_and_command_property(self, config_processor):
        """Test handling of properties that are both volume mount and command arg."""
        template = {
            "config_schema": {
                "properties": {
                    "shared_path": {
                        "env_mapping": "SHARED_PATH",
                        "volume_mount": True,
                        "command_arg": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        config = {"SHARED_PATH": "/shared/data"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should create volume mount
        assert "/shared/data" in result["template"]["volumes"]
        assert result["template"]["volumes"]["/shared/data"] == "/mnt/shared/data"

        # Should add container path to command (not host path)
        assert "/mnt/shared/data" in result["template"]["command"]

        # Should remove from config
        assert "SHARED_PATH" not in result["config"]

    def test_handle_host_container_volume_format(self, config_processor):
        """Test handling of host:container volume format."""
        template = {
            "config_schema": {
                "properties": {
                    "data_dir": {
                        "env_mapping": "DATA_DIR",
                        "volume_mount": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        config = {"DATA_DIR": "/host/data:/custom/container/path"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should use custom container path
        assert "/host/data" in result["template"]["volumes"]
        assert result["template"]["volumes"]["/host/data"] == "/custom/container/path"

    def test_handle_multiple_space_separated_paths(self, config_processor):
        """Test handling of multiple space-separated paths."""
        template = {
            "config_schema": {
                "properties": {
                    "data_dirs": {
                        "env_mapping": "DATA_DIRS",
                        "volume_mount": True,
                        "command_arg": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        config = {"DATA_DIRS": "/path1 /path2 /path3"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should create separate volume mounts for each path
        volumes = result["template"]["volumes"]
        assert "/path1" in volumes
        assert "/path2" in volumes
        assert "/path3" in volumes
        assert volumes["/path1"] == "/mnt/path1"
        assert volumes["/path2"] == "/mnt/path2"
        assert volumes["/path3"] == "/mnt/path3"

        # Should add container paths to command
        command = result["template"]["command"]
        assert "/mnt/path1" in command
        assert "/mnt/path2" in command
        assert "/mnt/path3" in command

    def test_preserve_existing_volumes_and_commands(self, config_processor):
        """Test that existing volumes and commands are preserved."""
        template = {
            "config_schema": {
                "properties": {
                    "new_path": {
                        "env_mapping": "NEW_PATH",
                        "volume_mount": True,
                        "command_arg": True,
                    }
                }
            },
            "volumes": {"/existing/volume": "/app/existing"},
            "command": ["--existing-arg"],
        }

        config = {"NEW_PATH": "/new/path"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should preserve existing volumes and add new ones
        volumes = result["template"]["volumes"]
        assert "/existing/volume" in volumes
        assert volumes["/existing/volume"] == "/app/existing"
        assert "/new/path" in volumes
        assert volumes["/new/path"] == "/mnt/new/path"

        # Should preserve existing commands and add container path for new one
        command = result["template"]["command"]
        assert "--existing-arg" in command
        assert "/mnt/new/path" in command

    def test_handle_none_volumes_and_commands(self, config_processor):
        """Test handling when template has None for volumes and command."""
        template = {
            "config_schema": {
                "properties": {
                    "data_dir": {
                        "env_mapping": "DATA_DIR",
                        "volume_mount": True,
                        "command_arg": True,
                    }
                }
            },
            "volumes": None,
            "command": None,
        }

        config = {"DATA_DIR": "/test/path"}

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should initialize volumes and command with container path
        assert result["template"]["volumes"] == {"/test/path": "/mnt/test/path"}
        assert result["template"]["command"] == ["/mnt/test/path"]

    def test_invalid_volume_mount_format_warning(self, config_processor):
        """Test warning for invalid volume mount format."""
        template = {
            "config_schema": {
                "properties": {
                    "data_dir": {
                        "env_mapping": "DATA_DIR",
                        "volume_mount": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        # Invalid format with too many colons
        config = {"DATA_DIR": "/host:/container:/extra"}

        with patch("mcp_platform.core.config_processor.logger") as mock_logger:
            result = config_processor.handle_volume_and_args_config_properties(
                template, config
            )

            # Should log warning
            mock_logger.warning.assert_called_once()

            # Should not create volume mount for invalid format
            assert not result["template"]["volumes"]

    def test_docker_artifact_removal(self, config_processor):
        """Test removal of Docker command artifacts from volume mount paths."""
        template = {
            "config_schema": {
                "properties": {
                    "data_dirs": {
                        "env_mapping": "DATA_DIRS",
                        "volume_mount": True,
                        "command_arg": True,
                    }
                }
            },
            "volumes": {},
            "command": [],
        }

        # Input with Docker command artifacts (like what user might accidentally paste)
        config = {
            "DATA_DIRS": "/path1:/container1 --volume /path2:/container2 --volume /path3"
        }

        result = config_processor.handle_volume_and_args_config_properties(
            template, config
        )

        # Should clean up artifacts and create proper volume mounts
        volumes = result["template"]["volumes"]
        assert "/path1" in volumes
        assert volumes["/path1"] == "/container1"
        assert "/path2" in volumes
        assert volumes["/path2"] == "/container2"
        assert "/path3" in volumes
        assert volumes["/path3"] == "/mnt/path3"

        # Should add container paths to command
        command = result["template"]["command"]
        assert "/container1" in command
        assert "/container2" in command
        assert "/mnt/path3" in command


@pytest.mark.unit
class TestConfigProcessorTypeConversion:
    """Test ConfigProcessor type conversion functionality."""

    def test_convert_boolean_values(self, config_processor):
        """Test conversion of boolean values."""
        template = {
            "config_schema": {
                "properties": {
                    "enable_feature": {
                        "type": "boolean",
                        "env_mapping": "ENABLE_FEATURE",
                    }
                }
            },
            "env_vars": {},
        }

        test_cases = [
            ("true", True),
            ("false", False),
            ("True", True),
            ("False", False),
            ("1", True),
            ("0", False),
        ]

        for input_value, expected in test_cases:
            config_values = {"enable_feature": input_value}
            result = config_processor._convert_config_values(config_values, template)
            assert result["ENABLE_FEATURE"] == expected

    def test_convert_integer_values(self, config_processor):
        """Test conversion of integer values."""
        template = {
            "config_schema": {
                "properties": {
                    "port": {
                        "type": "integer",
                        "env_mapping": "PORT",
                    }
                }
            },
            "env_vars": {},
        }

        config_values = {"port": "8080"}
        result = config_processor._convert_config_values(config_values, template)
        assert result["PORT"] == 8080

    def test_convert_number_values(self, config_processor):
        """Test conversion of number (float) values."""
        template = {
            "config_schema": {
                "properties": {
                    "timeout": {
                        "type": "number",
                        "env_mapping": "TIMEOUT",
                    }
                }
            },
            "env_vars": {},
        }

        config_values = {"timeout": "30.5"}
        result = config_processor._convert_config_values(config_values, template)
        assert abs(result["TIMEOUT"] - 30.5) < 0.001

    def test_convert_array_values(self, config_processor):
        """Test conversion of array values."""
        template = {
            "config_schema": {
                "properties": {
                    "tags": {
                        "type": "array",
                        "env_mapping": "TAGS",
                    }
                }
            },
            "env_vars": {},
        }

        # Test comma-separated string
        config_values = {"tags": "tag1,tag2,tag3"}
        result = config_processor._convert_config_values(config_values, template)
        assert result["TAGS"] == "tag1,tag2,tag3"

    def test_convert_string_values(self, config_processor):
        """Test conversion of string values (default case)."""
        template = {
            "config_schema": {
                "properties": {
                    "name": {
                        "type": "string",
                        "env_mapping": "NAME",
                    }
                }
            },
            "env_vars": {},
        }

        config_values = {"name": "test_value"}
        result = config_processor._convert_config_values(config_values, template)
        assert result["NAME"] == "test_value"

    def test_convert_values_type_error_fallback(self, config_processor):
        """Test fallback to string when type conversion fails."""
        template = {
            "config_schema": {
                "properties": {
                    "port": {
                        "type": "integer",
                        "env_mapping": "PORT",
                    }
                }
            },
            "env_vars": {},
        }

        # Invalid integer value
        config_values = {"port": "not_a_number"}

        with patch("mcp_platform.core.config_processor.logger") as mock_logger:
            result = config_processor._convert_config_values(config_values, template)

            # Should fall back to string and log warning
            assert result["PORT"] == "not_a_number"
            mock_logger.warning.assert_called_once()


@pytest.mark.unit
class TestConfigProcessorNestedConfiguration:
    """Test ConfigProcessor nested configuration handling."""

    def test_handle_nested_cli_config_two_parts(self, config_processor):
        """Test handling of double underscore notation with two parts."""
        properties = {
            "allowed_dirs": {"env_mapping": "ALLOWED_DIRS"},
            "log_level": {"env_mapping": "LOG_LEVEL"},
        }

        # Test category__property format
        result = config_processor._handle_nested_cli_config(
            "allowed__dirs", "value", properties
        )
        assert result == "allowed_dirs"

    def test_handle_nested_cli_config_three_parts(self, config_processor):
        """Test handling of double underscore notation with three parts."""
        properties = {
            "security_read_only_mode": {"env_mapping": "SECURITY_READ_ONLY_MODE"},
            "security_mode": {"env_mapping": "SECURITY_MODE"},
            "mode": {"env_mapping": "MODE"},
        }

        # Test category__subcategory__property format
        result = config_processor._handle_nested_cli_config(
            "security__read__only_mode", "value", properties
        )
        assert result == "security_read_only_mode"

    def test_handle_nested_cli_config_not_found(self, config_processor):
        """Test handling when nested config property is not found."""
        properties = {
            "existing_prop": {"env_mapping": "EXISTING_PROP"},
        }

        result = config_processor._handle_nested_cli_config(
            "nonexistent__prop", "value", properties
        )
        assert result is None

    def test_snake_to_camel_conversion(self, config_processor):
        """Test snake_case to camelCase conversion."""
        test_cases = [
            ("snake_case", "snakeCase"),
            ("single", "single"),
            ("multi_word_case", "multiWordCase"),
            ("a_b_c", "aBC"),
        ]

        for snake_str, expected_camel in test_cases:
            result = config_processor._snake_to_camel(snake_str)
            assert result == expected_camel

    def test_get_nested_value(self, config_processor):
        """Test getting nested values from dictionary."""
        data = {
            "level1": {"level2": {"level3": "nested_value"}},
            "simple": "simple_value",
        }

        # Test nested access
        result = config_processor._get_nested_value(data, "level1.level2.level3")
        assert result == "nested_value"

        # Test simple access
        result = config_processor._get_nested_value(data, "simple")
        assert result == "simple_value"

        # Test missing key
        with pytest.raises(KeyError):
            config_processor._get_nested_value(data, "nonexistent.key")


@pytest.mark.unit
class TestConfigProcessorFileMapping:
    """Test ConfigProcessor file mapping functionality."""

    def test_map_file_config_to_env_direct_mapping(self, config_processor):
        """Test direct property name mapping from file config."""
        template = {
            "config_schema": {
                "properties": {
                    "log_level": {"env_mapping": "LOG_LEVEL"},
                    "port": {"env_mapping": "PORT"},
                }
            }
        }

        file_config = {
            "log_level": "DEBUG",
            "port": 9000,
        }

        result = config_processor._map_file_config_to_env(file_config, template)

        assert result["LOG_LEVEL"] == "DEBUG"
        assert result["PORT"] == "9000"

    def test_map_file_config_to_env_with_file_mapping_hint(self, config_processor):
        """Test file mapping with explicit file_mapping hint."""
        template = {
            "config_schema": {
                "properties": {
                    "log_level": {
                        "env_mapping": "LOG_LEVEL",
                        "file_mapping": "logging.level",
                    }
                }
            }
        }

        file_config = {"logging": {"level": "WARNING"}}

        result = config_processor._map_file_config_to_env(file_config, template)

        assert result["LOG_LEVEL"] == "WARNING"

    def test_generate_common_patterns(self, config_processor):
        """Test generation of common nested patterns."""
        # Test specific property with predefined patterns
        patterns = config_processor._generate_common_patterns("log_level")
        assert "logging.level" in patterns
        assert "log.level" in patterns

        # Test generic property
        patterns = config_processor._generate_common_patterns("custom_prop")
        assert "config.custom_prop" in patterns
        assert "settings.custom_prop" in patterns
        assert "config.customProp" in patterns  # camelCase version

    def test_convert_value_to_env_string(self, config_processor):
        """Test conversion of values to environment variable string format."""
        prop_config = {}

        # Test list conversion
        result = config_processor._convert_value_to_env_string([1, 2, 3], prop_config)
        assert result == "1,2,3"

        # Test boolean conversion
        result = config_processor._convert_value_to_env_string(True, prop_config)
        assert result == "true"

        result = config_processor._convert_value_to_env_string(False, prop_config)
        assert result == "false"

        # Test string conversion
        result = config_processor._convert_value_to_env_string("test", prop_config)
        assert result == "test"

        # Test number conversion
        result = config_processor._convert_value_to_env_string(42, prop_config)
        assert result == "42"


@pytest.mark.unit
class TestConfigProcessorTemplateOverrides:
    """Test template override functionality in ConfigProcessor."""

    def test_simple_override(self, config_processor):
        """Test simple field override."""
        template_data = {"name": "test", "version": "1.0.0"}
        override_values = {"version": "2.0.0", "author": "Test User"}

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result["version"] == "2.0.0"
        assert result["author"] == "Test User"
        assert result["name"] == "test"  # unchanged

    def test_nested_override(self, config_processor):
        """Test nested field override with double underscores."""
        template_data = {
            "metadata": {"version": "1.0.0", "description": "Test"},
            "config": {"debug": False},
        }
        override_values = {
            "metadata__version": "2.0.0",
            "metadata__author": "Test User",
            "config__debug": "true",
            "config__port": "8080",
        }

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result["metadata"]["version"] == "2.0.0"
        assert result["metadata"]["author"] == "Test User"
        assert result["metadata"]["description"] == "Test"  # unchanged
        assert result["config"]["debug"] is True  # converted to boolean
        assert result["config"]["port"] == 8080  # converted to int

    def test_array_override(self, config_processor):
        """Test array element override."""
        template_data = {
            "tools": [
                {"name": "tool1", "enabled": True},
                {"name": "tool2", "enabled": True},
            ]
        }
        override_values = {
            "tools__0__enabled": "false",
            "tools__1__description": "Updated tool",
        }

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result["tools"][0]["enabled"] is False
        assert result["tools"][0]["name"] == "tool1"  # unchanged
        assert result["tools"][1]["description"] == "Updated tool"
        assert result["tools"][1]["name"] == "tool2"  # unchanged

    def test_deep_nested_override(self, config_processor):
        """Test deeply nested override."""
        template_data = {
            "config": {"database": {"connection": {"host": "localhost", "port": 5432}}}
        }
        override_values = {
            "config__database__connection__host": "remote.example.com",
            "config__database__connection__timeout": "30",
        }

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert (
            result["config"]["database"]["connection"]["host"] == "remote.example.com"
        )
        assert result["config"]["database"]["connection"]["port"] == 5432  # unchanged
        assert result["config"]["database"]["connection"]["timeout"] == 30  # new field

    def test_type_conversion(self, config_processor):
        """Test automatic type conversion of override values."""
        template_data = {"config": {}}
        override_values = {
            "config__debug": "true",
            "config__verbose": "false",
            "config__port": "8080",
            "config__timeout": "30.5",
            "config__name": "test-server",
            "config__tags": '["tag1", "tag2"]',
            "config__metadata": '{"key": "value"}',
        }

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result["config"]["debug"] is True
        assert result["config"]["verbose"] is False
        assert result["config"]["port"] == 8080
        assert result["config"]["timeout"] == 30.5
        assert result["config"]["name"] == "test-server"
        assert result["config"]["tags"] == ["tag1", "tag2"]
        assert result["config"]["metadata"] == {"key": "value"}

    def test_array_extension(self, config_processor):
        """Test that arrays are extended when accessing out-of-bounds indices."""
        template_data = {"tools": [{"name": "tool1"}]}
        override_values = {"tools__2__name": "tool3"}

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        # Array should be extended with empty objects
        assert len(result["tools"]) == 3
        assert result["tools"][0]["name"] == "tool1"
        assert result["tools"][1] == {}  # empty placeholder
        assert result["tools"][2]["name"] == "tool3"

    def test_invalid_array_index(self, config_processor):
        """Test handling of non-numeric array indices."""
        template_data = {"config": {"debug": True}}
        override_values = {"config__invalid__nested": "value"}

        # Should create nested dict structure
        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result["config"]["debug"] is True
        assert result["config"]["invalid"]["nested"] == "value"

    def test_empty_override_values(self, config_processor):
        """Test with empty override values."""
        template_data = {"name": "test"}
        override_values = {}

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result == template_data

    def test_none_override_values(self, config_processor):
        """Test with None override values."""
        template_data = {"name": "test"}

        result = config_processor._apply_template_overrides(template_data, None)

        assert result == template_data

    def test_override_creates_missing_structure(self, config_processor):
        """Test that override can create completely new nested structures."""
        template_data = {"existing": "value"}
        override_values = {
            "new__section__subsection__value": "new_value",
            "new__array__0__item": "first_item",
        }

        result = config_processor._apply_template_overrides(
            template_data, override_values
        )

        assert result["existing"] == "value"  # unchanged
        assert result["new"]["section"]["subsection"]["value"] == "new_value"
        assert result["new"]["array"][0]["item"] == "first_item"


@pytest.mark.unit
class TestConditionalConfigValidator:
    """Test suite for ConditionalConfigValidator class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = ConfigProcessor()

    def test_basic_validation_no_conditionals(self):
        """Test basic validation without conditional requirements."""
        config_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "port": {"type": "integer", "default": 8080},
            },
            "required": ["name"],
        }

        # Valid config
        config = {"name": "test-server", "port": 9000}
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is True
        assert result["missing_required"] == []
        assert result["conditional_issues"] == []
        assert result["suggestions"] == []

    def test_basic_validation_missing_required(self):
        """Test validation with missing required fields."""
        config_schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}, "host": {"type": "string"}},
            "required": ["name", "host"],
        }

        config = {"name": "test-server"}  # Missing host
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is False
        assert "host" in result["missing_required"]
        assert result["conditional_issues"] == []

    def test_anyof_validation_success(self):
        """Test anyOf validation with satisfied condition."""
        config_schema = {
            "type": "object",
            "properties": {
                "auth_mode": {"type": "string", "enum": ["none", "token", "basic"]},
                "auth_token": {"type": "string"},
                "auth_username": {"type": "string"},
                "auth_password": {"type": "string"},
            },
            "anyOf": [
                {"properties": {"auth_mode": {"const": "none"}}},
                {
                    "properties": {"auth_mode": {"const": "token"}},
                    "required": ["auth_token"],
                },
                {
                    "properties": {"auth_mode": {"const": "basic"}},
                    "required": ["auth_username", "auth_password"],
                },
            ],
        }

        # Test token auth (should succeed)
        config = {"auth_mode": "token", "auth_token": "secret123"}
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is True
        assert result["conditional_issues"] == []

    def test_anyof_validation_failure(self):
        """Test anyOf validation with no satisfied conditions."""
        config_schema = {
            "type": "object",
            "properties": {
                "auth_mode": {"type": "string", "enum": ["none", "token", "basic"]},
                "auth_token": {"type": "string"},
                "auth_username": {"type": "string"},
                "auth_password": {"type": "string"},
            },
            "anyOf": [
                {"properties": {"auth_mode": {"const": "none"}}},
                {
                    "properties": {"auth_mode": {"const": "token"}},
                    "required": ["auth_token"],
                },
                {
                    "properties": {"auth_mode": {"const": "basic"}},
                    "required": ["auth_username", "auth_password"],
                },
            ],
        }

        # Test token auth missing token (should fail)
        config = {"auth_mode": "token"}  # Missing auth_token
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is False
        assert len(result["conditional_issues"]) > 0
        assert len(result["suggestions"]) > 0

    def test_oneof_validation_success(self):
        """Test oneOf validation with exactly one satisfied condition."""
        config_schema = {
            "type": "object",
            "properties": {
                "storage_type": {"type": "string", "enum": ["local", "s3", "gcs"]},
                "local_path": {"type": "string"},
                "s3_bucket": {"type": "string"},
                "s3_key": {"type": "string"},
                "gcs_bucket": {"type": "string"},
                "gcs_key": {"type": "string"},
            },
            "oneOf": [
                {
                    "properties": {"storage_type": {"const": "local"}},
                    "required": ["local_path"],
                },
                {
                    "properties": {"storage_type": {"const": "s3"}},
                    "required": ["s3_bucket", "s3_key"],
                },
                {
                    "properties": {"storage_type": {"const": "gcs"}},
                    "required": ["gcs_bucket", "gcs_key"],
                },
            ],
        }

        # Test S3 storage (should succeed)
        config = {"storage_type": "s3", "s3_bucket": "my-bucket", "s3_key": "my-key"}
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is True
        assert result["conditional_issues"] == []

    def test_nested_oneof_in_anyof(self):
        """Test complex nested oneOf within anyOf structure."""
        config_schema = {
            "type": "object",
            "properties": {
                "engine_type": {
                    "type": "string",
                    "enum": ["elasticsearch", "opensearch"],
                },
                "elasticsearch_hosts": {"type": "string"},
                "elasticsearch_api_key": {"type": "string"},
                "elasticsearch_username": {"type": "string"},
                "elasticsearch_password": {"type": "string"},
                "opensearch_hosts": {"type": "string"},
                "opensearch_username": {"type": "string"},
                "opensearch_password": {"type": "string"},
            },
            "anyOf": [
                {
                    "properties": {"engine_type": {"const": "elasticsearch"}},
                    "required": ["elasticsearch_hosts"],
                    "oneOf": [
                        {"required": ["elasticsearch_api_key"]},
                        {
                            "required": [
                                "elasticsearch_username",
                                "elasticsearch_password",
                            ]
                        },
                    ],
                },
                {
                    "properties": {"engine_type": {"const": "opensearch"}},
                    "required": [
                        "opensearch_hosts",
                        "opensearch_username",
                        "opensearch_password",
                    ],
                },
            ],
        }

        # Test Elasticsearch with API key (should succeed)
        config = {
            "engine_type": "elasticsearch",
            "elasticsearch_hosts": "localhost:9200",
            "elasticsearch_api_key": "secret-key",
        }
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is True
        assert result["conditional_issues"] == []

    def test_nested_oneof_failure(self):
        """Test nested oneOf failure scenarios."""
        config_schema = {
            "type": "object",
            "properties": {
                "engine_type": {"type": "string"},
                "elasticsearch_hosts": {"type": "string"},
                "elasticsearch_api_key": {"type": "string"},
                "elasticsearch_username": {"type": "string"},
                "elasticsearch_password": {"type": "string"},
            },
            "anyOf": [
                {
                    "properties": {"engine_type": {"const": "elasticsearch"}},
                    "required": ["elasticsearch_hosts"],
                    "oneOf": [
                        {"required": ["elasticsearch_api_key"]},
                        {
                            "required": [
                                "elasticsearch_username",
                                "elasticsearch_password",
                            ]
                        },
                    ],
                }
            ],
        }

        # Test Elasticsearch with both API key and basic auth (should fail oneOf)
        config = {
            "engine_type": "elasticsearch",
            "elasticsearch_hosts": "localhost:9200",
            "elasticsearch_api_key": "secret-key",
            "elasticsearch_username": "user",
            "elasticsearch_password": "pass",
        }
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is False
        assert len(result["conditional_issues"]) > 0

    def test_suggestions_generation(self):
        """Test that helpful suggestions are generated for failures."""
        config_schema = {
            "type": "object",
            "properties": {
                "auth_mode": {
                    "type": "string",
                    "title": "Authentication Mode",
                    "enum": ["none", "token", "basic"],
                },
                "auth_token": {"type": "string", "title": "Authentication Token"},
                "auth_username": {"type": "string", "title": "Username"},
                "auth_password": {"type": "string", "title": "Password"},
            },
            "anyOf": [
                {"properties": {"auth_mode": {"const": "none"}}},
                {
                    "properties": {"auth_mode": {"const": "token"}},
                    "required": ["auth_token"],
                },
                {
                    "properties": {"auth_mode": {"const": "basic"}},
                    "required": ["auth_username", "auth_password"],
                },
            ],
        }

        config = {"auth_mode": "token"}  # Missing auth_token
        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is False
        assert len(result["suggestions"]) > 0

        # Check that suggestions mention the options
        suggestions_text = " ".join(result["suggestions"])
        assert "Authentication Mode" in suggestions_text

    def test_is_conditionally_required(self):
        """Test the is_conditionally_required method."""
        config_schema = {
            "type": "object",
            "properties": {
                "auth_mode": {"type": "string"},
                "auth_token": {"type": "string"},
                "auth_username": {"type": "string"},
                "auth_password": {"type": "string"},
            },
            "anyOf": [
                {"properties": {"auth_mode": {"const": "none"}}},
                {
                    "properties": {"auth_mode": {"const": "token"}},
                    "required": ["auth_token"],
                },
                {
                    "properties": {"auth_mode": {"const": "basic"}},
                    "required": ["auth_username", "auth_password"],
                },
            ],
        }

        # Test with token mode - auth_token should be conditionally required
        config = {"auth_mode": "token"}
        assert (
            self.validator.is_conditionally_required(
                "auth_token", config_schema, config
            )
            is True
        )
        assert (
            self.validator.is_conditionally_required(
                "auth_username", config_schema, config
            )
            is False
        )

        # Test with basic mode - username and password should be conditionally required
        config = {"auth_mode": "basic"}
        assert (
            self.validator.is_conditionally_required(
                "auth_username", config_schema, config
            )
            is True
        )
        assert (
            self.validator.is_conditionally_required(
                "auth_password", config_schema, config
            )
            is True
        )
        assert (
            self.validator.is_conditionally_required(
                "auth_token", config_schema, config
            )
            is False
        )

        # Test with none mode - nothing should be conditionally required
        config = {"auth_mode": "none"}
        assert (
            self.validator.is_conditionally_required(
                "auth_token", config_schema, config
            )
            is False
        )
        assert (
            self.validator.is_conditionally_required(
                "auth_username", config_schema, config
            )
            is False
        )

    def test_complex_real_world_schema(self):
        """Test with a complex real-world schema similar to open-elastic-search template."""
        config_schema = {
            "type": "object",
            "properties": {
                "engine_type": {
                    "type": "string",
                    "enum": ["elasticsearch", "opensearch"],
                    "default": "elasticsearch",
                },
                "elasticsearch_hosts": {"type": "string"},
                "elasticsearch_api_key": {"type": "string"},
                "elasticsearch_username": {"type": "string"},
                "elasticsearch_password": {"type": "string"},
                "opensearch_hosts": {"type": "string"},
                "opensearch_username": {"type": "string"},
                "opensearch_password": {"type": "string"},
            },
            "anyOf": [
                {
                    "properties": {"engine_type": {"const": "elasticsearch"}},
                    "required": ["elasticsearch_hosts"],
                    "oneOf": [
                        {"required": ["elasticsearch_api_key"]},
                        {
                            "required": [
                                "elasticsearch_username",
                                "elasticsearch_password",
                            ]
                        },
                    ],
                },
                {
                    "properties": {"engine_type": {"const": "opensearch"}},
                    "required": [
                        "opensearch_hosts",
                        "opensearch_username",
                        "opensearch_password",
                    ],
                },
            ],
        }

        # Test valid Elasticsearch config with basic auth
        config = {
            "engine_type": "elasticsearch",
            "elasticsearch_hosts": "localhost:9200",
            "elasticsearch_username": "elastic",
            "elasticsearch_password": "password",
        }
        result = self.validator.validate_config_schema(config_schema, config)
        assert result["valid"] is True

        # Test valid OpenSearch config
        config = {
            "engine_type": "opensearch",
            "opensearch_hosts": "localhost:9200",
            "opensearch_username": "admin",
            "opensearch_password": "admin",
        }
        result = self.validator.validate_config_schema(config_schema, config)
        assert result["valid"] is True

        # Test invalid - Elasticsearch without auth
        config = {
            "engine_type": "elasticsearch",
            "elasticsearch_hosts": "localhost:9200",
        }
        result = self.validator.validate_config_schema(config_schema, config)
        assert result["valid"] is False

    def test_empty_config_schema(self):
        """Test validation with empty config schema."""
        config_schema = {}
        config = {"some_field": "some_value"}

        result = self.validator.validate_config_schema(config_schema, config)

        assert result["valid"] is True
        assert result["missing_required"] == []
        assert result["conditional_issues"] == []
        assert result["suggestions"] == []

    def test_empty_config(self):
        """Test validation with empty config against schema with defaults."""
        config_schema = {
            "type": "object",
            "properties": {
                "auth_mode": {"type": "string", "default": "none"},
                "port": {"type": "integer", "default": 8080},
            },
        }

        config = {}
        result = self.validator.validate_config_schema(config_schema, config)

        # Should be valid because there are no required fields or conditional constraints
        assert result["valid"] is True


@pytest.mark.unit
class TestInlineAnyOfConditionalRules:
    """Tests for newly added inline anyOf rule-list conditional enforcement.

    These tests cover schemas that express multiple independent if/then rules
    inside an anyOf array (rule-list mode). Each rule is evaluated and if its
    'if' condition matches, its 'then.required' fields must be present. This
    differs from classic JSON Schema anyOf semantics (choice/alternatives) and
    reflects the extended behavior implemented in ConfigProcessor.
    """

    def test_inline_anyof_password_missing(self):
        schema = {
            "type": "object",
            "properties": {
                "auth_method": {"type": "string", "enum": ["password", "cert"]},
                "password": {"type": "string"},
                "cert_path": {"type": "string"},
                "key_path": {"type": "string"},
            },
            "required": ["auth_method"],
            "anyOf": [
                {
                    "if": {"properties": {"auth_method": {"const": "password"}}},
                    "then": {"required": ["password"]},
                },
                {
                    "if": {"properties": {"auth_method": {"const": "cert"}}},
                    "then": {"required": ["cert_path", "key_path"]},
                },
            ],
        }

        config = {"auth_method": "password"}  # missing password
        res = ConfigProcessor.validate_config_schema(schema, config)
        assert res["valid"] is False
        # Ensure password flagged as missing either directly or via conditional issue
        assert "password" in res.get("missing_required", []) or any(
            "password" in (issue.get("missing") or [])
            for issue in res.get("conditional_issues", [])
        )

    def test_inline_anyof_password_present(self):
        schema = {
            "type": "object",
            "properties": {
                "auth_method": {"type": "string", "enum": ["password", "cert"]},
                "password": {"type": "string"},
                "cert_path": {"type": "string"},
                "key_path": {"type": "string"},
            },
            "required": ["auth_method"],
            "anyOf": [
                {
                    "if": {"properties": {"auth_method": {"const": "password"}}},
                    "then": {"required": ["password"]},
                },
                {
                    "if": {"properties": {"auth_method": {"const": "cert"}}},
                    "then": {"required": ["cert_path", "key_path"]},
                },
            ],
        }
        config = {"auth_method": "password", "password": "secret"}
        res = ConfigProcessor.validate_config_schema(schema, config)
        assert res["valid"] is True

    def test_inline_anyof_cert_missing_key(self):
        schema = {
            "type": "object",
            "properties": {
                "auth_method": {"type": "string", "enum": ["password", "cert"]},
                "password": {"type": "string"},
                "cert_path": {"type": "string"},
                "key_path": {"type": "string"},
            },
            "required": ["auth_method"],
            "anyOf": [
                {
                    "if": {"properties": {"auth_method": {"const": "password"}}},
                    "then": {"required": ["password"]},
                },
                {
                    "if": {"properties": {"auth_method": {"const": "cert"}}},
                    "then": {"required": ["cert_path", "key_path"]},
                },
            ],
        }
        config = {"auth_method": "cert", "cert_path": "/p/cert.crt"}  # missing key_path
        res = ConfigProcessor.validate_config_schema(schema, config)
        assert res["valid"] is False
        assert "key_path" in res.get("missing_required", []) or any(
            "key_path" in (issue.get("missing") or [])
            for issue in res.get("conditional_issues", [])
        )


@pytest.mark.unit
class TestAllOfConditionalRules:
    """Tests for allOf support with independent conditional blocks."""

    def test_allof_multiple_conditions(self):
        schema = {
            "type": "object",
            "properties": {
                "feature_a_enabled": {"type": "boolean"},
                "feature_b_enabled": {"type": "boolean"},
                "a_api_key": {"type": "string"},
                "b_api_key": {"type": "string"},
            },
            "allOf": [
                {
                    "if": {"properties": {"feature_a_enabled": {"const": True}}},
                    "then": {"required": ["a_api_key"]},
                },
                {
                    "if": {"properties": {"feature_b_enabled": {"const": True}}},
                    "then": {"required": ["b_api_key"]},
                },
            ],
        }

        # Enable both but supply only one key -> invalid
        config = {
            "feature_a_enabled": True,
            "feature_b_enabled": True,
            "a_api_key": "AAA",
        }
        res = ConfigProcessor.validate_config_schema(schema, config)
        assert res["valid"] is False
        assert "b_api_key" in res.get("missing_required", []) or any(
            "b_api_key" in (issue.get("missing") or [])
            for issue in res.get("conditional_issues", [])
        )

        # Provide both keys -> valid
        config["b_api_key"] = "BBB"
        res2 = ConfigProcessor.validate_config_schema(schema, config)
        assert res2["valid"] is True

        # Enable none -> valid (no keys required)
        res3 = ConfigProcessor.validate_config_schema(schema, {})
        assert res3["valid"] is True


@pytest.mark.unit
class TestTemplateNormalizationInlineRules:
    """Integration-style tests for template-normalization path with inline anyOf rules (postgres-like)."""

    def test_postgres_like_missing_password(self):
        template = {
            "config_schema": {
                "type": "object",
                "properties": {
                    "pg_host": {"type": "string", "env_mapping": "PG_HOST"},
                    "pg_user": {"type": "string", "env_mapping": "PG_USER"},
                    "pg_password": {"type": "string", "env_mapping": "PG_PASSWORD"},
                    "auth_method": {
                        "type": "string",
                        "enum": ["password", "cert"],
                        "env_mapping": "PG_AUTH_METHOD",
                    },
                    "pg_ssl_cert": {"type": "string", "env_mapping": "PG_SSL_CERT"},
                    "pg_ssl_key": {"type": "string", "env_mapping": "PG_SSL_KEY"},
                },
                "required": ["pg_host", "pg_user"],
                "anyOf": [
                    {
                        "if": {"properties": {"auth_method": {"const": "password"}}},
                        "then": {"required": ["pg_password"]},
                    },
                    {
                        "if": {"properties": {"auth_method": {"const": "cert"}}},
                        "then": {"required": ["pg_ssl_cert", "pg_ssl_key"]},
                    },
                ],
            }
        }

        # Provide env vars missing password while auth_method=password
        env_vars = {
            "PG_HOST": "localhost",
            "PG_USER": "analyst",
            "PG_AUTH_METHOD": "password",
        }
        result = ConfigProcessor().validate_config(template, env_vars=env_vars)
        assert result.valid is False
        assert "pg_password" in result.missing_required or any(
            "pg_password" in (issue.get("missing") or [])
            for issue in result.conditional_issues
        )

    def test_postgres_like_password_present(self):
        template = {
            "config_schema": {
                "type": "object",
                "properties": {
                    "pg_host": {"type": "string", "env_mapping": "PG_HOST"},
                    "pg_user": {"type": "string", "env_mapping": "PG_USER"},
                    "pg_password": {"type": "string", "env_mapping": "PG_PASSWORD"},
                    "auth_method": {
                        "type": "string",
                        "enum": ["password", "cert"],
                        "env_mapping": "PG_AUTH_METHOD",
                    },
                    "pg_ssl_cert": {"type": "string", "env_mapping": "PG_SSL_CERT"},
                    "pg_ssl_key": {"type": "string", "env_mapping": "PG_SSL_KEY"},
                },
                "required": ["pg_host", "pg_user"],
                "anyOf": [
                    {
                        "if": {"properties": {"auth_method": {"const": "password"}}},
                        "then": {"required": ["pg_password"]},
                    },
                    {
                        "if": {"properties": {"auth_method": {"const": "cert"}}},
                        "then": {"required": ["pg_ssl_cert", "pg_ssl_key"]},
                    },
                ],
            }
        }

        env_vars = {
            "PG_HOST": "localhost",
            "PG_USER": "analyst",
            "PG_AUTH_METHOD": "password",
            "PG_PASSWORD": "secret",
        }
        result = ConfigProcessor().validate_config(template, env_vars=env_vars)
        assert result.valid is True
