"""
Test custom templates functionality.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from mcp_platform.template.utils.discovery import TemplateDiscovery
from mcp_platform.utils import get_all_template_directories, get_custom_templates_dir


@pytest.mark.unit
class TestCustomTemplates:
    """Test custom templates functionality."""

    def test_get_custom_templates_dir_no_env(self):
        """Test get_custom_templates_dir when no environment variable is set."""
        with patch.dict(os.environ, {}, clear=True):
            custom_dir = get_custom_templates_dir()
            assert custom_dir is None

    def test_get_custom_templates_dir_with_env(self):
        """Test get_custom_templates_dir with environment variable set."""
        test_path = "/tmp/custom_templates"
        with patch.dict(os.environ, {"MCP_CUSTOM_TEMPLATES_DIR": test_path}):
            custom_dir = get_custom_templates_dir()
            assert custom_dir == Path(test_path).resolve()

    def test_get_custom_templates_dir_with_tilde(self):
        """Test get_custom_templates_dir with tilde expansion."""
        test_path = "~/custom_templates"
        with patch.dict(os.environ, {"MCP_CUSTOM_TEMPLATES_DIR": test_path}):
            custom_dir = get_custom_templates_dir()
            assert custom_dir == Path(test_path).expanduser().resolve()

    def test_get_all_template_directories_no_custom(self, tmp_path):
        """Test get_all_template_directories with no custom directory."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("mcp_platform.utils.TEMPLATES_DIR", tmp_path / "templates"):
                # Create the templates dir
                (tmp_path / "templates").mkdir()

                dirs = get_all_template_directories()
                assert len(dirs) == 1
                assert dirs[0] == tmp_path / "templates"

    def test_get_all_template_directories_with_custom(self, tmp_path):
        """Test get_all_template_directories with custom directory."""
        custom_dir = tmp_path / "custom"
        built_in_dir = tmp_path / "templates"

        # Create both directories
        custom_dir.mkdir()
        built_in_dir.mkdir()

        with patch.dict(os.environ, {"MCP_CUSTOM_TEMPLATES_DIR": str(custom_dir)}):
            with patch("mcp_platform.utils.TEMPLATES_DIR", built_in_dir):
                dirs = get_all_template_directories()
                assert len(dirs) == 2
                assert dirs[0] == custom_dir  # Custom first for override precedence
                assert dirs[1] == built_in_dir

    def test_get_all_template_directories_custom_not_exists(self, tmp_path):
        """Test get_all_template_directories when custom directory doesn't exist."""
        custom_dir = tmp_path / "nonexistent"
        built_in_dir = tmp_path / "templates"
        built_in_dir.mkdir()

        with patch.dict(os.environ, {"MCP_CUSTOM_TEMPLATES_DIR": str(custom_dir)}):
            with patch("mcp_platform.utils.TEMPLATES_DIR", built_in_dir):
                dirs = get_all_template_directories()
                assert len(dirs) == 1
                assert dirs[0] == built_in_dir


@pytest.mark.unit
class TestTemplateDiscoveryCustom:
    """Test TemplateDiscovery with custom templates."""

    def test_init_default_uses_all_directories(self, tmp_path):
        """Test that TemplateDiscovery by default uses all template directories."""
        custom_dir = tmp_path / "custom"
        built_in_dir = tmp_path / "templates"

        custom_dir.mkdir()
        built_in_dir.mkdir()

        with patch.dict(os.environ, {"MCP_CUSTOM_TEMPLATES_DIR": str(custom_dir)}):
            with patch("mcp_platform.utils.TEMPLATES_DIR", built_in_dir):
                with patch(
                    "mcp_platform.template.utils.discovery.get_all_template_directories"
                ) as mock_get_dirs:
                    mock_get_dirs.return_value = [custom_dir, built_in_dir]

                    discovery = TemplateDiscovery()
                    assert discovery.templates_dirs == [custom_dir, built_in_dir]
                    assert (
                        discovery.templates_dir == custom_dir
                    )  # First for backward compatibility

    def test_init_single_directory_backward_compatibility(self, tmp_path):
        """Test that TemplateDiscovery works with single directory (backward compatibility)."""
        test_dir = tmp_path / "templates"
        test_dir.mkdir()

        discovery = TemplateDiscovery(templates_dir=test_dir)
        assert discovery.templates_dirs == [test_dir]
        assert discovery.templates_dir == test_dir

    def test_init_multiple_directories(self, tmp_path):
        """Test that TemplateDiscovery works with multiple directories."""
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        discovery = TemplateDiscovery(templates_dirs=[dir1, dir2])
        assert discovery.templates_dirs == [dir1, dir2]
        assert discovery.templates_dir == dir1

    def test_discover_templates_from_multiple_directories(self, tmp_path):
        """Test discovering templates from multiple directories."""
        # Create custom templates directory
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()

        # Create built-in templates directory
        builtin_dir = tmp_path / "builtin"
        builtin_dir.mkdir()

        # Create a template in custom directory
        custom_template_dir = custom_dir / "my-custom-template"
        custom_template_dir.mkdir()

        custom_template_json = custom_template_dir / "template.json"
        custom_template_json.write_text(
            json.dumps(
                {
                    "name": "My Custom Template",
                    "description": "A custom template",
                    "version": "1.0.0",
                    "docker_image": "custom/template:latest",
                }
            )
        )

        # Create a template in builtin directory
        builtin_template_dir = builtin_dir / "builtin-template"
        builtin_template_dir.mkdir()

        builtin_template_json = builtin_template_dir / "template.json"
        builtin_template_json.write_text(
            json.dumps(
                {
                    "name": "Builtin Template",
                    "description": "A builtin template",
                    "version": "1.0.0",
                    "docker_image": "builtin/template:latest",
                }
            )
        )

        # Test discovery
        discovery = TemplateDiscovery(templates_dirs=[custom_dir, builtin_dir])
        templates = discovery.discover_templates()

        assert "my-custom-template" in templates
        assert "builtin-template" in templates
        assert templates["my-custom-template"]["name"] == "My Custom Template"
        assert templates["builtin-template"]["name"] == "Builtin Template"
        assert templates["my-custom-template"]["source_directory"] == str(custom_dir)
        assert templates["builtin-template"]["source_directory"] == str(builtin_dir)

    def test_template_override_behavior(self, tmp_path):
        """Test that custom templates override builtin templates with same name."""
        # Create custom templates directory
        custom_dir = tmp_path / "custom"
        custom_dir.mkdir()

        # Create built-in templates directory
        builtin_dir = tmp_path / "builtin"
        builtin_dir.mkdir()

        # Create a template with same name in both directories
        template_name = "demo"

        # Custom template
        custom_template_dir = custom_dir / template_name
        custom_template_dir.mkdir()
        custom_template_json = custom_template_dir / "template.json"
        custom_template_json.write_text(
            json.dumps(
                {
                    "name": "Custom Demo Template",
                    "description": "A custom demo template",
                    "version": "2.0.0",
                    "docker_image": "custom/demo:latest",
                }
            )
        )

        # Builtin template
        builtin_template_dir = builtin_dir / template_name
        builtin_template_dir.mkdir()
        builtin_template_json = builtin_template_dir / "template.json"
        builtin_template_json.write_text(
            json.dumps(
                {
                    "name": "Builtin Demo Template",
                    "description": "A builtin demo template",
                    "version": "1.0.0",
                    "docker_image": "builtin/demo:latest",
                }
            )
        )

        # Test discovery - custom should override builtin
        discovery = TemplateDiscovery(templates_dirs=[custom_dir, builtin_dir])
        templates = discovery.discover_templates()

        assert template_name in templates
        # Should be the custom version (custom dir processed in reverse order)
        assert templates[template_name]["name"] == "Custom Demo Template"
        assert templates[template_name]["version"] == "2.0.0"
        assert templates[template_name]["source_directory"] == str(custom_dir)

    def test_get_template_config_searches_all_directories(self, tmp_path):
        """Test that get_template_config searches all directories."""
        # Create directories
        custom_dir = tmp_path / "custom"
        builtin_dir = tmp_path / "builtin"
        custom_dir.mkdir()
        builtin_dir.mkdir()

        # Create template only in builtin directory
        template_dir = builtin_dir / "test-template"
        template_dir.mkdir()
        template_json = template_dir / "template.json"
        template_json.write_text(
            json.dumps(
                {
                    "name": "Test Template",
                    "description": "A test template",
                    "version": "1.0.0",
                    "docker_image": "test/template:latest",
                }
            )
        )

        discovery = TemplateDiscovery(templates_dirs=[custom_dir, builtin_dir])
        config = discovery.get_template_config("test-template")

        assert config is not None
        assert config["name"] == "Test Template"

    def test_get_template_config_prefers_custom(self, tmp_path):
        """Test that get_template_config prefers custom over builtin."""
        # Create directories
        custom_dir = tmp_path / "custom"
        builtin_dir = tmp_path / "builtin"
        custom_dir.mkdir()
        builtin_dir.mkdir()

        template_name = "test-template"

        # Create template in both directories
        for i, (dir_path, version) in enumerate(
            [(custom_dir, "2.0.0"), (builtin_dir, "1.0.0")]
        ):
            template_dir = dir_path / template_name
            template_dir.mkdir()
            template_json = template_dir / "template.json"
            template_json.write_text(
                json.dumps(
                    {
                        "name": f"Test Template {version}",
                        "description": "A test template",
                        "version": version,
                        "docker_image": f"test/template:{version}",
                    }
                )
            )

        discovery = TemplateDiscovery(templates_dirs=[custom_dir, builtin_dir])
        config = discovery.get_template_config(template_name)

        assert config is not None
        # Should get the custom version (first in search order)
        assert config["version"] == "2.0.0"

    def test_get_template_path_searches_all_directories(self, tmp_path):
        """Test that get_template_path searches all directories."""
        # Create directories
        custom_dir = tmp_path / "custom"
        builtin_dir = tmp_path / "builtin"
        custom_dir.mkdir()
        builtin_dir.mkdir()

        # Create template only in builtin directory
        template_dir = builtin_dir / "test-template"
        template_dir.mkdir()

        discovery = TemplateDiscovery(templates_dirs=[custom_dir, builtin_dir])
        path = discovery.get_template_path("test-template")

        assert path is not None
        assert path == template_dir

    def test_get_template_path_prefers_custom(self, tmp_path):
        """Test that get_template_path prefers custom over builtin."""
        # Create directories
        custom_dir = tmp_path / "custom"
        builtin_dir = tmp_path / "builtin"
        custom_dir.mkdir()
        builtin_dir.mkdir()

        template_name = "test-template"

        # Create template directories in both locations
        custom_template_dir = custom_dir / template_name
        builtin_template_dir = builtin_dir / template_name
        custom_template_dir.mkdir()
        builtin_template_dir.mkdir()

        discovery = TemplateDiscovery(templates_dirs=[custom_dir, builtin_dir])
        path = discovery.get_template_path(template_name)

        assert path is not None
        # Should get the custom path (first in search order)
        assert path == custom_template_dir
