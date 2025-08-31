#!/usr/bin/env python3
"""
Simple BigQuery MCP Template Tests.

Simplified tests focusing on template validation, tool categorization,
and basic functionality without complex mocking.
"""

import json
import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


class TestBigQueryTemplateValidation:
    """Test BigQuery template validation and basic structure."""

    def test_template_json_structure(self):
        """Test that template.json has required structure and valid JSON."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        assert os.path.exists(template_path), "template.json file must exist"

        with open(template_path, "r") as f:
            template_data = json.load(f)

        # Test required fields
        required_fields = ["id", "name", "description", "version"]
        for field in required_fields:
            assert (
                field in template_data
            ), f"Required field '{field}' missing from template.json"

        # Test specific BigQuery template fields
        assert template_data["id"] == "bigquery"
        assert "BigQuery" in template_data["name"]
        assert "config_schema" in template_data
        assert "tools" in template_data
        assert "capabilities" in template_data

    def test_config_schema_validation(self):
        """Test configuration schema structure."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        config_schema = template_data["config_schema"]
        properties = config_schema["properties"]

        # Test required configuration fields
        assert "project_id" in properties
        assert properties["project_id"]["type"] == "string"
        assert "env_mapping" in properties["project_id"]

        # Test authentication configuration
        assert "auth_method" in properties
        assert "enum" in properties["auth_method"]
        auth_methods = properties["auth_method"]["enum"]
        assert "service_account" in auth_methods
        assert "oauth2" in auth_methods
        assert "application_default" in auth_methods

        # Test security configuration
        assert "read_only" in properties
        assert properties["read_only"]["type"] == "boolean"
        assert properties["read_only"]["default"] is True

        # Test access control
        assert "allowed_datasets" in properties
        assert "dataset_regex" in properties

    def test_tool_definitions(self):
        """Test tool definitions in template."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        tools = template_data["tools"]
        tool_names = [tool["name"] for tool in tools]

        # Test expected tools are present
        expected_tools = [
            "list_datasets",
            "list_tables",
            "describe_table",
            "execute_query",
            "get_job_status",
            "get_dataset_info",
        ]

        for expected_tool in expected_tools:
            assert (
                expected_tool in tool_names
            ), f"Expected tool '{expected_tool}' not found"

        # Test tool structure
        for tool in tools:
            assert "name" in tool
            assert "description" in tool
            assert "parameters" in tool

            # Test parameters structure
            for param in tool["parameters"]:
                assert "name" in param
                assert "description" in param
                assert "type" in param
                assert "required" in param

    def test_capabilities_definition(self):
        """Test capabilities are properly defined."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        capabilities = template_data["capabilities"]

        # Test expected capabilities
        capability_names = [cap["name"] for cap in capabilities]
        expected_capabilities = [
            "Dataset Discovery",
            "Schema Inspection",
            "Query Execution",
            "Access Control",
        ]

        for expected_cap in expected_capabilities:
            assert (
                expected_cap in capability_names
            ), f"Expected capability '{expected_cap}' not found"

        # Test capability structure
        for capability in capabilities:
            assert "name" in capability
            assert "description" in capability
            assert "example" in capability

    def test_docker_configuration(self):
        """Test Docker configuration is present."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        assert "docker_image" in template_data
        assert "docker_tag" in template_data
        assert "ports" in template_data
        assert "transport" in template_data

        # Test transport configuration
        transport = template_data["transport"]
        assert "default" in transport
        assert "supported" in transport
        assert "port" in transport
        assert "http" in transport["supported"]
        assert "stdio" in transport["supported"]

    def test_environment_variable_mapping(self):
        """Test environment variable mappings are consistent."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        properties = template_data["config_schema"]["properties"]

        # Test environment variable mappings
        expected_env_mappings = {
            "project_id": "GOOGLE_CLOUD_PROJECT",
            "auth_method": "BIGQUERY_AUTH_METHOD",
            "service_account_path": "GOOGLE_APPLICATION_CREDENTIALS",
            "read_only": "BIGQUERY_READ_ONLY",
            "allowed_datasets": "BIGQUERY_ALLOWED_DATASETS",
            "dataset_regex": "BIGQUERY_DATASET_REGEX",
            "query_timeout": "BIGQUERY_QUERY_TIMEOUT",
            "max_results": "BIGQUERY_MAX_RESULTS",
            "log_level": "MCP_LOG_LEVEL",
        }

        for config_key, expected_env in expected_env_mappings.items():
            if config_key in properties and "env_mapping" in properties[config_key]:
                actual_env = properties[config_key]["env_mapping"]
                assert (
                    actual_env == expected_env
                ), f"Environment mapping for '{config_key}' should be '{expected_env}', got '{actual_env}'"

    def test_security_defaults(self):
        """Test security-related defaults are safe."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        properties = template_data["config_schema"]["properties"]

        # Test read-only mode is default
        assert properties["read_only"]["default"] is True

        # Test default dataset access is wildcard (but controlled by read-only)
        assert properties["allowed_datasets"]["default"] == "*"

        # Test reasonable query limits
        assert properties["query_timeout"]["default"] == 300  # 5 minutes
        assert properties["max_results"]["default"] == 1000
        assert properties["query_timeout"]["maximum"] == 3600  # 1 hour max
        assert properties["max_results"]["maximum"] == 10000

    def test_required_files_exist(self):
        """Test that all required template files exist."""
        template_dir = os.path.dirname(os.path.dirname(__file__))

        required_files = [
            "template.json",
            "server.py",
            "config.py",
            "requirements.txt",
            "README.md",
            "Dockerfile",
        ]

        for required_file in required_files:
            file_path = os.path.join(template_dir, required_file)
            assert os.path.exists(
                file_path
            ), f"Required file '{required_file}' is missing"

    def test_readme_completeness(self):
        """Test README.md has essential sections."""
        readme_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "README.md"
        )

        with open(readme_path, "r") as f:
            readme_content = f.read()

        # Test essential sections are present
        essential_sections = [
            "# BigQuery MCP Server",
            "## Features",
            "## Quick Start",
            "## Configuration",
            "## Authentication",
            "## API Reference",
            "## Security",
            "## Docker",
        ]

        for section in essential_sections:
            assert (
                section in readme_content
            ), f"README missing essential section: {section}"

        # Test security warnings are present
        assert "WARNING" in readme_content.upper()
        assert "read-only" in readme_content.lower()

    def test_dockerfile_best_practices(self):
        """Test Dockerfile follows best practices."""
        dockerfile_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "Dockerfile"
        )

        with open(dockerfile_path, "r") as f:
            dockerfile_content = f.read()

        # Test security best practices
        assert "USER mcpuser" in dockerfile_content  # Non-root user
        assert "HEALTHCHECK" in dockerfile_content  # Health check
        assert "EXPOSE 7072" in dockerfile_content  # Port exposure

        # Test build optimization
        assert "COPY requirements.txt" in dockerfile_content  # Copy requirements first
        assert "pip install" in dockerfile_content  # Install dependencies

        # Test labels for metadata
        assert "LABEL" in dockerfile_content

    def test_requirements_includes_bigquery(self):
        """Test requirements.txt includes BigQuery dependencies."""
        requirements_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "requirements.txt"
        )

        with open(requirements_path, "r") as f:
            requirements_content = f.read()

        # Test essential dependencies
        essential_deps = [
            "fastmcp",
            "google-cloud-bigquery",
            "google-auth",
            "google-oauth2",
            "requests",
        ]

        for dep in essential_deps:
            assert (
                dep in requirements_content.lower()
            ), f"Missing essential dependency: {dep}"


class TestBigQueryToolCategorization:
    """Test BigQuery tool categorization and functionality expectations."""

    def test_read_only_vs_write_tools(self):
        """Test categorization of read-only vs write tools."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        tools = template_data["tools"]
        tool_names = [tool["name"] for tool in tools]

        # All tools should be read-only safe by default
        read_only_tools = [
            "list_datasets",
            "list_tables",
            "describe_table",
            "get_job_status",
            "get_dataset_info",
        ]

        for tool in read_only_tools:
            assert tool in tool_names, f"Read-only tool '{tool}' should be available"

        # execute_query is conditional based on query content
        assert "execute_query" in tool_names

    def test_tool_parameter_requirements(self):
        """Test tool parameter requirements are properly defined."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        tools = template_data["tools"]

        # Test specific tool requirements
        tool_requirements = {
            "list_datasets": 0,  # No parameters
            "list_tables": 1,  # dataset_id required
            "describe_table": 2,  # dataset_id and table_id required
            "execute_query": 1,  # query required (dry_run optional)
            "get_job_status": 1,  # job_id required
            "get_dataset_info": 1,  # dataset_id required
        }

        for tool in tools:
            tool_name = tool["name"]
            if tool_name in tool_requirements:
                expected_required = tool_requirements[tool_name]
                actual_required = sum(1 for p in tool["parameters"] if p["required"])

                assert (
                    actual_required >= expected_required
                ), f"Tool '{tool_name}' should have at least {expected_required} required parameters, has {actual_required}"

    def test_query_tool_safety(self):
        """Test that query execution tool has proper safety parameters."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        tools = template_data["tools"]

        # Find execute_query tool
        execute_query_tool = None
        for tool in tools:
            if tool["name"] == "execute_query":
                execute_query_tool = tool
                break

        assert execute_query_tool is not None, "execute_query tool must be defined"

        # Test it has query parameter
        param_names = [p["name"] for p in execute_query_tool["parameters"]]
        assert "query" in param_names, "execute_query must have query parameter"

        # Test it has dry_run option for safety
        assert (
            "dry_run" in param_names
        ), "execute_query should have dry_run parameter for safety"

        # Find dry_run parameter
        dry_run_param = None
        for param in execute_query_tool["parameters"]:
            if param["name"] == "dry_run":
                dry_run_param = param
                break

        assert dry_run_param is not None
        assert dry_run_param["type"] == "boolean"
        assert dry_run_param["required"] is False  # Optional parameter

    def test_transport_compatibility(self):
        """Test BigQuery template supports expected transport modes."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        transport = template_data["transport"]
        supported = transport["supported"]

        # Should support both HTTP and stdio
        assert "http" in supported, "Should support HTTP transport"
        assert "stdio" in supported, "Should support stdio transport"

        # Default should be HTTP for web access
        assert transport["default"] == "http", "Default transport should be HTTP"

        # Should have proper port configuration
        assert transport["port"] == 7072, "Should use port 7072"
        assert (
            template_data["ports"]["7072"] == 7072
        ), "Port mapping should be consistent"

    def test_authentication_method_coverage(self):
        """Test that all major Google Cloud authentication methods are supported."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        auth_method_config = template_data["config_schema"]["properties"]["auth_method"]
        supported_methods = auth_method_config["enum"]

        # Test all required authentication methods
        required_methods = [
            "service_account",  # For production
            "oauth2",  # For interactive use
            "application_default",  # For development
        ]

        for method in required_methods:
            assert (
                method in supported_methods
            ), f"Authentication method '{method}' should be supported"

    def test_example_configurations(self):
        """Test that example configurations are provided and valid."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        examples = template_data["examples"]

        # Test CLI usage examples
        assert "cli_usage" in examples
        cli_examples = examples["cli_usage"]
        assert len(cli_examples) >= 3, "Should have multiple CLI usage examples"

        # Test basic deployment example
        basic_example = cli_examples[0]
        assert (
            "project_id" in basic_example
        ), "Basic example should show project_id configuration"

        # Test service account example
        service_account_example = None
        for example in cli_examples:
            if "service_account" in example:
                service_account_example = example
                break

        assert (
            service_account_example is not None
        ), "Should have service account example"

        # Test client integration examples
        assert "client_integration" in examples
        client_examples = examples["client_integration"]
        assert "fastmcp" in client_examples, "Should have FastMCP client example"
        assert "curl" in client_examples, "Should have curl example"

    def test_version_and_metadata(self):
        """Test version and metadata are properly set."""
        template_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "template.json"
        )

        with open(template_path, "r") as f:
            template_data = json.load(f)

        # Test version format
        version = template_data["version"]
        version_parts = version.split(".")
        assert len(version_parts) == 3, "Version should follow semver format (x.y.z)"

        # Test metadata
        assert template_data["author"] == "Data Everything"
        assert template_data["category"] == "Database"

        # Test tags
        tags = template_data["tags"]
        expected_tags = ["bigquery", "google-cloud", "database", "analytics", "sql"]
        for tag in expected_tags:
            assert tag in tags, f"Expected tag '{tag}' not found"
