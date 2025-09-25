#!/usr/bin/env python3
"""
Documentation builder for MCP Platform.

This script:
1. Uses the existing TemplateDiscovery utility to find usable templates
2. Dynamically generates Templates navigation for mkdocs.yml
3. Uses MCPClient for dynamic tool discovery instead of static configs
4. Generates documentation with tabbed examples for CLI/MCPClient
5. Builds the documentation with mkdocs
"""

import asyncio
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List

import yaml

# Import the TemplateDiscovery utility
sys.path.insert(0, str(Path(__file__).parent.parent))

# Try to import with fallback if kubernetes is not available
try:
    from mcp_platform.template.utils.discovery import TemplateDiscovery
except ImportError as e:
    print(f"Warning: Could not import TemplateDiscovery: {e}")
    print("Falling back to standalone template discovery...")

    # Fallback class for standalone operation
    class TemplateDiscovery:
        def __init__(self):
            pass

        def discover_templates(self):
            """Fallback template discovery."""
            templates = {}
            templates_dir = (
                Path(__file__).parent.parent / "mcp_platform" / "template" / "templates"
            )

            for template_dir in templates_dir.iterdir():
                if template_dir.is_dir():
                    template_json = template_dir / "template.json"
                    if template_json.exists():
                        try:
                            with open(template_json) as f:
                                config = json.load(f)
                            templates[template_dir.name] = config
                            print(f"  ✅ Found template: {template_dir.name}")
                        except Exception as exc:
                            print(f"  ⚠️  Error loading {template_dir.name}: {exc}")

            return templates


# Try to import MCPClient for dynamic tool discovery
try:
    from mcp_platform.client import MCPClient
    from mcp_platform.core.tool_manager import ToolManager

    MCP_CLIENT_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import MCPClient: {e}")
    print("Falling back to static tool discovery...")
    MCP_CLIENT_AVAILABLE = False

try:
    from mcp_platform.utils import ROOT_DIR, TEMPLATES_DIR
except ImportError:
    # Fallback constants if utils module is not available
    ROOT_DIR = Path(__file__).parent.parent
    TEMPLATES_DIR = ROOT_DIR / "mcp_platform" / "template" / "templates"


def cleanup_old_docs(docs_dir: Path):
    """Clean up old generated documentation."""
    print("🧹 Cleaning up old docs...")

    templates_docs_dir = docs_dir / "server-templates"
    if templates_docs_dir.exists():
        for item in templates_docs_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            elif item.name != "index.md":  # Keep the main index.md
                item.unlink()
        print("  🗑️  Cleaned up old server-templates docs")


async def discover_tools_dynamically(template_id: str) -> List[Dict]:
    """
    Discover tools dynamically using MCPClient if available.
    Falls back to static discovery if MCPClient is not available.
    Runs in a separate process to avoid event loop conflicts.
    """
    if not MCP_CLIENT_AVAILABLE:
        print(f"  ⚠️  MCPClient not available, using static discovery for {template_id}")
        return []

    try:
        print(f"  🔍 Attempting dynamic tool discovery for {template_id}...")

        # Use subprocess to avoid event loop conflicts

        # Create a temporary script to run discovery in a separate process
        discovery_script = f"""
import sys
import json
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, "{Path(__file__).parent.parent}")

try:
    from mcp_platform.core.tool_manager import ToolManager

    # Use ToolManager directly for better control
    tool_manager = ToolManager(backend_type="docker")

    # Try to discover tools dynamically with a short timeout
    result = tool_manager.list_tools(
        "{template_id}",
        static=False,  # Only use dynamic discovery
        dynamic=True,
        timeout=60,  # Longer timeout for CI environment
        force_refresh=True,
    )

    tools = result.get("tools", [])
    discovery_method = result.get("discovery_method", "unknown")

    print(json.dumps({{"tools": tools, "discovery_method": discovery_method}}))

except Exception as e:
    print(json.dumps({{"tools": [], "error": str(e)}}))
"""

        # Write script to temporary file and execute
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(discovery_script)
            script_path = f.name

        try:
            # Run in subprocess with timeout
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=120,  # 2 minute timeout
                cwd=Path(__file__).parent.parent,
            )

            if result.returncode == 0:
                try:
                    output_lines = result.stdout.strip().split("\n")
                    # Get the last line which should be our JSON output
                    json_line = output_lines[-1]
                    data = json.loads(json_line)

                    tools = data.get("tools", [])
                    discovery_method = data.get("discovery_method", "unknown")
                    error = data.get("error")

                    if error:
                        print(
                            f"  ⚠️  Dynamic discovery error for {template_id}: {error}"
                        )
                        return []
                    elif tools:
                        print(
                            f"  ✅ Dynamic discovery found {len(tools)} tools via {discovery_method}"
                        )
                        return tools
                    else:
                        print(
                            f"  ⚠️  Dynamic discovery found no tools for {template_id}"
                        )
                        return []

                except (json.JSONDecodeError, IndexError) as e:
                    print(
                        f"  ⚠️  Failed to parse discovery output for {template_id}: {e}"
                    )
                    return []
            else:
                print(
                    f"  ⚠️  Discovery process failed for {template_id}: {result.stderr}"
                )
                return []

        finally:
            # Clean up temporary file
            Path(script_path).unlink(missing_ok=True)

    except Exception as e:
        print(f"  ⚠️  Dynamic tool discovery failed for {template_id}: {e}")
        return []


def discover_tools_static(template_config: Dict) -> List[Dict]:
    """Fallback to static tool discovery from template config."""
    return template_config.get("tools", [])


def scan_template_docs(templates_dir: Path) -> Dict[str, Dict]:
    """Scan template directories for documentation using TemplateDiscovery."""
    print("🔍 Using TemplateDiscovery to find usable templates...")

    template_docs = {}

    # Use the existing TemplateDiscovery utility to find working templates
    discovery = TemplateDiscovery()
    try:
        templates = discovery.discover_templates()
        print(f"✅ TemplateDiscovery found {len(templates)} usable templates")
    except Exception as e:
        print(f"❌ Error using TemplateDiscovery: {e}")
        return {}

    for template_name, template_config in templates.items():
        template_dir = templates_dir / template_name
        docs_index = template_dir / "docs" / "index.md"

        if docs_index.exists():
            template_docs[template_name] = {
                "name": template_config.get("name", template_name.title()),
                "description": template_config.get("description", ""),
                "docs_file": docs_index,
                "config": template_config,
            }
            print(f"  ✅ Found docs for {template_name}")
        else:
            print(f"  ⚠️  Template {template_name} is usable but missing docs/index.md")

    print(f"📋 Found documentation for {len(template_docs)} templates")
    return template_docs


async def generate_usage_md(template_id: str, template_info: Dict) -> str:
    """Generate standardized usage.md content for a template using dynamic tool discovery."""
    template_config = template_info["config"]
    template_name = template_config.get("name", template_id.title())

    # Try dynamic tool discovery first, fall back to static
    tools = await discover_tools_dynamically(template_id)
    discovery_method = "dynamic"

    if not tools:
        tools = discover_tools_static(template_config)
        discovery_method = "static"

    print(
        f"  📝 Generating usage.md for {template_id} using {discovery_method} discovery ({len(tools)} tools)"
    )

    usage_content = f"""# {template_name} Usage Guide

## Overview

This guide shows how to use the {template_name} with different MCP clients and integration methods.

## Tool Discovery

=== "Interactive CLI"

    ```bash
    # Start interactive mode
    mcp_platform interactive

    # List available tools
    mcpp> tools {template_id}
    ```

=== "Regular CLI"

    ```bash
    # Discover tools using CLI
    mcp_platform tools {template_id}
    ```

=== "Python Client"

    ```python
    from mcp_platform.client import MCPClient

    async def discover_tools():
        client = MCPClient()
        tools = client.list_tools("{template_id}")
        for tool in tools:
            print(f"Tool: {{tool['name']}} - {{tool['description']}}")
    ```

## Available Tools

"""

    # Add tool documentation
    if tools:
        for tool in tools:
            tool_name = tool.get("name", "unknown_tool")
            tool_desc = tool.get("description", "No description available")

            # Handle different parameter formats
            tool_params = []
            if "parameters" in tool:
                # Handle parameters as list of objects
                if isinstance(tool["parameters"], list):
                    tool_params = tool["parameters"]
                # Handle parameters as schema with properties
                elif (
                    isinstance(tool["parameters"], dict)
                    and "properties" in tool["parameters"]
                ):
                    properties = tool["parameters"]["properties"]
                    required = tool["parameters"].get("required", [])
                    for param_name, param_def in properties.items():
                        param_obj = {
                            "name": param_name,
                            "type": param_def.get("type", "string"),
                            "description": param_def.get(
                                "description", "No description"
                            ),
                            "required": param_name in required,
                        }
                        tool_params.append(param_obj)

            usage_content += f"""### {tool_name}

**Description**: {tool_desc}

**Parameters**:
"""
            if tool_params:
                for param in tool_params:
                    param_name = param.get("name", "unknown")
                    param_desc = param.get("description", "No description")
                    param_type = param.get("type", "string")
                    param_required = (
                        " (required)" if param.get("required", False) else " (optional)"
                    )
                    usage_content += f"- `{param_name}` ({param_type}){param_required}: {param_desc}\n"
            else:
                usage_content += "- No parameters required\n"

            usage_content += "\n"
    else:
        # Handle templates without defined tools (like GitHub MCP Server)
        usage_content += f"""This template uses an external MCP server implementation. Tools are dynamically discovered at runtime.

Use the tool discovery methods above to see the full list of available tools for this template.

"""

    # Add usage examples section with tabs
    usage_content += f"""## Usage Examples

=== "Interactive CLI"

    ```bash
    # Start interactive mode
    mcp_platform interactive

    # Deploy the template (if not already deployed)
    mcpp> deploy {template_id}

    # List available tools after deployment
    mcpp> tools {template_id}
    ```

"""

    if tools:
        usage_content += "    Then call tools:\n\n"
        # Add interactive CLI examples for each tool
        for tool in tools[:3]:  # Show examples for first 3 tools
            tool_name = tool.get("name", "unknown_tool")

            # Handle different parameter formats for examples
            tool_params = []
            if "parameters" in tool:
                if isinstance(tool["parameters"], list):
                    tool_params = tool["parameters"]
                elif (
                    isinstance(tool["parameters"], dict)
                    and "properties" in tool["parameters"]
                ):
                    properties = tool["parameters"]["properties"]
                    for param_name, param_def in properties.items():
                        param_obj = {
                            "name": param_name,
                            "type": param_def.get("type", "string"),
                        }
                        tool_params.append(param_obj)

            if tool_params:
                # Create example parameters
                example_params = {}
                for param in tool_params:
                    param_name = param.get("name", "param")
                    param_type = param.get("type", "string")
                    if param_type == "string":
                        example_params[param_name] = "example_value"
                    elif param_type == "boolean":
                        example_params[param_name] = True
                    elif param_type == "number" or param_type == "integer":
                        example_params[param_name] = 123
                    else:
                        example_params[param_name] = "example_value"

                params_json = json.dumps(example_params)
                usage_content += f"""    ```bash
    mcpp> call {template_id} {tool_name} '{params_json}'
    ```

"""
            else:
                usage_content += f"""    ```bash
    mcpp> call {template_id} {tool_name}
    ```

"""
    else:
        # Generic example for templates without predefined tools
        usage_content += f"""    Example tool calls (replace with actual tool names discovered above):

    ```bash
    # Example - replace 'tool_name' with actual tool from discovery
    mcpp> call {template_id} tool_name '{{"param": "value"}}'
    ```

"""

    # Add CLI deployment section with tabs
    usage_content += f"""=== "Regular CLI"

    ```bash
    # Deploy the template
    mcp_platform deploy {template_id}

    # Check deployment status
    mcp_platform status

    # View logs
    mcp_platform logs {template_id}

    # Stop the template
    mcp_platform stop {template_id}
    ```

=== "Python Client"

    ```python
    import asyncio
    from mcp_platform.client import MCPClient

    async def use_{template_id.replace('-', '_')}():
        client = MCPClient()

        # Start the server
        deployment = client.start_server("{template_id}", {{}})

        if deployment["success"]:
            deployment_id = deployment["deployment_id"]

            try:
                # Discover available tools
                tools = client.list_tools("{template_id}")
                print(f"Available tools: {{[t['name'] for t in tools]}}")

"""

    # Add Python client examples for each tool
    if tools:
        for tool in tools[:2]:  # Show examples for first 2 tools
            tool_name = tool.get("name", "unknown_tool")

            # Handle different parameter formats for examples
            tool_params = []
            if "parameters" in tool:
                if isinstance(tool["parameters"], list):
                    tool_params = tool["parameters"]
                elif (
                    isinstance(tool["parameters"], dict)
                    and "properties" in tool["parameters"]
                ):
                    properties = tool["parameters"]["properties"]
                    for param_name, param_def in properties.items():
                        param_obj = {
                            "name": param_name,
                            "type": param_def.get("type", "string"),
                        }
                        tool_params.append(param_obj)

            if tool_params:
                example_params = {}
                for param in tool_params:
                    param_name = param.get("name", "param")
                    param_type = param.get("type", "string")
                    if param_type == "string":
                        example_params[param_name] = "example_value"
                    elif param_type == "boolean":
                        example_params[param_name] = True
                    elif param_type == "number" or param_type == "integer":
                        example_params[param_name] = 123
                    else:
                        example_params[param_name] = "example_value"

                usage_content += f"""                # Call {tool_name}
                result = client.call_tool("{template_id}", "{tool_name}", {example_params})
                print(f"{tool_name} result: {{result}}")

"""
            else:
                usage_content += f"""                # Call {tool_name}
                result = client.call_tool("{template_id}", "{tool_name}", {{}})
                print(f"{tool_name} result: {{result}}")

"""
    else:
        # Generic example for templates without predefined tools
        usage_content += f"""                # Example tool call (replace with actual tool name)
                # result = client.call_tool("{template_id}", "tool_name", {{"param": "value"}})
                # print(f"Tool result: {{result}}")

"""

    usage_content += f"""            finally:
                # Clean up
                client.stop_server(deployment_id)
        else:
            print("Failed to start server")

    # Run the example
    asyncio.run(use_{template_id.replace('-', '_')}())
    ```

## Integration Examples

=== "Claude Desktop"

    Add this configuration to your Claude Desktop configuration file:

    **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
    **Windows**: `%APPDATA%\\Claude\\claude_desktop_config.json`

    ```json
    {{
      "mcpServers": {{
        "{template_id}": {{
          "command": "python",
          "args": ["-m", "mcp_platform", "connect", "{template_id}", "--stdio"],
          "env": {{
            "LOG_LEVEL": "info"
          }}
        }}
      }}
    }}
    ```

=== "VS Code"

    Install the MCP extension and add this to your VS Code settings (`.vscode/settings.json`):

    ```json
    {{
      "mcp.servers": {{
        "{template_id}": {{
          "command": "python",
          "args": ["-m", "mcp_platform", "connect", "{template_id}", "--stdio"],
          "env": {{
            "LOG_LEVEL": "info"
          }}
        }}
      }}
    }}
    ```

=== "Manual Connection"

    ```bash
    # Get connection details for other integrations
    mcp_platform connect {template_id} --llm claude
    mcp_platform connect {template_id} --llm vscode
    ```

## Configuration

For template-specific configuration options, see the main template documentation. Common configuration methods:

=== "Environment Variables"

    ```bash
    # Deploy with environment variables
    mcp_platform deploy {template_id} --env KEY=VALUE
    ```

=== "CLI Configuration"

    ```bash
    # Deploy with configuration
    mcp_platform deploy {template_id} --config key=value

    # Deploy with nested configuration
    mcp_platform deploy {template_id} --config category__property=value
    ```

=== "Config File"

    ```bash
    # Deploy with config file
    mcp_platform deploy {template_id} --config-file config.json
    ```

## Troubleshooting

### Common Issues

1. **Template not found**: Ensure the template name is correct
   ```bash
   mcp_platform list  # List available templates
   ```

2. **Connection issues**: Check if the server is running
   ```bash
   mcp_platform status
   ```

3. **Tool discovery fails**: Try refreshing the tool cache
   ```bash
   mcpp> tools {template_id} --refresh
   ```

### Debug Mode

Enable debug logging for troubleshooting:

=== "Interactive CLI"

    ```bash
    # Interactive CLI with debug
    LOG_LEVEL=debug mcp_platform interactive
    ```

=== "Deploy with Debug"

    ```bash
    # Deploy with debug logging
    mcp_platform deploy {template_id} --config log_level=debug
    ```

For more help, see the [main documentation](../../) or open an issue in the repository.
"""

    return usage_content


def generate_api_reference(
    template_id: str, template_info: Dict, tools: List[Dict]
) -> str:
    """Generate API reference documentation for a template."""
    template_name = template_info["name"]
    description = template_info.get("description", "")

    content = f"""# {template_name} API Reference

{description}

This reference provides detailed information about all available tools and their parameters.

## Available Tools

"""

    # Group tools by category if available
    categorized_tools = {}
    for tool in tools:
        category = tool.get("category", "General")
        if category not in categorized_tools:
            categorized_tools[category] = []
        categorized_tools[category].append(tool)

    # Generate documentation for each category
    for category, category_tools in categorized_tools.items():
        if len(categorized_tools) > 1:
            content += f"### {category}\n\n"

        for tool in category_tools:
            name = tool.get("name", "Unknown")
            description = tool.get("description", "No description available")

            content += f"#### `{name}`\n\n"
            content += f"**Description**: {description}\n\n"

            # Parameters
            params = tool.get("parameters", {})

            # Handle both old format (list) and new format (dict)
            if isinstance(params, list):
                # Old format: list of parameter objects
                if params:
                    content += "**Parameters**:\n\n"
                    for param in params:
                        param_name = param.get("name", "unknown")
                        param_type = param.get("type", "unknown")
                        param_desc = param.get("description", "No description")
                        is_required = param.get("required", False)

                        required_badge = (
                            " *(required)*" if is_required else " *(optional)*"
                        )
                        content += f"- **`{param_name}`** ({param_type}){required_badge}: {param_desc}\n"
                    content += "\n"
                else:
                    content += "**Parameters**: No parameters required\n\n"
            elif isinstance(params, dict) and params.get("properties"):
                # New format: dict with properties
                content += "**Parameters**:\n\n"

                properties = params.get("properties", {})
                required = params.get("required", [])

                for param_name, param_info in properties.items():
                    param_type = param_info.get("type", "unknown")
                    param_desc = param_info.get("description", "No description")
                    is_required = param_name in required

                    required_badge = " *(required)*" if is_required else " *(optional)*"
                    content += f"- **`{param_name}`** ({param_type}){required_badge}: {param_desc}\n"

                content += "\n"
            else:
                content += "**Parameters**: No parameters required\n\n"

            # Example usage
            content += f"""**Example Usage**:

=== "CLI"
    ```bash
    mcp_platform call {template_id} {name}
    ```

=== "Python"
    ```python
    from mcp_platform.client import MCPClient

    client = MCPClient()
    result = client.call_tool("{template_id}", "{name}", {{}})
    ```

=== "HTTP API"
    ```bash
    curl -X POST http://localhost:8000/v1/call \\
      -H "Content-Type: application/json" \\
      -d '{{"template_id": "{template_id}", "tool_name": "{name}", "arguments": {{}}}}'
    ```

---

"""

    # Add footer
    content += f"""
## Integration Examples

For more integration examples and usage patterns, see the [Usage Guide](usage.md).

## Support

For questions and issues related to the {template_name}, please refer to:
- [Usage Guide](usage.md) for comprehensive examples
- [Template Overview](index.md) for setup and configuration
- [MCP Platform Documentation](../../index.md) for general platform usage
"""

    return content


async def copy_template_docs(template_docs: Dict[str, Dict], docs_dir: Path):
    """Copy template documentation to docs directory and fix CLI commands."""
    print("📄 Copying template documentation...")

    templates_docs_dir = docs_dir / "server-templates"
    templates_docs_dir.mkdir(exist_ok=True)

    for template_id, template_info in template_docs.items():
        template_doc_dir = templates_docs_dir / template_id
        template_doc_dir.mkdir(exist_ok=True)

        # Generate usage.md file
        usage_content = await generate_usage_md(template_id, template_info)
        with open(template_doc_dir / "usage.md", "w", encoding="utf-8") as f:
            f.write(usage_content)
        print(f"  📝 Generated usage.md for {template_id}")

        # Generate API reference if template has tools
        tools = template_info["config"].get("tools", [])
        if tools and len(tools) > 0:
            api_content = generate_api_reference(template_id, template_info, tools)
            with open(template_doc_dir / "api.md", "w", encoding="utf-8") as f:
                f.write(api_content)
            print(f"  📝 Generated api.md for {template_id} ({len(tools)} tools)")

        # Copy the index.md file and fix CLI commands
        dest_file = template_doc_dir / "index.md"
        with open(template_info["docs_file"], "r", encoding="utf-8") as f:
            content = f.read()

        # Fix CLI commands - add 'python -m' prefix and 'deploy' command
        content = content.replace(
            f"mcpp deploy {template_id}",
            f"mcp_platform deploy {template_id}",
        )
        content = content.replace(
            f"mcpp {template_id}",
            f"mcp_platform deploy {template_id}",
        )
        content = content.replace("mcpp create", "mcp_platform create")
        content = content.replace("mcpp list", "mcp_platform list")
        content = content.replace("mcpp stop", "mcp_platform stop")
        content = content.replace("mcpp logs", "mcp_platform logs")
        content = content.replace("mcpp shell", "mcp_platform shell")
        content = content.replace("mcpp cleanup", "mcp_platform cleanup")

        # Remove existing usage sections and replace with link to usage.md
        content = remove_usage_sections_and_add_link(content, template_id)

        # Add configuration information from template schema if not present
        config_schema = template_info["config"].get("config_schema", {})
        properties = config_schema.get("properties", {})

        if properties and "## Configuration" in content:
            # Generate configuration table
            config_section = "\n## Configuration Options\n\n"
            config_section += (
                "| Property | Type | Environment Variable | Default | Description |\n"
            )
            config_section += (
                "|----------|------|---------------------|---------|-------------|\n"
            )

            for prop_name, prop_config in properties.items():
                prop_type = prop_config.get("type", "string")
                env_mapping = prop_config.get("env_mapping", "")
                default = str(prop_config.get("default", ""))
                description = prop_config.get("description", "")

                config_section += f"| `{prop_name}` | {prop_type} | `{env_mapping}` | `{default}` | {description} |\n"

            config_section += "\n### Usage Examples\n\n"
            config_section += "```bash\n"
            config_section += "# Deploy with configuration\n"
            config_section += f"mcp_platform deploy {template_id} --show-config\n\n"
            if properties:
                first_prop = next(iter(properties.keys()))
                first_prop_config = properties[first_prop]
                if first_prop_config.get("env_mapping"):
                    config_section += "# Using environment variables\n"
                    config_section += f"mcp_platform deploy {template_id} --env {first_prop_config['env_mapping']}=value\n\n"
                config_section += "# Using CLI configuration\n"
                config_section += (
                    "mcp_platform deploy {template_id} --config {first_prop}=value\n\n"
                )
                config_section += "# Using nested configuration\n"
                config_section += "mcp_platform deploy {template_id} --config category__property=value\n"
            config_section += "```\n"

            # Replace or append configuration section
            if "## Configuration" in content and "This template supports" in content:
                # Replace simple configuration section with detailed one

                pattern = r"## Configuration.*?(?=##|\Z)"
                content = re.sub(
                    pattern, config_section.strip(), content, flags=re.DOTALL
                )
            else:
                # Append before Development section or at end
                if "## Development" in content:
                    content = content.replace(
                        "## Development", config_section + "\n## Development"
                    )
                else:
                    content += "\n" + config_section

        # Apply usage section addition AFTER configuration processing
        content = remove_usage_sections_and_add_link(content, template_id)

        with open(dest_file, "w", encoding="utf-8") as f:
            f.write(content)

        # Copy any other documentation files if they exist
        template_docs_source = template_info["docs_file"].parent
        for doc_file in template_docs_source.iterdir():
            if doc_file.name != "index.md" and doc_file.is_file():
                shutil.copy2(doc_file, template_doc_dir / doc_file.name)

        print(f"  📄 Copied and enhanced docs for {template_id}")


def remove_usage_sections_and_add_link(content: str, template_id: str) -> str:
    """Remove usage sections from content and add link to usage.md."""
    import re

    # More comprehensive usage section patterns to remove
    usage_patterns = [
        r"## Available Tools\s*\n.*?(?=##|\Z)",  # ## Available Tools section
        r"### Available Tools\s*\n.*?(?=###|##|\Z)",  # ### Available Tools section
        r"## Usage\s*\n.*?(?=##|\Z)",  # ## Usage section
        r"### Usage\s*\n.*?(?=###|##|\Z)",  # ### Usage section
        r"## Usage Examples\s*\n.*?(?=##|\Z)",  # ## Usage Examples section
        r"### Usage Examples\s*\n.*?(?=###|##|\Z)",  # ### Usage Examples section
        r"## API Reference\s*\n.*?(?=##|\Z)",  # ## API Reference section
        r"### API Reference\s*\n.*?(?=###|##|\Z)",  # ### API Reference section
        r"## Tool Documentation\s*\n.*?(?=##|\Z)",  # ## Tool Documentation section
        r"### Tool Documentation\s*\n.*?(?=###|##|\Z)",  # ### Tool Documentation section
        r"## Client Integration\s*\n.*?(?=##|\Z)",  # ## Client Integration section
        r"### Client Integration\s*\n.*?(?=###|##|\Z)",  # ### Client Integration section
        r"## Integration Examples\s*\n.*?(?=##|\Z)",  # ## Integration Examples section
        r"### Integration Examples\s*\n.*?(?=###|##|\Z)",  # ### Integration Examples section
        r"## FastMCP Client\s*\n.*?(?=##|\Z)",  # ## FastMCP Client section
        r"### FastMCP Client\s*\n.*?(?=###|##|\Z)",  # ### FastMCP Client section
        r"## Claude Desktop Integration\s*\n.*?(?=##|\Z)",  # ## Claude Desktop Integration section
        r"### Claude Desktop Integration\s*\n.*?(?=###|##|\Z)",  # ### Claude Desktop Integration section
        r"## VS Code Integration\s*\n.*?(?=##|\Z)",  # ## VS Code Integration section
        r"### VS Code Integration\s*\n.*?(?=###|##|\Z)",  # ### VS Code Integration section
        r"## cURL Testing\s*\n.*?(?=##|\Z)",  # ## cURL Testing section
        r"### cURL Testing\s*\n.*?(?=###|##|\Z)",  # ### cURL Testing section
        r"## HTTP Endpoints\s*\n.*?(?=##|\Z)",  # ## HTTP Endpoints section
        r"### HTTP Endpoints\s*\n.*?(?=###|##|\Z)",  # ### HTTP Endpoints section
        r"## Tool Management\s*\n.*?(?=##|\Z)",  # ## Tool Management section
        r"### Tool Management\s*\n.*?(?=###|##|\Z)",  # ### Tool Management section
    ]

    # Remove curl examples that show direct server access
    curl_patterns = [
        r"```bash\s*\n.*?curl.*?:707[0-9]/call.*?\n.*?```",  # curl examples with port 7070-7079
        r"```bash\s*\n.*?curl.*?localhost:707[0-9].*?\n.*?```",  # curl examples with localhost:707x
        r"```\s*\n.*?curl.*?:707[0-9]/call.*?\n.*?```",  # curl examples without bash
    ]

    # Remove usage-related sections
    for pattern in usage_patterns:
        content = re.sub(pattern, "", content, flags=re.DOTALL)

    # Remove incorrect curl examples
    for pattern in curl_patterns:
        content = re.sub(pattern, "", content, flags=re.DOTALL | re.MULTILINE)

    # Clean up multiple consecutive newlines
    content = re.sub(r"\n{3,}", "\n\n", content)

    # Create a more prominent usage section with better integration
    usage_section = (
        "## Usage & API Reference\n\n"
        "For comprehensive usage examples, tool documentation, and integration guides:\n\n"
        "**[View Complete Usage Guide](usage.md)**\n\n"
        "The usage guide includes:\n"
        "- **Available Tools** - Complete list of tools with parameters and examples\n"
        "- **Integration Examples** - Python, JavaScript, and CLI usage\n"
        "- **HTTP API** - REST endpoint documentation\n"
        "- **Configuration** - Setup and deployment options\n"
        "- **Best Practices** - Tips for optimal usage\n\n"
    )

    # Insert before common end sections
    insert_patterns = [
        "## Configuration",
        "## Troubleshooting",
        "## Contributing",
        "## License",
        "## Support",
        "## Development",
    ]

    inserted = False
    for pattern in insert_patterns:
        if pattern in content:
            content = content.replace(pattern, usage_section + pattern)
            inserted = True
            break

    # If no insertion point found, append at the end
    if not inserted:
        content = content.rstrip() + "\n" + usage_section

    return content


def generate_templates_index(template_docs: Dict[str, Dict], docs_dir: Path):
    """Generate an index page for all templates."""
    print("📝 Generating templates index...")

    templates_docs_dir = docs_dir / "server-templates"

    # Generate the main index.md for the templates section
    index_md = templates_docs_dir / "index.md"
    index_content = """# MCP Platform Templates

Welcome to the MCP Platform Templates documentation! This section provides comprehensive information about available Model Context Protocol (MCP) server templates that you can use to quickly deploy MCP servers for various use cases.

## What are MCP Platform Templates?

MCP Platform Templates are pre-configured, production-ready templates that implement the Model Context Protocol specification. Each template is designed for specific use cases and comes with:

- 🔧 **Complete configuration files**
- 📖 **Comprehensive documentation**
- 🧪 **Built-in tests**
- 🐳 **Docker support**
- ☸️ **Kubernetes deployment manifests**

## Available Templates

Browse our collection of templates:

- [Available Templates](available.md) - Complete list of all available templates

## Quick Start

1. **Choose a template** from our [available templates](available.md)
2. **Deploy locally** using Docker Compose or our deployment tools
3. **Configure** the template for your specific needs
4. **Deploy to production** using Kubernetes or your preferred platform

## Template Categories

Our templates are organized by functionality:

- **Database Connectors** - Connect to various database systems
- **File Servers** - File management and sharing capabilities
- **API Integrations** - Third-party service integrations
- **Demo Servers** - Learning and testing examples

## Getting Help

If you need assistance with any template:

1. Check the template-specific documentation
2. Review the troubleshooting guides
3. Visit our GitHub repository for issues and discussions

## Contributing

Interested in contributing a new template? See our contribution guidelines to get started.
"""

    with open(index_md, "w", encoding="utf-8") as f:
        f.write(index_content)

    # Generate the available.md file
    available_md = templates_docs_dir / "available.md"

    content = """# Available Templates

This page lists all available MCP Platform server templates.

"""

    # Sort templates by name
    sorted_templates = sorted(template_docs.items(), key=lambda x: x[1]["name"])

    for template_id, template_info in sorted_templates:
        content += f"""## [{template_info["name"]}]({template_id}/index.md)

{template_info["description"]}

**Template ID:** `{template_id}`

**Version:** {template_info["config"].get("version", "1.0.0")}

**Author:** {template_info["config"].get("author", "Unknown")}

---

"""

    with open(available_md, "w", encoding="utf-8") as f:
        f.write(content)

    print("✅ Templates index generated")


def update_mkdocs_nav(template_docs: Dict[str, Dict], mkdocs_file: Path):
    """Update mkdocs.yml navigation with modern template structure."""
    print("⚙️  Updating mkdocs navigation with modern template structure...")

    with open(mkdocs_file, "r", encoding="utf-8") as f:
        mkdocs_config = yaml.safe_load(f)

    # Find the Templates section in nav
    nav = mkdocs_config.get("nav", [])

    # Build modern template navigation structure
    template_nav_items = [
        {"Overview": "server-templates/index.md"},
        {"Available Templates": "server-templates/available.md"},
    ]

    # Add each template with subsections for overview and usage
    sorted_templates = sorted(template_docs.items(), key=lambda x: x[1]["name"])
    for template_id, template_info in sorted_templates:
        template_name = template_info["name"]

        # Create subsection for each template with multiple pages
        template_subsection = {
            template_name: [
                {"Overview": f"server-templates/{template_id}/index.md"},
                {"Usage Guide": f"server-templates/{template_id}/usage.md"},
            ]
        }

        # Add API Reference if the template has tools
        tools = template_info["config"].get("tools", [])
        if tools and len(tools) > 0:
            template_subsection[template_name].append(
                {"API Reference": f"server-templates/{template_id}/api.md"}
            )

        template_nav_items.append(template_subsection)

    # Find and update the Templates section, or create it if not found
    templates_section_found = False
    for i, section in enumerate(nav):
        if isinstance(section, dict) and "Templates" in section:
            nav[i]["Templates"] = template_nav_items
            templates_section_found = True
            print(
                f"  ✅ Updated Templates section with modern structure for {len(sorted_templates)} templates"
            )
            break

    # If Templates section not found, add it after Getting Started
    if not templates_section_found:
        templates_section = {"Templates": template_nav_items}

        # Find where to insert (after Getting Started if it exists)
        insert_index = 1  # Default to after Home
        for i, section in enumerate(nav):
            if isinstance(section, dict) and "Getting Started" in section:
                insert_index = i + 1
                break

        nav.insert(insert_index, templates_section)
        print(
            f"  ✅ Created Templates section with modern structure for {len(sorted_templates)} templates"
        )

    # Write back the updated config
    with open(mkdocs_file, "w", encoding="utf-8") as f:
        yaml.dump(
            mkdocs_config,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=1000,
        )

    print("✅ MkDocs navigation updated with modern template structure")


def build_docs():
    """Build the documentation with mkdocs."""
    print("🏗️  Building documentation with MkDocs...")

    try:
        result = subprocess.run(
            ["mkdocs", "build"], check=True, capture_output=True, text=True
        )
        print("✅ Documentation built successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Documentation build failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        print(
            "❌ mkdocs command not found. Please install mkdocs: pip install mkdocs mkdocs-material"
        )
        return False


async def main():
    """Main function to build documentation."""
    project_root = ROOT_DIR
    templates_dir = TEMPLATES_DIR
    docs_dir = project_root / "docs"
    mkdocs_file = project_root / "mkdocs.yml"

    print("🚀 Starting documentation build process...")

    # Ensure docs directory exists
    docs_dir.mkdir(exist_ok=True)

    # Clean docs directory
    cleanup_old_docs(docs_dir)

    # Scan for template documentation
    template_docs = scan_template_docs(templates_dir)

    if not template_docs:
        print("❌ No template documentation found. Exiting.")
        sys.exit(1)

    # Copy template docs (now with async tool discovery)
    await copy_template_docs(template_docs, docs_dir)

    # Generate templates index
    generate_templates_index(template_docs, docs_dir)

    # Update mkdocs navigation dynamically
    update_mkdocs_nav(template_docs, mkdocs_file)


if __name__ == "__main__":
    asyncio.run(main())
