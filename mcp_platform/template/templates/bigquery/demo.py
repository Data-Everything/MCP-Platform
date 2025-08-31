#!/usr/bin/env python3
"""
BigQuery MCP Server Demo Script.

This script demonstrates the BigQuery MCP server functionality with mock data
to show how the server would work in a real environment.
"""

import json
import os
import sys

# Add the parent directory to sys.path for imports
sys.path.append(os.path.dirname(__file__))


def demonstrate_bigquery_mcp_server():
    """Demonstrate BigQuery MCP server capabilities."""

    print("🚀 BigQuery MCP Server Demo")
    print("=" * 50)

    # Load template configuration
    with open("template.json", "r") as f:
        template = json.load(f)

    print(f"Server: {template['name']}")
    print(f"Version: {template['version']}")
    print(f"Description: {template['description']}")
    print()

    # Demonstrate configuration options
    print("📋 Configuration Options:")
    print("-" * 30)

    config_examples = [
        {
            "name": "Basic Setup (Application Default Credentials)",
            "config": {
                "project_id": "my-gcp-project",
                "auth_method": "application_default",
                "read_only": True,
            },
        },
        {
            "name": "Production Setup (Service Account)",
            "config": {
                "project_id": "prod-analytics-project",
                "auth_method": "service_account",
                "service_account_path": "/secrets/bigquery-sa.json",
                "read_only": True,
                "allowed_datasets": "analytics_*,reporting_*",
            },
        },
        {
            "name": "Development Setup (OAuth2 with Write Access)",
            "config": {
                "project_id": "dev-project",
                "auth_method": "oauth2",
                "read_only": False,
                "allowed_datasets": "dev_*,staging_*",
                "query_timeout": 600,
                "max_results": 500,
            },
        },
    ]

    for i, example in enumerate(config_examples, 1):
        print(f"{i}. {example['name']}:")
        for key, value in example["config"].items():
            print(f"   {key}: {value}")
        print()

    # Demonstrate available tools
    print("🔧 Available Tools:")
    print("-" * 20)

    tools = template["tools"]
    for i, tool in enumerate(tools, 1):
        print(f"{i}. {tool['name']}")
        print(f"   Description: {tool['description']}")
        if tool["parameters"]:
            print("   Parameters:")
            for param in tool["parameters"]:
                required = "required" if param["required"] else "optional"
                print(
                    f"     - {param['name']} ({param['type']}, {required}): {param['description']}"
                )
        else:
            print("   Parameters: None")
        print()

    # Demonstrate security features
    print("🔒 Security Features:")
    print("-" * 22)

    security_features = [
        "✅ Read-only mode enabled by default",
        "✅ Dataset access control via patterns (e.g., 'analytics_*')",
        "✅ Advanced regex filtering for datasets",
        "✅ Query timeout limits (10s - 1 hour)",
        "✅ Result size limits (1 - 10,000 rows)",
        "✅ Write operation detection and blocking",
        "✅ Multiple authentication methods",
        "✅ Environment variable configuration",
        "✅ Comprehensive error handling",
        "✅ Audit logging capabilities",
    ]

    for feature in security_features:
        print(f"  {feature}")
    print()

    # Demonstrate usage examples
    print("💡 Usage Examples:")
    print("-" * 18)

    usage_examples = [
        {
            "title": "List all accessible datasets",
            "method": "list_datasets",
            "params": {},
            "description": "Returns all datasets the server has access to based on configuration",
        },
        {
            "title": "Explore table schema",
            "method": "describe_table",
            "params": {"dataset_id": "analytics_prod", "table_id": "user_events"},
            "description": "Get detailed schema information including nested fields",
        },
        {
            "title": "Execute analytics query",
            "method": "execute_query",
            "params": {
                "query": "SELECT user_id, COUNT(*) as events FROM `project.analytics.events` WHERE DATE(timestamp) = CURRENT_DATE() GROUP BY user_id LIMIT 100"
            },
            "description": "Run SQL query with automatic safety checks",
        },
        {
            "title": "Validate query without execution",
            "method": "execute_query",
            "params": {"query": "SELECT COUNT(*) FROM large_table", "dry_run": True},
            "description": "Check query validity and estimate processing cost",
        },
    ]

    for i, example in enumerate(usage_examples, 1):
        print(f"{i}. {example['title']}:")
        print(f"   Method: {example['method']}")
        print(f"   Parameters: {json.dumps(example['params'], indent=6)}")
        print(f"   Description: {example['description']}")
        print()

    # Show deployment commands
    print("🚀 Deployment Commands:")
    print("-" * 24)

    deployment_commands = [
        "# Basic deployment",
        "python -m mcp_platform deploy bigquery --config project_id=my-project",
        "",
        "# Production deployment with service account",
        "python -m mcp_platform deploy bigquery \\",
        "  --config project_id=prod-project \\",
        "  --config auth_method=service_account \\",
        "  --config service_account_path=/path/to/sa.json \\",
        "  --config allowed_datasets='analytics_*,reporting_*'",
        "",
        "# Docker deployment",
        "docker run -p 7072:7072 \\",
        "  -e GOOGLE_CLOUD_PROJECT=my-project \\",
        "  -e BIGQUERY_READ_ONLY=true \\",
        "  -e BIGQUERY_ALLOWED_DATASETS='analytics_*' \\",
        "  dataeverything/mcp-bigquery:latest",
    ]

    for cmd in deployment_commands:
        print(cmd)

    print()
    print("✨ BigQuery MCP Server is ready for production use!")
    print("📚 See README.md for complete documentation")


if __name__ == "__main__":
    demonstrate_bigquery_mcp_server()
