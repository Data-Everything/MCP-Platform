"""
Databricks MCP Server Package

This package provides a comprehensive MCP server for interacting with
Databricks SQL warehouses and clusters using FastMCP.
"""

from .config import DatabricksServerConfig

# Only import server if fastmcp is available (for runtime)
try:
    from .server import DatabricksMCPServer  # noqa: F401

    __all__ = ["DatabricksMCPServer", "DatabricksServerConfig"]
except ImportError:
    # For testing without fastmcp
    __all__ = ["DatabricksServerConfig"]
