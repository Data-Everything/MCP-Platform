"""
Databricks MCP Server Package

This package provides a comprehensive MCP server for interacting with
Databricks SQL warehouses and clusters using FastMCP.
"""

from .config import DatabricksServerConfig
from .server import DatabricksMCPServer

__all__ = ["DatabricksMCPServer", "DatabricksServerConfig"]
