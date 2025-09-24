"""
PostgreSQL MCP Server Template.

A production-ready PostgreSQL MCP server for secure database access with
configurable authentication, read-only mode, SSH tunneling, and comprehensive
query capabilities.
"""

from .config import PostgresServerConfig
from .server import PostgresMCPServer

__all__ = ["PostgresMCPServer", "PostgresServerConfig"]
