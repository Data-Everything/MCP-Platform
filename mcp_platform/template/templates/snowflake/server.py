#!/usr/bin/env python3
"""
Snowflake MCP Server - Enterprise Data Warehouse Integration

A comprehensive MCP server for Snowflake that provides secure read-only access
to databases, schemas, tables, and query execution capabilities with multiple
authentication methods and advanced filtering.
"""

import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
except ImportError:
    # Allow import for testing without dependencies
    snowflake = None
    serialization = None
    load_pem_private_key = None

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .config import SnowflakeServerConfig
except ImportError:
    try:
        from config import SnowflakeServerConfig
    except ImportError:
        # Fallback for Docker or direct script execution
        sys.path.append(os.path.dirname(__file__))
        from config import SnowflakeServerConfig


class SnowflakeMCPServer:
    """
    Snowflake MCP Server implementation using FastMCP.

    Provides secure access to Snowflake data warehouse with:
    - Multiple authentication methods (username/password, OAuth, key pair, browser SSO)
    - Read-only mode enforcement by default
    - Database, schema, and table metadata browsing
    - SQL query execution with configurable limits
    - Pattern-based filtering for databases and schemas
    """

    def __init__(self, config_dict: dict = None):
        """Initialize the Snowflake MCP Server with configuration."""
        self.config = SnowflakeServerConfig(config_dict=config_dict or {})

        # Standard configuration data from config_schema
        self.config_data = self.config.get_template_config()

        # Full template data (potentially modified by double underscore notation)
        self.template_data = self.config.get_template_data()

        self.logger = self.config.logger
        self.connection = None

        # Read-only mode validation
        self.read_only = self.config.get_read_only()
        if not self.read_only:
            self.logger.warning(
                "⚠️  READ-ONLY MODE IS DISABLED! This allows potentially dangerous "
                "DML/DDL operations. Use with extreme caution in production environments."
            )

        # Compile filter patterns
        self.database_filter = self._compile_filter_pattern(
            self.config.get_database_filter_pattern(), "database"
        )
        self.schema_filter = self._compile_filter_pattern(
            self.config.get_schema_filter_pattern(), "schema"
        )

        self.mcp = FastMCP(
            name=self.template_data.get("name", "snowflake-server"),
            instructions="Snowflake MCP server providing secure data warehouse access",
            version=self.template_data.get("version", "1.0.0"),
            host=os.getenv("MCP_HOST", "0.0.0.0"),
            port=(
                int(
                    os.getenv(
                        "MCP_PORT",
                        self.template_data.get("transport", {}).get("port", 7071),
                    )
                )
                if not os.getenv("MCP_TRANSPORT") == "stdio"
                else None
            ),
        )
        logger.info("Snowflake MCP server %s created", self.mcp.name)
        self.register_tools()

    def _compile_filter_pattern(self, pattern: str, filter_type: str) -> Optional[re.Pattern]:
        """Compile a regex pattern for filtering."""
        if not pattern:
            return None

        try:
            compiled_pattern = re.compile(pattern, re.IGNORECASE)
            self.logger.info("Compiled %s filter pattern: %s", filter_type, pattern)
            return compiled_pattern
        except re.error as e:
            self.logger.error("Invalid %s filter pattern '%s': %s", filter_type, pattern, str(e))
            return None

    def _get_connection(self) -> 'snowflake.connector.SnowflakeConnection':
        """Get or create a Snowflake connection."""
        if snowflake is None:
            raise ImportError("snowflake-connector-python is required but not installed")
        if self.connection is None or self.connection.is_closed():
            self.connection = self._create_connection()
        return self.connection

    def _create_connection(self) -> 'snowflake.connector.SnowflakeConnection':
        """Create a new Snowflake connection with configured authentication."""
        if snowflake is None:
            raise ImportError("snowflake-connector-python is required but not installed")
        conn_params = {
            "account": self.config.get_snowflake_account(),
            "user": self.config.get_snowflake_user(),
            "authenticator": self.config.get_snowflake_authenticator(),
            "login_timeout": self.config.get_connection_timeout(),
            "network_timeout": self.config.get_query_timeout(),
        }

        # Add optional connection parameters
        if self.config.get_snowflake_warehouse():
            conn_params["warehouse"] = self.config.get_snowflake_warehouse()
        if self.config.get_snowflake_database():
            conn_params["database"] = self.config.get_snowflake_database()
        if self.config.get_snowflake_schema():
            conn_params["schema"] = self.config.get_snowflake_schema()
        if self.config.get_snowflake_role():
            conn_params["role"] = self.config.get_snowflake_role()

        # Configure authentication method
        auth_method = self.config.get_snowflake_authenticator()
        
        if auth_method == "snowflake":
            # Username/password authentication
            conn_params["password"] = self.config.get_snowflake_password()
            
        elif auth_method == "oauth":
            # OAuth token authentication
            conn_params["token"] = self.config.get_snowflake_oauth_token()
            
        elif auth_method == "snowflake_jwt":
            # Key pair authentication
            private_key = self._load_private_key(
                self.config.get_snowflake_private_key(),
                self.config.get_snowflake_private_key_passphrase()
            )
            conn_params["private_key"] = private_key
            
        elif auth_method == "externalbrowser":
            # Browser-based SSO authentication
            # No additional parameters needed
            pass
            
        elif auth_method.startswith("https://"):
            # Okta endpoint authentication
            conn_params["authenticator"] = auth_method

        try:
            self.logger.info("Connecting to Snowflake account: %s", conn_params["account"])
            connection = snowflake.connector.connect(**conn_params)
            self.logger.info("Successfully connected to Snowflake")
            return connection
        except Exception as e:
            self.logger.error("Failed to connect to Snowflake: %s", str(e))
            raise

    def _load_private_key(self, private_key_data: str, passphrase: str = None) -> bytes:
        """Load and validate RSA private key for key pair authentication."""
        if not private_key_data:
            raise ValueError("Private key is required for snowflake_jwt authentication")
        
        if load_pem_private_key is None:
            raise ImportError("cryptography library is required for key pair authentication")

        # Check if it's a file path
        if private_key_data.startswith("/") or private_key_data.startswith("./"):
            key_path = Path(private_key_data)
            if not key_path.exists():
                raise ValueError(f"Private key file not found: {private_key_data}")
            with open(key_path, "rb") as key_file:
                private_key_bytes = key_file.read()
        else:
            # Assume it's the key content directly
            private_key_bytes = private_key_data.encode('utf-8')

        # Load and validate the private key
        try:
            private_key = load_pem_private_key(
                private_key_bytes,
                password=passphrase.encode('utf-8') if passphrase else None,
            )
            return private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except Exception as e:
            raise ValueError(f"Failed to load private key: {str(e)}")

    def _validate_read_only_query(self, sql_query: str) -> None:
        """Validate that a query is read-only when read_only mode is enabled."""
        if not self.read_only:
            return

        # Normalize the query
        normalized_query = sql_query.strip().upper()
        
        # List of DML/DDL keywords that are not allowed in read-only mode
        prohibited_keywords = [
            "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE",
            "CREATE", "ALTER", "DROP", "RENAME",
            "GRANT", "REVOKE",
            "COPY", "PUT", "GET",
            "CALL", "EXECUTE"
        ]

        # Check if query starts with any prohibited keyword
        for keyword in prohibited_keywords:
            if normalized_query.startswith(keyword):
                raise ValueError(
                    f"Query not allowed in read-only mode: {keyword} operations are prohibited. "
                    f"To enable write operations, set read_only=false (NOT RECOMMENDED for production)."
                )

    def _filter_names(self, names: List[str], filter_pattern: Optional[re.Pattern]) -> List[str]:
        """Filter a list of names using a regex pattern."""
        if not filter_pattern:
            return names

        filtered_names = []
        for name in names:
            if filter_pattern.search(name):
                filtered_names.append(name)
            else:
                self.logger.debug("Filtered out: %s", name)

        self.logger.info("Filtered %d items to %d using pattern", len(names), len(filtered_names))
        return filtered_names

    def _execute_query(self, query: str, fetch_size: int = None) -> Dict[str, Any]:
        """Execute a SQL query and return results."""
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            
            self.logger.info("Executing query: %s", query[:100] + "..." if len(query) > 100 else query)
            cursor.execute(query)
            
            # Fetch results
            if fetch_size:
                results = cursor.fetchmany(fetch_size)
            else:
                results = cursor.fetchall()
                
            # Get column descriptions
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            cursor.close()
            
            return {
                "columns": columns,
                "rows": results,
                "row_count": len(results)
            }
            
        except Exception as e:
            self.logger.error("Query execution failed: %s", str(e))
            raise

    def register_tools(self):
        """Register tools with the MCP server."""
        self.mcp.tool(self.list_databases, tags=["metadata", "database"])
        self.mcp.tool(self.list_schemas, tags=["metadata", "schema"])
        self.mcp.tool(self.list_tables, tags=["metadata", "table"])
        self.mcp.tool(self.describe_table, tags=["metadata", "table", "schema"])
        self.mcp.tool(self.execute_query, tags=["query", "sql"])
        self.mcp.tool(self.get_connection_info, tags=["info", "connection"])
        logger.info("Tools registered with MCP server")

    def list_databases(self) -> Dict[str, Any]:
        """
        List all accessible databases in the Snowflake account.
        
        Returns:
            Dictionary containing list of databases and count
        """
        try:
            query = "SHOW DATABASES"
            result = self._execute_query(query)
            
            # Extract database names (typically in the 'name' column)
            database_names = [row[1] for row in result["rows"] if len(row) > 1]  # Second column is usually name
            
            # Apply filtering if configured
            filtered_databases = self._filter_names(database_names, self.database_filter)
            
            return {
                "databases": filtered_databases,
                "count": len(filtered_databases),
                "total_available": len(database_names),
                "filtered": len(database_names) - len(filtered_databases)
            }
            
        except Exception as e:
            error_msg = f"Failed to list databases: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def list_schemas(self, database_name: str) -> Dict[str, Any]:
        """
        List all schemas in a specified database.
        
        Args:
            database_name: Name of the database to list schemas from
            
        Returns:
            Dictionary containing list of schemas and count
        """
        try:
            # Check if database passes filter
            if self.database_filter and not self.database_filter.search(database_name):
                return {
                    "error": f"Database '{database_name}' is not accessible due to filter restrictions"
                }
            
            query = f"SHOW SCHEMAS IN DATABASE {database_name}"
            result = self._execute_query(query)
            
            # Extract schema names
            schema_names = [row[1] for row in result["rows"] if len(row) > 1]  # Second column is usually name
            
            # Apply filtering if configured
            filtered_schemas = self._filter_names(schema_names, self.schema_filter)
            
            return {
                "database": database_name,
                "schemas": filtered_schemas,
                "count": len(filtered_schemas),
                "total_available": len(schema_names),
                "filtered": len(schema_names) - len(filtered_schemas)
            }
            
        except Exception as e:
            error_msg = f"Failed to list schemas in database '{database_name}': {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def list_tables(self, database_name: str, schema_name: str) -> Dict[str, Any]:
        """
        List all tables in a specified database and schema.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            
        Returns:
            Dictionary containing list of tables and count
        """
        try:
            # Check filters
            if self.database_filter and not self.database_filter.search(database_name):
                return {
                    "error": f"Database '{database_name}' is not accessible due to filter restrictions"
                }
            
            if self.schema_filter and not self.schema_filter.search(schema_name):
                return {
                    "error": f"Schema '{schema_name}' is not accessible due to filter restrictions"
                }
            
            query = f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}"
            result = self._execute_query(query)
            
            # Extract table information
            tables = []
            for row in result["rows"]:
                if len(row) >= 4:  # Typical columns: created_on, name, database_name, schema_name
                    table_info = {
                        "name": row[1],
                        "database": row[2] if len(row) > 2 else database_name,
                        "schema": row[3] if len(row) > 3 else schema_name,
                        "created_on": str(row[0]) if row[0] else None
                    }
                    # Add additional columns if available
                    if len(row) > 4:
                        table_info["kind"] = row[4]  # TABLE, VIEW, etc.
                    tables.append(table_info)
            
            return {
                "database": database_name,
                "schema": schema_name,
                "tables": tables,
                "count": len(tables)
            }
            
        except Exception as e:
            error_msg = f"Failed to list tables in {database_name}.{schema_name}: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def describe_table(self, database_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a table's structure and columns.
        
        Args:
            database_name: Name of the database
            schema_name: Name of the schema
            table_name: Name of the table to describe
            
        Returns:
            Dictionary containing table structure information
        """
        try:
            # Check filters
            if self.database_filter and not self.database_filter.search(database_name):
                return {
                    "error": f"Database '{database_name}' is not accessible due to filter restrictions"
                }
            
            if self.schema_filter and not self.schema_filter.search(schema_name):
                return {
                    "error": f"Schema '{schema_name}' is not accessible due to filter restrictions"
                }
            
            # Get column information
            query = f"DESCRIBE TABLE {database_name}.{schema_name}.{table_name}"
            result = self._execute_query(query)
            
            columns = []
            for row in result["rows"]:
                if len(row) >= 3:  # name, type, kind, null?, default, primary key, unique key, check, expression, comment
                    column_info = {
                        "name": row[0],
                        "type": row[1],
                        "kind": row[2] if len(row) > 2 else "COLUMN",
                        "nullable": row[3] if len(row) > 3 else None,
                        "default": row[4] if len(row) > 4 else None,
                        "primary_key": row[5] if len(row) > 5 else None,
                        "unique": row[6] if len(row) > 6 else None,
                        "check": row[7] if len(row) > 7 else None,
                        "expression": row[8] if len(row) > 8 else None,
                        "comment": row[9] if len(row) > 9 else None
                    }
                    columns.append(column_info)
            
            return {
                "database": database_name,
                "schema": schema_name,
                "table": table_name,
                "columns": columns,
                "column_count": len(columns)
            }
            
        except Exception as e:
            error_msg = f"Failed to describe table {database_name}.{schema_name}.{table_name}: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def execute_query(self, sql_query: str, limit: int = 100) -> Dict[str, Any]:
        """
        Execute a SQL query against the Snowflake database.
        
        Args:
            sql_query: SQL query to execute
            limit: Maximum number of rows to return (default: 100)
            
        Returns:
            Dictionary containing query results
        """
        try:
            # Validate read-only constraints
            self._validate_read_only_query(sql_query)
            
            # Add LIMIT clause if not present and limit is specified
            if limit and limit > 0:
                # Simple check to see if LIMIT is already present
                if "LIMIT" not in sql_query.upper():
                    sql_query = f"{sql_query.rstrip(';')} LIMIT {limit}"
            
            result = self._execute_query(sql_query, fetch_size=limit if limit else None)
            
            return {
                "query": sql_query,
                "columns": result["columns"],
                "rows": result["rows"],
                "row_count": result["row_count"],
                "limited": bool(limit and limit > 0),
                "limit": limit if limit else None,
                "read_only_mode": self.read_only
            }
            
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg, "query": sql_query}

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current Snowflake connection.
        
        Returns:
            Dictionary containing connection information
        """
        try:
            connection = self._get_connection()
            
            # Get current session info
            cursor = connection.cursor()
            cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
            session_info = cursor.fetchone()
            cursor.close()
            
            return {
                "account": session_info[0] if session_info else None,
                "user": session_info[1] if session_info else None,
                "role": session_info[2] if session_info else None,
                "database": session_info[3] if session_info else None,
                "schema": session_info[4] if session_info else None,
                "warehouse": session_info[5] if session_info else None,
                "authenticator": self.config.get_snowflake_authenticator(),
                "read_only_mode": self.read_only,
                "connection_timeout": self.config.get_connection_timeout(),
                "query_timeout": self.config.get_query_timeout(),
                "database_filter": self.config.get_database_filter_pattern() or "none",
                "schema_filter": self.config.get_schema_filter_pattern() or "none"
            }
            
        except Exception as e:
            error_msg = f"Failed to get connection info: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def run(self):
        """Run the Snowflake MCP server."""
        try:
            self.logger.info("Starting Snowflake MCP server...")
            if os.getenv("MCP_TRANSPORT") == "stdio":
                self.mcp.run(transport="stdio")
            else:
                self.mcp.run()
        except KeyboardInterrupt:
            self.logger.info("Server interrupted by user")
        except Exception as e:
            self.logger.error("Server error: %s", str(e))
            raise
        finally:
            if self.connection and not self.connection.is_closed():
                self.connection.close()
                self.logger.info("Snowflake connection closed")


# Create the server instance
server = SnowflakeMCPServer(config_dict={})


@server.mcp.custom_route(path="/health", methods=["GET"])
async def health_check(request: Request):
    """
    Health check endpoint to verify server status.
    """
    try:
        # Try to get connection info as a basic health check
        connection_info = server.get_connection_info()
        
        if "error" not in connection_info:
            return JSONResponse(
                content={
                    "status": "healthy",
                    "service": "snowflake-mcp-server",
                    "version": server.template_data.get("version", "1.0.0"),
                    "read_only": server.read_only,
                    "account": connection_info.get("account"),
                    "user": connection_info.get("user")
                }
            )
        else:
            return JSONResponse(
                content={
                    "status": "unhealthy",
                    "service": "snowflake-mcp-server",
                    "error": connection_info["error"]
                },
                status_code=503
            )
    except Exception as e:
        return JSONResponse(
            content={
                "status": "unhealthy",
                "service": "snowflake-mcp-server",
                "error": str(e)
            },
            status_code=503
        )


if __name__ == "__main__":
    server.run()