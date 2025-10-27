#!/usr/bin/env python3
"""
Snowflake MCP Server - Production-ready implementation.

A secure Snowflake MCP server that provides controlled access to Snowflake
data warehouses with configurable authentication, read-only mode, and
comprehensive query execution capabilities using FastMCP and Snowflake Python Connector.
"""

import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

import sqlparse
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .config import SnowflakeServerConfig
    from .response_formatter import SnowflakeResponseFormatter
except ImportError:
    try:
        from config import SnowflakeServerConfig
        from response_formatter import SnowflakeResponseFormatter
    except ImportError:
        # Fallback for Docker or direct script execution
        sys.path.append(os.path.dirname(__file__))
        from config import SnowflakeServerConfig
        from response_formatter import SnowflakeResponseFormatter

# Snowflake imports
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    from snowflake.connector.errors import DatabaseError

    SNOWFLAKE_AVAILABLE = True
except ImportError as e:
    logger.error("Snowflake connector not available: %s", e)
    SNOWFLAKE_AVAILABLE = False


class SnowflakeMCPServer:
    """
    Snowflake MCP Server implementation using FastMCP and Snowflake Python Connector.

    This server provides secure access to Snowflake data warehouses with:
    - Multiple authentication methods (password, key-pair, OAuth, SSO, etc.)
    - Read-only mode enforcement with configurable override
    - Comprehensive query execution and schema inspection tools
    - Session management and keep-alive support
    """

    def __init__(self, config_dict: dict = None, skip_validation: bool = False):
        """Initialize the Snowflake MCP Server with configuration."""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is required but not available"
            )

        self._skip_validation = skip_validation
        self.config = SnowflakeServerConfig(
            config_dict=config_dict or {}, skip_validation=skip_validation
        )

        # Get configuration
        self.config_data = self.config.get_template_config()
        self.template_data = self.config.get_template_data()

        self.logger = logging.getLogger(__name__)
        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None
        self.formatter = SnowflakeResponseFormatter()
        self.version = self.template_data.get("version", "1.0.0")

        # Initialize FastMCP
        self.mcp = FastMCP(
            name=self.template_data.get("name", "snowflake-server"),
            instructions="Snowflake data warehouse server for secure data access",
            version=self.version,
        )

        # Register tools
        self._register_tools()

        # Initialize connection
        if not skip_validation:
            self._initialize_connection()

    def _register_tools(self):
        """Register all MCP tools."""

        @self.mcp.tool()
        def list_databases() -> Dict[str, Any]:
            """List all accessible databases in Snowflake."""
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SHOW DATABASES")
                    databases = cursor.fetchall()

                return self.formatter.format_database_list(databases)

            except Exception as e:
                self.logger.error("Error listing databases: %s", e)
                return self.formatter.format_error(e, "list_databases")

        @self.mcp.tool()
        def list_schemas(database: str = None) -> Dict[str, Any]:
            """
            List all accessible schemas in a database.

            Args:
                database: Database name to list schemas from (optional)
            """
            try:
                with self._get_cursor() as cursor:
                    if database:
                        if not self._is_database_allowed(database):
                            raise ValueError(f"Access denied to database: {database}")
                        cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
                    else:
                        cursor.execute("SHOW SCHEMAS")
                    schemas = cursor.fetchall()

                return self.formatter.format_schema_list(schemas, database)

            except Exception as e:
                self.logger.error("Error listing schemas: %s", e)
                return self.formatter.format_error(e, "list_schemas")

        @self.mcp.tool()
        def list_tables(schema: str = None, database: str = None) -> Dict[str, Any]:
            """
            List tables in a specific schema.

            Args:
                schema: Schema name to list tables from (optional)
                database: Database name containing the schema (optional)
            """
            try:
                with self._get_cursor() as cursor:
                    if database and schema:
                        if not self._is_database_allowed(database):
                            raise ValueError(f"Access denied to database: {database}")
                        if not self._is_schema_allowed(schema):
                            raise ValueError(f"Access denied to schema: {schema}")
                        cursor.execute(f"SHOW TABLES IN {database}.{schema}")
                    elif schema:
                        if not self._is_schema_allowed(schema):
                            raise ValueError(f"Access denied to schema: {schema}")
                        cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
                    else:
                        cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()

                return self.formatter.format_table_list(tables, database, schema)

            except Exception as e:
                self.logger.error("Error listing tables: %s", e)
                return self.formatter.format_error(e, "list_tables")

        @self.mcp.tool()
        def describe_table(
            table: str, schema: str = None, database: str = None
        ) -> Dict[str, Any]:
            """
            Get detailed schema information for a table.

            Args:
                table: Table name to describe
                schema: Schema name containing the table (optional)
                database: Database name containing the schema (optional)
            """
            try:
                with self._get_cursor() as cursor:
                    table_ref = self._build_table_reference(table, schema, database)
                    if not self._is_table_access_allowed(table_ref):
                        raise ValueError(f"Access denied to table: {table_ref}")

                    cursor.execute(f"DESCRIBE TABLE {table_ref}")
                    columns = cursor.fetchall()

                return self.formatter.format_table_description(
                    columns, table, database, schema
                )

            except Exception as e:
                self.logger.error("Error describing table %s: %s", table, e)
                return self.formatter.format_error(e, f"describe_table: {table}")

        @self.mcp.tool()
        def list_columns(
            table: str, schema: str = None, database: str = None
        ) -> Dict[str, Any]:
            """
            List columns in a specific table.

            Args:
                table: Table name to list columns from
                schema: Schema name containing the table (optional)
                database: Database name containing the schema (optional)
            """
            try:
                with self._get_cursor() as cursor:
                    table_ref = self._build_table_reference(table, schema, database)
                    if not self._is_table_access_allowed(table_ref):
                        raise ValueError(f"Access denied to table: {table_ref}")

                    cursor.execute(f"DESCRIBE TABLE {table_ref}")
                    columns = cursor.fetchall()

                # Extract just column names and types for simpler response
                column_list = [
                    {"name": col.get("name", ""), "type": col.get("type", "")}
                    for col in columns
                ]

                return {
                    "database": database,
                    "schema": schema,
                    "table": table,
                    "columns": column_list,
                    "column_count": len(column_list),
                }

            except Exception as e:
                self.logger.error("Error listing columns for %s: %s", table, e)
                return self.formatter.format_error(e, f"list_columns: {table}")

        @self.mcp.tool()
        def execute_query(query: str, limit: int = None) -> Dict[str, Any]:
            """
            Execute a SQL query against Snowflake (subject to read-only restrictions).

            Args:
                query: SQL query to execute
                limit: Maximum number of rows to return (optional)
            """
            try:
                # Validate and potentially modify query
                validated_query = self._validate_and_prepare_query(query, limit)

                with self._get_cursor() as cursor:
                    start_time = time.time()
                    cursor.execute(validated_query)
                    execution_time = time.time() - start_time

                    result = self.formatter.format_query_results(cursor, query, limit)
                    result["execution_time_seconds"] = round(execution_time, 3)

                    return result

            except Exception as e:
                self.logger.error("Error executing query: %s", e)
                return self.formatter.format_error(e, "execute_query")

        @self.mcp.tool()
        def explain_query(query: str) -> Dict[str, Any]:
            """
            Get query execution plan for a SQL query.

            Args:
                query: SQL query to explain
            """
            try:
                with self._get_cursor() as cursor:
                    explain_query = f"EXPLAIN {query}"
                    cursor.execute(explain_query)
                    explain_rows = cursor.fetchall()

                return self.formatter.format_explain_results(explain_rows, query)

            except Exception as e:
                self.logger.error("Error explaining query: %s", e)
                return self.formatter.format_error(e, "explain_query")

        @self.mcp.tool()
        def get_warehouse_info() -> Dict[str, Any]:
            """Get information about the current Snowflake warehouse."""
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SHOW WAREHOUSES LIKE CURRENT_WAREHOUSE()")
                    warehouse_data = cursor.fetchone()

                    if warehouse_data:
                        return self.formatter.format_warehouse_info(warehouse_data)
                    else:
                        return {"error": "No current warehouse information available"}

            except Exception as e:
                self.logger.error("Error getting warehouse info: %s", e)
                return self.formatter.format_error(e, "get_warehouse_info")

        @self.mcp.tool()
        def list_warehouses() -> Dict[str, Any]:
            """List available Snowflake warehouses."""
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SHOW WAREHOUSES")
                    warehouses = cursor.fetchall()

                    warehouse_list = [
                        {
                            "name": wh.get("name", ""),
                            "state": wh.get("state", ""),
                            "size": wh.get("size", ""),
                            "type": wh.get("type", ""),
                            "is_current": wh.get("is_current", ""),
                            "is_default": wh.get("is_default", ""),
                        }
                        for wh in warehouses
                    ]

                return {"warehouses": warehouse_list, "count": len(warehouse_list)}

            except Exception as e:
                self.logger.error("Error listing warehouses: %s", e)
                return self.formatter.format_error(e, "list_warehouses")

        @self.mcp.tool()
        def get_account_info() -> Dict[str, Any]:
            """Get information about the Snowflake account."""
            try:
                with self._get_cursor() as cursor:
                    # Get account information
                    cursor.execute(
                        "SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION()"
                    )
                    account_info = cursor.fetchone()

                    return {
                        "account": account_info[0] if account_info else None,
                        "region": account_info[1] if account_info else None,
                        "version": account_info[2] if account_info else None,
                    }

            except Exception as e:
                self.logger.error("Error getting account info: %s", e)
                return self.formatter.format_error(e, "get_account_info")

        @self.mcp.tool()
        def get_current_role() -> Dict[str, Any]:
            """Get the current Snowflake role."""
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT CURRENT_ROLE()")
                    role_result = cursor.fetchone()

                    return {"current_role": role_result[0] if role_result else None}

            except Exception as e:
                self.logger.error("Error getting current role: %s", e)
                return self.formatter.format_error(e, "get_current_role")

        @self.mcp.tool()
        def list_roles() -> Dict[str, Any]:
            """List available Snowflake roles."""
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SHOW ROLES")
                    roles = cursor.fetchall()

                    role_list = [
                        {
                            "name": role.get("name", ""),
                            "owner": role.get("owner", ""),
                            "comment": role.get("comment", ""),
                            "is_default": role.get("is_default", ""),
                            "is_current": role.get("is_current", ""),
                            "is_inherited": role.get("is_inherited", ""),
                        }
                        for role in roles
                    ]

                return {"roles": role_list, "count": len(role_list)}

            except Exception as e:
                self.logger.error("Error listing roles: %s", e)
                return self.formatter.format_error(e, "list_roles")

        @self.mcp.tool()
        def get_table_stats(
            table: str, schema: str = None, database: str = None
        ) -> Dict[str, Any]:
            """
            Get statistics for a specific table.

            Args:
                table: Table name to get statistics for
                schema: Schema name containing the table (optional)
                database: Database name containing the schema (optional)
            """
            try:
                with self._get_cursor() as cursor:
                    table_ref = self._build_table_reference(table, schema, database)
                    if not self._is_table_access_allowed(table_ref):
                        raise ValueError(f"Access denied to table: {table_ref}")

                    # Get table information from information schema
                    cursor.execute(
                        f"""
                        SELECT table_catalog, table_schema, table_name, table_type,
                               row_count, bytes, created, last_altered, comment
                        FROM information_schema.tables 
                        WHERE table_name = '{table}'
                        {f"AND table_schema = '{schema}'" if schema else ""}
                        {f"AND table_catalog = '{database}'" if database else ""}
                    """
                    )
                    stats = cursor.fetchone()

                    if stats:
                        return {
                            "database": stats[0],
                            "schema": stats[1],
                            "table": stats[2],
                            "type": stats[3],
                            "row_count": stats[4],
                            "bytes": stats[5],
                            "created": self.formatter._format_timestamp(stats[6]),
                            "last_altered": self.formatter._format_timestamp(stats[7]),
                            "comment": stats[8],
                        }
                    else:
                        return {"error": f"Table not found: {table_ref}"}

            except Exception as e:
                self.logger.error("Error getting table stats for %s: %s", table, e)
                return self.formatter.format_error(e, f"get_table_stats: {table}")

        @self.mcp.tool()
        def test_connection() -> Dict[str, Any]:
            """Test the Snowflake connection."""
            try:
                with self._get_cursor() as cursor:
                    cursor.execute("SELECT 1 as test_result")
                    result = cursor.fetchone()

                    return {
                        "connection_status": "success",
                        "test_result": result[0] if result else None,
                        "message": "Connection test successful",
                    }

            except Exception as e:
                self.logger.error("Connection test failed: %s", e)
                return self.formatter.format_error(e, "test_connection")

        @self.mcp.tool()
        def get_connection_info() -> Dict[str, Any]:
            """Get information about the current Snowflake connection."""
            try:
                with self._get_cursor() as cursor:
                    # Get connection information
                    cursor.execute(
                        """
                        SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_DATABASE(), 
                               CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE(),
                               CURRENT_REGION(), CURRENT_VERSION()
                    """
                    )
                    info = cursor.fetchone()

                    connection_info = {
                        "account": info[0] if info else self.config_data.get("account"),
                        "user": info[1] if info else self.config_data.get("user"),
                        "database": (
                            info[2] if info else self.config_data.get("database")
                        ),
                        "schema": info[3] if info else self.config_data.get("schema"),
                        "warehouse": (
                            info[4] if info else self.config_data.get("warehouse")
                        ),
                        "role": info[5] if info else self.config_data.get("role"),
                        "region": info[6] if info else self.config_data.get("region"),
                        "server_version": info[7] if info else None,
                        "authenticator": self.config_data.get("authenticator"),
                        "read_only_mode": self.config_data.get("read_only", True),
                        "max_results": self.config_data.get("max_results"),
                        "connection_timeout": self.config_data.get(
                            "connection_timeout"
                        ),
                        "query_timeout": self.config_data.get("query_timeout"),
                    }

                return self.formatter.format_connection_info(connection_info)

            except Exception as e:
                self.logger.error("Error getting connection info: %s", e)
                return self.formatter.format_error(e, "get_connection_info")

    @contextmanager
    def _get_cursor(self):
        """Get a database cursor with proper error handling."""
        cursor = None
        try:
            if not self.connection:
                self._initialize_connection()

            cursor = self.connection.cursor(DictCursor)
            yield cursor
        except Exception as e:
            self.logger.error("Database cursor error: %s", e)
            raise
        finally:
            if cursor:
                cursor.close()

    def _initialize_connection(self):
        """Initialize the Snowflake connection."""
        try:
            self.logger.info("Initializing Snowflake connection...")

            # Get connection parameters
            connection_params = self.config.get_connection_params()

            # Create connection
            self.connection = snowflake.connector.connect(**connection_params)

            # Test connection
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")

            self.logger.info("Snowflake connection established successfully")

        except Exception as e:
            self.logger.error("Failed to initialize Snowflake connection: %s", e)
            if self.connection:
                self.connection.close()
                self.connection = None
            raise

    def _validate_and_prepare_query(
        self, query: str, limit: Optional[int] = None
    ) -> str:
        """
        Validate and prepare SQL query for execution.

        Args:
            query: SQL query to validate
            limit: Optional row limit to apply

        Returns:
            Validated and potentially modified query

        Raises:
            ValueError: If query is not allowed in read-only mode
        """
        # Check if in read-only mode
        if self.config_data.get("read_only", True):
            if self._is_write_query(query):
                raise ValueError("Write queries are not allowed in read-only mode")

        # Apply row limit if specified
        max_results = limit or self.config_data.get("max_results", 10000)

        # Parse query to check if it already has a LIMIT clause
        try:
            parsed = sqlparse.parse(query)
            if parsed:
                statement = parsed[0]
                tokens = list(statement.flatten())

                # Check if LIMIT is already present
                has_limit = any(
                    token.ttype is sqlparse.tokens.Keyword
                    and token.value.upper() == "LIMIT"
                    for token in tokens
                )

                if not has_limit and max_results:
                    # Add LIMIT clause
                    query = f"{query.rstrip(';')} LIMIT {max_results}"

        except Exception as e:
            self.logger.warning("Failed to parse query for LIMIT clause: %s", e)
            # If parsing fails, just add LIMIT if no semicolon at end
            if max_results and not query.strip().endswith(";"):
                if "limit" not in query.lower():
                    query = f"{query} LIMIT {max_results}"

        return query

    def _is_write_query(self, query: str) -> bool:
        """
        Check if a query is a write operation.

        Args:
            query: SQL query to check

        Returns:
            True if query is a write operation
        """
        # Normalize query for checking
        normalized_query = query.strip().upper()

        # Write operation keywords
        write_keywords = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "CREATE",
            "ALTER",
            "TRUNCATE",
            "MERGE",
            "REPLACE",
            "COPY",
            "PUT",
            "GET",
        ]

        return any(normalized_query.startswith(keyword) for keyword in write_keywords)

    def _build_table_reference(
        self, table: str, schema: str = None, database: str = None
    ) -> str:
        """
        Build a fully qualified table reference.

        Args:
            table: Table name
            schema: Schema name (optional)
            database: Database name (optional)

        Returns:
            Fully qualified table reference
        """
        parts = []
        if database:
            parts.append(database)
        if schema:
            parts.append(schema)
        parts.append(table)

        return ".".join(parts)

    def _is_database_allowed(self, database: str) -> bool:
        """
        Check if access to a database is allowed.

        Args:
            database: Database name to check

        Returns:
            True if access is allowed
        """
        allowed_pattern = self.config_data.get("allowed_databases", "*")
        return self._matches_pattern(database, allowed_pattern)

    def _is_schema_allowed(self, schema: str) -> bool:
        """
        Check if access to a schema is allowed.

        Args:
            schema: Schema name to check

        Returns:
            True if access is allowed
        """
        allowed_pattern = self.config_data.get("allowed_schemas", "*")
        return self._matches_pattern(schema, allowed_pattern)

    def _is_table_access_allowed(self, table_ref: str) -> bool:
        """
        Check if access to a table is allowed based on database/schema restrictions.

        Args:
            table_ref: Fully qualified table reference

        Returns:
            True if access is allowed
        """
        parts = table_ref.split(".")

        if len(parts) >= 3:  # database.schema.table
            database = parts[0]
            schema = parts[1]
            return self._is_database_allowed(database) and self._is_schema_allowed(
                schema
            )
        elif len(parts) == 2:  # schema.table
            schema = parts[0]
            return self._is_schema_allowed(schema)
        else:  # just table
            return True  # Allow if no schema/database specified

    def _matches_pattern(self, name: str, pattern: str) -> bool:
        """
        Check if a name matches a pattern (supports wildcards and regex).

        Args:
            name: Name to check
            pattern: Pattern to match against

        Returns:
            True if name matches pattern
        """
        if pattern == "*":
            return True

        # If pattern contains comma, treat as list
        if "," in pattern:
            patterns = [p.strip() for p in pattern.split(",")]
            return any(self._matches_pattern(name, p) for p in patterns)

        # If pattern contains regex special chars, use regex
        if any(char in pattern for char in "^$[]{}()+*?|\\"):
            try:
                return bool(re.match(pattern, name))
            except re.error:
                # If regex is invalid, fall back to simple wildcard
                pass

        # Simple wildcard matching
        pattern = pattern.replace("*", ".*").replace("?", ".")
        try:
            return bool(re.match(f"^{pattern}$", name))
        except re.error:
            return name == pattern

    def run(self, host: str = "0.0.0.0", port: int = None):
        """
        Run the MCP server.

        Args:
            host: Host to bind to
            port: Port to bind to
        """
        if port is None:
            port = self.template_data.get("transport", {}).get("port", 7081)

        self.logger.info("Starting Snowflake MCP Server on %s:%d", host, port)
        self.mcp.run(host=host, port=port)

    def close(self):
        """Close the Snowflake connection."""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("Snowflake connection closed")
            except Exception as e:
                self.logger.error("Error closing connection: %s", e)
            finally:
                self.connection = None


def setup_health_check(server: SnowflakeMCPServer):
    """Set up health check endpoint for the server."""

    @server.mcp.app.get("/health")
    async def health_check(request: Request):
        """Health check endpoint."""
        try:
            # Test database connection
            with server._get_cursor() as cursor:
                cursor.execute("SELECT 1")

            return JSONResponse(
                {
                    "status": "healthy",
                    "service": "snowflake-mcp-server",
                    "version": server.version,
                    "timestamp": time.time(),
                }
            )
        except Exception as e:
            return JSONResponse(
                {
                    "status": "unhealthy",
                    "service": "snowflake-mcp-server",
                    "version": server.version,
                    "error": str(e),
                    "timestamp": time.time(),
                },
                status_code=503,
            )


def main():
    """Main entry point for the server."""
    # Parse any command line arguments or environment variables
    config_dict = {}

    # You can add CLI argument parsing here if needed
    # For now, rely on environment variables and defaults

    # Create and run the server
    server = SnowflakeMCPServer(config_dict=config_dict)
    setup_health_check(server)

    try:
        server.run()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error("Server error: %s", e)
        raise
    finally:
        server.close()


if __name__ == "__main__":
    main()
