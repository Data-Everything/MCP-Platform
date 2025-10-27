#!/usr/bin/env python3
"""
Snowflake MCP Server Response Formatter.

This module provides response formatting utilities for the Snowflake MCP server
to ensure consistent and well-structured responses across all tools.
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional


class SnowflakeResponseFormatter:
    """Response formatter for Snowflake MCP server tools."""

    def __init__(self):
        """Initialize the response formatter."""
        self.logger = logging.getLogger(__name__)

    def format_database_list(self, databases: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Format database list response.

        Args:
            databases: List of database dictionaries

        Returns:
            Formatted response dictionary
        """
        return {
            "databases": [
                {
                    "name": db.get("name", ""),
                    "owner": db.get("owner", ""),
                    "comment": db.get("comment", ""),
                    "created_on": self._format_timestamp(db.get("created_on")),
                    "retention_time": db.get("retention_time", ""),
                    "options": db.get("options", ""),
                }
                for db in databases
            ],
            "count": len(databases),
        }

    def format_schema_list(
        self, schemas: List[Dict[str, Any]], database: str = None
    ) -> Dict[str, Any]:
        """
        Format schema list response.

        Args:
            schemas: List of schema dictionaries
            database: Database name for context

        Returns:
            Formatted response dictionary
        """
        return {
            "database": database,
            "schemas": [
                {
                    "name": schema.get("name", ""),
                    "database_name": schema.get("database_name", database),
                    "owner": schema.get("owner", ""),
                    "comment": schema.get("comment", ""),
                    "created_on": self._format_timestamp(schema.get("created_on")),
                    "retention_time": schema.get("retention_time", ""),
                    "options": schema.get("options", ""),
                }
                for schema in schemas
            ],
            "count": len(schemas),
        }

    def format_table_list(
        self, tables: List[Dict[str, Any]], database: str = None, schema: str = None
    ) -> Dict[str, Any]:
        """
        Format table list response.

        Args:
            tables: List of table dictionaries
            database: Database name for context
            schema: Schema name for context

        Returns:
            Formatted response dictionary
        """
        return {
            "database": database,
            "schema": schema,
            "tables": [
                {
                    "name": table.get("name", ""),
                    "database_name": table.get("database_name", database),
                    "schema_name": table.get("schema_name", schema),
                    "kind": table.get("kind", ""),
                    "owner": table.get("owner", ""),
                    "comment": table.get("comment", ""),
                    "created_on": self._format_timestamp(table.get("created_on")),
                    "bytes": table.get("bytes"),
                    "rows": table.get("rows"),
                    "clustering_key": table.get("clustering_key", ""),
                    "retention_time": table.get("retention_time", ""),
                    "automatic_clustering": table.get("automatic_clustering", ""),
                    "change_tracking": table.get("change_tracking", ""),
                }
                for table in tables
            ],
            "count": len(tables),
        }

    def format_table_description(
        self,
        columns: List[Dict[str, Any]],
        table: str,
        database: str = None,
        schema: str = None,
    ) -> Dict[str, Any]:
        """
        Format table description response.

        Args:
            columns: List of column dictionaries
            table: Table name
            database: Database name for context
            schema: Schema name for context

        Returns:
            Formatted response dictionary
        """
        return {
            "database": database,
            "schema": schema,
            "table": table,
            "columns": [
                {
                    "name": col.get("name", ""),
                    "type": col.get("type", ""),
                    "kind": col.get("kind", ""),
                    "null?": col.get("null?", ""),
                    "default": col.get("default"),
                    "primary key": col.get("primary key", ""),
                    "unique key": col.get("unique key", ""),
                    "check": col.get("check"),
                    "expression": col.get("expression"),
                    "comment": col.get("comment", ""),
                    "policy name": col.get("policy name", ""),
                    "privacy domain": col.get("privacy domain", ""),
                }
                for col in columns
            ],
            "column_count": len(columns),
        }

    def format_query_results(
        self, cursor, query: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Format query execution results.

        Args:
            cursor: Snowflake cursor with results
            query: Original SQL query
            limit: Optional row limit applied

        Returns:
            Formatted response dictionary
        """
        try:
            # Get column descriptions
            columns = []
            if cursor.description:
                columns = [
                    {
                        "name": desc[0],
                        "type": self._snowflake_type_to_string(desc[1]),
                        "display_size": desc[2],
                        "internal_size": desc[3],
                        "precision": desc[4],
                        "scale": desc[5],
                        "null_ok": desc[6],
                    }
                    for desc in cursor.description
                ]

            # Get all rows and format them
            rows = []
            for row in cursor.fetchall():
                formatted_row = []
                for i, value in enumerate(row):
                    formatted_value = self._format_value(value)
                    formatted_row.append(formatted_value)
                rows.append(formatted_row)

            return {
                "query": query,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "limit_applied": limit,
                "truncated": limit is not None and len(rows) == limit,
            }

        except Exception as e:
            self.logger.error("Error formatting query results: %s", e)
            return {
                "query": query,
                "error": f"Failed to format results: {str(e)}",
                "columns": [],
                "rows": [],
                "row_count": 0,
            }

    def format_explain_results(
        self, explain_rows: List[Dict[str, Any]], query: str
    ) -> Dict[str, Any]:
        """
        Format query explanation results.

        Args:
            explain_rows: List of explanation plan dictionaries
            query: Original SQL query

        Returns:
            Formatted response dictionary
        """
        return {
            "query": query,
            "execution_plan": [
                {
                    "step": row.get("step", ""),
                    "id": row.get("id", ""),
                    "parent": row.get("parent", ""),
                    "operation": row.get("operation", ""),
                    "objects": row.get("objects", ""),
                    "alias": row.get("alias", ""),
                    "expressions": row.get("expressions", ""),
                    "partitions_assigned": row.get("partitions_assigned", ""),
                    "partitions_total": row.get("partitions_total", ""),
                    "bytes_assigned": row.get("bytes_assigned", ""),
                    "bytes_total": row.get("bytes_total", ""),
                    "rows": row.get("rows", ""),
                    "cost": row.get("cost", ""),
                }
                for row in explain_rows
            ],
            "plan_steps": len(explain_rows),
        }

    def format_warehouse_info(self, warehouse_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format warehouse information response.

        Args:
            warehouse_data: Warehouse information dictionary

        Returns:
            Formatted response dictionary
        """
        return {
            "name": warehouse_data.get("name", ""),
            "state": warehouse_data.get("state", ""),
            "type": warehouse_data.get("type", ""),
            "size": warehouse_data.get("size", ""),
            "min_cluster_count": warehouse_data.get("min_cluster_count", ""),
            "max_cluster_count": warehouse_data.get("max_cluster_count", ""),
            "started_clusters": warehouse_data.get("started_clusters", ""),
            "running": warehouse_data.get("running", ""),
            "queued": warehouse_data.get("queued", ""),
            "is_default": warehouse_data.get("is_default", ""),
            "is_current": warehouse_data.get("is_current", ""),
            "auto_suspend": warehouse_data.get("auto_suspend", ""),
            "auto_resume": warehouse_data.get("auto_resume", ""),
            "available": warehouse_data.get("available", ""),
            "provisioning": warehouse_data.get("provisioning", ""),
            "quiescing": warehouse_data.get("quiescing", ""),
            "other": warehouse_data.get("other", ""),
            "created_on": self._format_timestamp(warehouse_data.get("created_on")),
            "resumed_on": self._format_timestamp(warehouse_data.get("resumed_on")),
            "updated_on": self._format_timestamp(warehouse_data.get("updated_on")),
            "owner": warehouse_data.get("owner", ""),
            "comment": warehouse_data.get("comment", ""),
            "enable_query_acceleration": warehouse_data.get(
                "enable_query_acceleration", ""
            ),
            "query_acceleration_max_scale_factor": warehouse_data.get(
                "query_acceleration_max_scale_factor", ""
            ),
        }

    def format_connection_info(self, connection_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format connection information response.

        Args:
            connection_data: Connection information dictionary

        Returns:
            Formatted response dictionary
        """
        return {
            "account": connection_data.get("account", ""),
            "user": connection_data.get("user", ""),
            "database": connection_data.get("database", ""),
            "schema": connection_data.get("schema", ""),
            "warehouse": connection_data.get("warehouse", ""),
            "role": connection_data.get("role", ""),
            "region": connection_data.get("region", ""),
            "authenticator": connection_data.get("authenticator", ""),
            "session_id": connection_data.get("session_id", ""),
            "server_version": connection_data.get("server_version", ""),
            "client_version": connection_data.get("client_version", ""),
            "timezone": connection_data.get("timezone", ""),
        }

    def format_error(self, error: Exception, context: str = "") -> Dict[str, Any]:
        """
        Format error response.

        Args:
            error: Exception that occurred
            context: Additional context about the error

        Returns:
            Formatted error response dictionary
        """
        return {
            "error": True,
            "error_type": error.__class__.__name__,
            "message": str(error),
            "context": context,
        }

    def _format_timestamp(self, timestamp: Any) -> Optional[str]:
        """
        Format timestamp value to ISO string.

        Args:
            timestamp: Timestamp value (datetime, date, or string)

        Returns:
            Formatted timestamp string or None
        """
        if timestamp is None:
            return None

        try:
            if isinstance(timestamp, datetime):
                return timestamp.isoformat()
            elif isinstance(timestamp, date):
                return timestamp.isoformat()
            elif isinstance(timestamp, str):
                return timestamp
            else:
                return str(timestamp)
        except Exception:
            return str(timestamp) if timestamp else None

    def _format_value(self, value: Any) -> Any:
        """
        Format individual query result value.

        Args:
            value: Raw value from query result

        Returns:
            Formatted value suitable for JSON serialization
        """
        if value is None:
            return None
        elif isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, Decimal):
            # Convert Decimal to float for JSON serialization
            return float(value)
        elif isinstance(value, (datetime, date)):
            return self._format_timestamp(value)
        elif isinstance(value, bytes):
            # Convert bytes to hex string
            return value.hex()
        else:
            # Convert other types to string
            return str(value)

    def _snowflake_type_to_string(self, type_code: int) -> str:
        """
        Convert Snowflake type code to string representation.

        Args:
            type_code: Numeric type code from Snowflake

        Returns:
            String representation of the type
        """
        # Snowflake type mapping (based on snowflake.connector.constants)
        type_mapping = {
            0: "NUMBER",
            1: "REAL",
            2: "TEXT",
            3: "DATE",
            4: "TIMESTAMP",
            5: "VARIANT",
            6: "TIMESTAMP_LTZ",
            7: "TIMESTAMP_TZ",
            8: "TIMESTAMP_NTZ",
            9: "OBJECT",
            10: "ARRAY",
            11: "BINARY",
            12: "TIME",
            13: "BOOLEAN",
            14: "GEOGRAPHY",
            15: "GEOMETRY",
        }

        return type_mapping.get(type_code, f"UNKNOWN({type_code})")

    def format_simple_list(
        self, items: List[Any], title: str = "items"
    ) -> Dict[str, Any]:
        """
        Format a simple list response.

        Args:
            items: List of items to format
            title: Title for the list

        Returns:
            Formatted response dictionary
        """
        return {title: [str(item) for item in items], "count": len(items)}
