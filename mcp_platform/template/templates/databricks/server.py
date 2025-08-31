#!/usr/bin/env python3
"""
Databricks MCP Server - Comprehensive Database Integration

A powerful MCP server that enables LLM clients to interact with Databricks
SQL warehouses and clusters. Supports multiple authentication methods,
read-only operations by default, and comprehensive data access controls.
"""

import asyncio
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .config import DatabricksServerConfig
except ImportError:
    try:
        from config import DatabricksServerConfig
    except ImportError:
        # Fallback for Docker or direct script execution
        sys.path.append(os.path.dirname(__file__))
        from config import DatabricksServerConfig

# Databricks SDK imports
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo
    from databricks.sdk.service.compute import ClusterDetails
    from databricks.sdk.service.sql import QueryExecutionState, StatementResponse, StatementState
    from databricks.sdk.service.sql import EndpointInfo as WarehouseInfo
except ImportError as e:
    logger.error("Failed to import Databricks SDK. Install with: pip install databricks-sdk")
    raise ImportError(
        "Databricks SDK is required. Install with: pip install databricks-sdk>=0.18.0"
    ) from e


class DatabricksMCPServer:
    """
    Databricks MCP Server implementation using FastMCP.

    This server provides comprehensive access to Databricks resources including:
    - SQL warehouses and compute clusters
    - Catalogs, schemas, and tables
    - Query execution with read-only safety
    - Pattern-based access filtering
    - Multiple authentication methods
    """

    def __init__(self, config_dict: dict = None):
        """Initialize the Databricks MCP Server with configuration."""
        self.config = DatabricksServerConfig(config_dict=config_dict or {})

        # Standard configuration data from config_schema
        self.config_data = self.config.get_template_config()

        # Full template data (potentially modified by double underscore notation)
        self.template_data = self.config.get_template_data()

        self.logger = self.config.logger

        # Initialize Databricks workspace client
        self.workspace_client = None
        self._initialize_databricks_client()

        self.mcp = FastMCP(
            name=self.template_data.get("name", "databricks-server"),
            instructions="Databricks MCP server for warehouse and cluster access",
            version=self.template_data.get("version", "1.0.0"),
            host=os.getenv("MCP_HOST", "0.0.0.0"),
            port=(
                int(
                    os.getenv(
                        "MCP_PORT",
                        self.template_data.get("transport", {}).get("port", 7072),
                    )
                )
                if not os.getenv("MCP_TRANSPORT") == "stdio"
                else None
            ),
        )
        logger.info("%s MCP server %s created", self.template_data["name"], self.mcp.name)
        self.register_tools()

    def _initialize_databricks_client(self):
        """Initialize the Databricks workspace client with authentication."""
        try:
            # Create Databricks config based on authentication method
            databricks_config = self._create_databricks_config()
            
            # Initialize the workspace client
            self.workspace_client = WorkspaceClient(config=databricks_config)
            
            # Test the connection
            self._test_connection()
            
            self.logger.info("Successfully connected to Databricks workspace")
            
        except Exception as e:
            self.logger.error("Failed to initialize Databricks client: %s", str(e))
            raise

    def _create_databricks_config(self) -> Config:
        """Create Databricks SDK configuration based on auth method."""
        auth_method = self.config_data.get("auth_method", "pat")
        workspace_host = self.config_data.get("workspace_host")
        
        if not workspace_host:
            raise ValueError("workspace_host is required")

        config_kwargs = {
            "host": workspace_host,
            "timeout": self.config_data.get("connection_timeout", 30)
        }

        if auth_method == "pat":
            access_token = self.config_data.get("access_token")
            if not access_token:
                raise ValueError("access_token is required for PAT authentication")
            config_kwargs["token"] = access_token
            
        elif auth_method == "oauth":
            oauth_token = self.config_data.get("oauth_token")
            if not oauth_token:
                raise ValueError("oauth_token is required for OAuth authentication")
            config_kwargs["oauth_token"] = oauth_token
            
        elif auth_method == "username_password":
            username = self.config_data.get("username")
            password = self.config_data.get("password")
            if not username or not password:
                raise ValueError("username and password are required for username/password authentication")
            config_kwargs["username"] = username
            config_kwargs["password"] = password
            
        else:
            raise ValueError(f"Unsupported authentication method: {auth_method}")

        return Config(**config_kwargs)

    def _test_connection(self):
        """Test the Databricks connection."""
        try:
            # Simple test to verify connection works
            current_user = self.workspace_client.current_user.me()
            self.logger.info("Connected as user: %s", current_user.user_name)
        except Exception as e:
            self.logger.error("Connection test failed: %s", str(e))
            raise

    def _check_read_only_mode(self, operation: str) -> None:
        """Check if operation is allowed in read-only mode."""
        if self.config_data.get("read_only", True):
            write_operations = {
                "CREATE", "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", 
                "TRUNCATE", "MERGE", "COPY", "MSCK", "REFRESH"
            }
            
            # Simple check for write operations in SQL
            operation_upper = operation.upper()
            for write_op in write_operations:
                if write_op in operation_upper:
                    raise ValueError(
                        f"Write operation '{write_op}' is not allowed in read-only mode. "
                        f"Set read_only=false to enable write operations (not recommended)."
                    )

    def _match_pattern(self, name: str, patterns: str) -> bool:
        """Check if name matches any of the patterns (supports regex)."""
        if patterns == "*":
            return True
            
        pattern_list = [p.strip() for p in patterns.split(",")]
        
        for pattern in pattern_list:
            try:
                if re.match(pattern, name) or pattern == name:
                    return True
            except re.error:
                # If regex fails, try exact match
                if pattern == name:
                    return True
                    
        return False

    def _filter_databases(self, databases: List[str]) -> List[str]:
        """Filter databases based on allowed patterns."""
        allowed_patterns = self.config_data.get("allowed_databases", "*")
        
        if allowed_patterns == "*":
            return databases
            
        return [db for db in databases if self._match_pattern(db, allowed_patterns)]

    def _filter_schemas(self, schemas: List[str]) -> List[str]:
        """Filter schemas based on allowed patterns."""
        allowed_patterns = self.config_data.get("allowed_schemas", "*")
        
        if allowed_patterns == "*":
            return schemas
            
        return [schema for schema in schemas if self._match_pattern(schema, allowed_patterns)]

    def register_tools(self):
        """Register tools with the MCP server."""
        self.mcp.tool(self.list_clusters, tags=["cluster", "management"])
        self.mcp.tool(self.list_warehouses, tags=["warehouse", "management"])
        self.mcp.tool(self.list_databases, tags=["database", "discovery"])
        self.mcp.tool(self.list_tables, tags=["table", "discovery"])
        self.mcp.tool(self.describe_table, tags=["table", "schema"])
        self.mcp.tool(self.execute_query, tags=["query", "sql"])
        self.mcp.tool(self.get_cluster_info, tags=["cluster", "info"])
        self.mcp.tool(self.get_warehouse_info, tags=["warehouse", "info"])

    async def list_clusters(self) -> Dict[str, Any]:
        """
        List all available Databricks clusters.
        
        Returns:
            Dictionary containing cluster information
        """
        try:
            clusters = list(self.workspace_client.clusters.list())
            
            cluster_list = []
            for cluster in clusters:
                cluster_list.append({
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "state": cluster.state.value if cluster.state else "unknown",
                    "node_type_id": cluster.node_type_id,
                    "num_workers": cluster.num_workers,
                    "spark_version": cluster.spark_version,
                    "runtime_engine": cluster.runtime_engine.value if cluster.runtime_engine else None,
                    "creator_user_name": cluster.creator_user_name
                })
            
            return {
                "clusters": cluster_list,
                "count": len(cluster_list)
            }
            
        except Exception as e:
            self.logger.error("Failed to list clusters: %s", str(e))
            return {"error": f"Failed to list clusters: {str(e)}"}

    async def list_warehouses(self) -> Dict[str, Any]:
        """
        List all available SQL warehouses.
        
        Returns:
            Dictionary containing warehouse information
        """
        try:
            warehouses = list(self.workspace_client.warehouses.list())
            
            warehouse_list = []
            for warehouse in warehouses:
                warehouse_list.append({
                    "warehouse_id": warehouse.id,
                    "warehouse_name": warehouse.name,
                    "state": warehouse.state.value if warehouse.state else "unknown",
                    "cluster_size": warehouse.cluster_size,
                    "min_num_clusters": warehouse.min_num_clusters,
                    "max_num_clusters": warehouse.max_num_clusters,
                    "auto_stop_mins": warehouse.auto_stop_mins,
                    "warehouse_type": warehouse.warehouse_type.value if warehouse.warehouse_type else None
                })
            
            return {
                "warehouses": warehouse_list,
                "count": len(warehouse_list)
            }
            
        except Exception as e:
            self.logger.error("Failed to list warehouses: %s", str(e))
            return {"error": f"Failed to list warehouses: {str(e)}"}

    async def list_databases(self, pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        List all accessible databases in the workspace.
        
        Args:
            pattern: Optional pattern to filter database names
            
        Returns:
            Dictionary containing database information
        """
        try:
            # Get catalogs and schemas
            catalogs = list(self.workspace_client.catalogs.list())
            
            databases = []
            for catalog in catalogs:
                catalog_name = catalog.name
                
                try:
                    schemas = list(self.workspace_client.schemas.list(catalog_name=catalog_name))
                    
                    for schema in schemas:
                        database_name = f"{catalog_name}.{schema.name}"
                        
                        # Apply pattern filter if provided
                        if pattern and not self._match_pattern(database_name, pattern):
                            continue
                            
                        databases.append({
                            "database": database_name,
                            "catalog": catalog_name,
                            "schema": schema.name,
                            "owner": schema.owner,
                            "comment": schema.comment
                        })
                        
                except Exception as e:
                    self.logger.warning("Failed to list schemas for catalog %s: %s", catalog_name, str(e))
                    
            # Apply access filtering
            filtered_databases = self._filter_databases([db["database"] for db in databases])
            
            # Filter the database list to only include allowed ones
            filtered_db_list = [db for db in databases if db["database"] in filtered_databases]
            
            return {
                "databases": filtered_db_list,
                "count": len(filtered_db_list)
            }
            
        except Exception as e:
            self.logger.error("Failed to list databases: %s", str(e))
            return {"error": f"Failed to list databases: {str(e)}"}

    async def list_tables(self, database: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        List tables in a specific database/schema.
        
        Args:
            database: Database name (catalog.schema format)
            schema: Schema name (optional, for backward compatibility)
            
        Returns:
            Dictionary containing table information
        """
        try:
            # Parse database name
            if "." in database and schema is None:
                catalog_name, schema_name = database.split(".", 1)
            else:
                # Legacy format support
                catalog_name = database
                schema_name = schema or "default"
                
            # Check if database is allowed
            full_database_name = f"{catalog_name}.{schema_name}"
            if not self._match_pattern(full_database_name, self.config_data.get("allowed_databases", "*")):
                return {"error": f"Access to database '{full_database_name}' is not allowed"}
                
            # Check if schema is allowed
            if not self._match_pattern(schema_name, self.config_data.get("allowed_schemas", "*")):
                return {"error": f"Access to schema '{schema_name}' is not allowed"}
            
            tables = list(self.workspace_client.tables.list(
                catalog_name=catalog_name, 
                schema_name=schema_name
            ))
            
            table_list = []
            for table in tables:
                table_list.append({
                    "table_name": table.name,
                    "table_type": table.table_type.value if table.table_type else None,
                    "data_source_format": table.data_source_format.value if table.data_source_format else None,
                    "owner": table.owner,
                    "comment": table.comment,
                    "full_name": f"{catalog_name}.{schema_name}.{table.name}"
                })
            
            return {
                "tables": table_list,
                "database": full_database_name,
                "count": len(table_list)
            }
            
        except Exception as e:
            self.logger.error("Failed to list tables: %s", str(e))
            return {"error": f"Failed to list tables: {str(e)}"}

    async def describe_table(self, database: str, table: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get detailed schema information for a specific table.
        
        Args:
            database: Database name
            table: Table name
            schema: Schema name (optional)
            
        Returns:
            Dictionary containing table schema information
        """
        try:
            # Parse database name
            if "." in database and schema is None:
                catalog_name, schema_name = database.split(".", 1)
            else:
                catalog_name = database
                schema_name = schema or "default"
                
            # Check if database is allowed
            full_database_name = f"{catalog_name}.{schema_name}"
            if not self._match_pattern(full_database_name, self.config_data.get("allowed_databases", "*")):
                return {"error": f"Access to database '{full_database_name}' is not allowed"}
            
            # Get table information
            table_info = self.workspace_client.tables.get(
                full_name=f"{catalog_name}.{schema_name}.{table}"
            )
            
            # Get column information
            columns = []
            if table_info.columns:
                for column in table_info.columns:
                    columns.append({
                        "name": column.name,
                        "type_name": column.type_name.value if column.type_name else None,
                        "type_text": column.type_text,
                        "nullable": column.nullable,
                        "comment": column.comment,
                        "position": column.position
                    })
            
            return {
                "table_name": table_info.name,
                "full_name": table_info.full_name,
                "table_type": table_info.table_type.value if table_info.table_type else None,
                "data_source_format": table_info.data_source_format.value if table_info.data_source_format else None,
                "owner": table_info.owner,
                "comment": table_info.comment,
                "columns": columns,
                "column_count": len(columns)
            }
            
        except Exception as e:
            self.logger.error("Failed to describe table: %s", str(e))
            return {"error": f"Failed to describe table: {str(e)}"}

    async def execute_query(
        self, 
        query: str, 
        warehouse_id: Optional[str] = None, 
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query against a Databricks warehouse.
        
        Args:
            query: SQL query to execute
            warehouse_id: SQL warehouse ID to execute the query on
            limit: Maximum number of rows to return
            
        Returns:
            Dictionary containing query results
        """
        try:
            # Check read-only mode
            self._check_read_only_mode(query)
            
            # Apply row limit
            max_rows = limit or self.config_data.get("max_rows", 1000)
            
            # If no warehouse_id specified, try to get the first available warehouse
            if not warehouse_id:
                warehouses = list(self.workspace_client.warehouses.list())
                if not warehouses:
                    return {"error": "No SQL warehouses available"}
                warehouse_id = warehouses[0].id
                
            # Execute the query
            response = self.workspace_client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=warehouse_id,
                wait_timeout="10m"
            )
            
            # Get the results
            result_data = []
            if response.result and response.result.data_array:
                for row in response.result.data_array[:max_rows]:
                    result_data.append(row)
            
            # Get column information
            columns = []
            if response.manifest and response.manifest.schema and response.manifest.schema.columns:
                for col in response.manifest.schema.columns:
                    columns.append({
                        "name": col.name,
                        "type": col.type_name
                    })
            
            return {
                "query": query,
                "warehouse_id": warehouse_id,
                "columns": columns,
                "data": result_data,
                "row_count": len(result_data),
                "limited": len(result_data) >= max_rows,
                "execution_state": response.status.state.value if response.status and response.status.state else "unknown"
            }
            
        except Exception as e:
            self.logger.error("Failed to execute query: %s", str(e))
            return {"error": f"Failed to execute query: {str(e)}"}

    async def get_cluster_info(self, cluster_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific cluster.
        
        Args:
            cluster_id: Cluster ID to get information for
            
        Returns:
            Dictionary containing cluster information
        """
        try:
            cluster = self.workspace_client.clusters.get(cluster_id=cluster_id)
            
            return {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": cluster.state.value if cluster.state else "unknown",
                "state_message": cluster.state_message,
                "node_type_id": cluster.node_type_id,
                "driver_node_type_id": cluster.driver_node_type_id,
                "num_workers": cluster.num_workers,
                "autoscale": {
                    "min_workers": cluster.autoscale.min_workers if cluster.autoscale else None,
                    "max_workers": cluster.autoscale.max_workers if cluster.autoscale else None
                } if cluster.autoscale else None,
                "spark_version": cluster.spark_version,
                "runtime_engine": cluster.runtime_engine.value if cluster.runtime_engine else None,
                "creator_user_name": cluster.creator_user_name,
                "start_time": cluster.start_time,
                "default_tags": dict(cluster.default_tags) if cluster.default_tags else {},
                "custom_tags": dict(cluster.custom_tags) if cluster.custom_tags else {}
            }
            
        except Exception as e:
            self.logger.error("Failed to get cluster info: %s", str(e))
            return {"error": f"Failed to get cluster info: {str(e)}"}

    async def get_warehouse_info(self, warehouse_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific SQL warehouse.
        
        Args:
            warehouse_id: Warehouse ID to get information for
            
        Returns:
            Dictionary containing warehouse information
        """
        try:
            warehouse = self.workspace_client.warehouses.get(id=warehouse_id)
            
            return {
                "warehouse_id": warehouse.id,
                "warehouse_name": warehouse.name,
                "state": warehouse.state.value if warehouse.state else "unknown",
                "cluster_size": warehouse.cluster_size,
                "min_num_clusters": warehouse.min_num_clusters,
                "max_num_clusters": warehouse.max_num_clusters,
                "auto_stop_mins": warehouse.auto_stop_mins,
                "auto_resume": warehouse.auto_resume,
                "warehouse_type": warehouse.warehouse_type.value if warehouse.warehouse_type else None,
                "channel": warehouse.channel.value if warehouse.channel else None,
                "creator_name": warehouse.creator_name,
                "tags": dict(warehouse.tags.as_dict()) if warehouse.tags else {},
                "spot_instance_policy": warehouse.spot_instance_policy.value if warehouse.spot_instance_policy else None,
                "enable_photon": warehouse.enable_photon,
                "enable_serverless_compute": warehouse.enable_serverless_compute
            }
            
        except Exception as e:
            self.logger.error("Failed to get warehouse info: %s", str(e))
            return {"error": f"Failed to get warehouse info: {str(e)}"}

    def run(self):
        """Run the MCP server."""
        if os.getenv("MCP_TRANSPORT") == "stdio":
            self.mcp.run_stdio()
        else:
            self.mcp.run()


# Create the server instance
server = DatabricksMCPServer(config_dict={})


@server.mcp.custom_route(path="/health", methods=["GET"])
async def health_check(request: Request):
    """
    Health check endpoint to verify server status.
    """
    try:
        # Test Databricks connection
        current_user = server.workspace_client.current_user.me()
        
        return JSONResponse({
            "status": "healthy",
            "server": "databricks-mcp",
            "version": server.template_data.get("version", "1.0.0"),
            "databricks_user": current_user.user_name,
            "read_only_mode": server.config_data.get("read_only", True),
            "timestamp": asyncio.get_event_loop().time()
        })
    except Exception as e:
        return JSONResponse({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": asyncio.get_event_loop().time()
        }, status_code=500)


if __name__ == "__main__":
    server.run()