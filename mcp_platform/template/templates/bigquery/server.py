#!/usr/bin/env python3
"""
BigQuery MCP Server - Production-ready implementation.

A secure BigQuery MCP server that provides controlled access to Google BigQuery
datasets with configurable authentication, read-only mode, and dataset filtering.
"""

import fnmatch
import logging
import os
import re
import sys
from typing import Any, Dict, List

from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .config import BigQueryServerConfig
except ImportError:
    try:
        from config import BigQueryServerConfig
    except ImportError:
        # Fallback for Docker or direct script execution
        sys.path.append(os.path.dirname(__file__))
        from config import BigQueryServerConfig

# Google Cloud BigQuery imports
try:
    from google.api_core import exceptions as gcp_exceptions
    from google.auth import default
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError:
    # Check if we're in test mode (pytest running or test modules imported)
    if "pytest" in sys.modules or any("test" in module for module in sys.modules):
        # In test mode, create mock objects
        import types

        bigquery = types.ModuleType("bigquery")
        bigquery.Client = type("MockClient", (), {})
        gcp_exceptions = types.ModuleType("exceptions")
        gcp_exceptions.NotFound = Exception
        gcp_exceptions.Forbidden = Exception
        gcp_exceptions.BadRequest = Exception

        def default():
            return (None, None)

        service_account = types.ModuleType("service_account")
        service_account.Credentials = type("MockCredentials", (), {})
    else:
        logger.error(
            "Google Cloud BigQuery client libraries are not installed. "
            "Please install with: pip install google-cloud-bigquery"
        )
        sys.exit(1)


class BigQueryMCPServer:
    """
    BigQuery MCP Server implementation using FastMCP.

    This server provides secure access to BigQuery datasets with:
    - Multiple authentication methods (service account, OAuth2, application default)
    - Read-only mode enforcement with configurable override
    - Dataset access controls via patterns and regex
    - Comprehensive query execution and schema inspection tools
    """

    def __init__(self, config_dict: dict = None, skip_validation: bool = False):
        """Initialize the BigQuery MCP Server with configuration."""
        self._skip_validation = skip_validation
        self.config = BigQueryServerConfig(
            config_dict=config_dict or {}, skip_validation=skip_validation
        )

        # Standard configuration data from config_schema
        self.config_data = self.config.get_template_config()

        # Full template data (potentially modified by double underscore notation)
        self.template_data = self.config.get_template_data()

        self.logger = self.config.logger

        # Initialize BigQuery client
        self.client = None
        self._initialize_bigquery_client()

        # Validate read-only mode warning
        if not self.config_data.get("read_only", True):
            warning_msg = "⚠️  WARNING: BigQuery write mode is ENABLED! This allows data modifications and is potentially unsafe."
            self.logger.warning(warning_msg)
            print(f"\n{warning_msg}\n")

        self.mcp = FastMCP(
            name=self.template_data.get("name", "bigquery-server"),
            instructions="BigQuery MCP server for secure dataset querying and schema inspection",
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
        self.logger.info(
            "%s MCP server %s created", self.template_data["name"], self.mcp.name
        )
        self.register_tools()

    def _initialize_bigquery_client(self):
        """Initialize the BigQuery client with the configured authentication method."""
        auth_method = self.config_data.get("auth_method", "application_default")
        project_id = self.config_data.get("project_id")

        if not project_id:
            raise ValueError(
                "project_id is required for BigQuery client initialization"
            )

        try:
            if auth_method == "service_account":
                service_account_path = self.config_data.get("service_account_path")
                # Skip file validation in test mode (when using skip_validation)
                if hasattr(self, "_skip_validation") and self._skip_validation:
                    # In test mode, use mock credentials
                    credentials = service_account.Credentials.from_service_account_file(
                        service_account_path or "/fake/path.json"
                    )
                else:
                    if not service_account_path or not os.path.exists(
                        service_account_path
                    ):
                        raise ValueError(
                            f"Service account key file not found: {service_account_path}"
                        )
                    credentials = service_account.Credentials.from_service_account_file(
                        service_account_path
                    )

                self.client = bigquery.Client(
                    credentials=credentials, project=project_id
                )
                self.logger.info(
                    "BigQuery client initialized with service account authentication"
                )

            elif auth_method == "oauth2":
                # For OAuth2, we rely on the user having set up credentials via gcloud auth
                credentials, _ = default()
                self.client = bigquery.Client(
                    credentials=credentials, project=project_id
                )
                self.logger.info(
                    "BigQuery client initialized with OAuth2 authentication"
                )

            elif auth_method == "application_default":
                # Use Application Default Credentials
                self.client = bigquery.Client(project=project_id)
                self.logger.info(
                    "BigQuery client initialized with application default credentials"
                )

            else:
                raise ValueError(f"Unsupported authentication method: {auth_method}")

        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def _is_dataset_allowed(self, dataset_id: str) -> bool:
        """Check if a dataset is allowed based on configuration filters."""
        # Check regex filter first (takes precedence)
        dataset_regex = self.config_data.get("dataset_regex")
        if dataset_regex:
            try:
                return bool(re.match(dataset_regex, dataset_id))
            except re.error as e:
                self.logger.warning(f"Invalid regex pattern '{dataset_regex}': {e}")
                return False

        # Check allowed_datasets patterns
        allowed_datasets = self.config_data.get("allowed_datasets", "*")
        if allowed_datasets == "*":
            return True

        patterns = [pattern.strip() for pattern in allowed_datasets.split(",")]
        return any(fnmatch.fnmatch(dataset_id, pattern) for pattern in patterns)

    def _filter_datasets(
        self, datasets: List[bigquery.DatasetListItem]
    ) -> List[bigquery.DatasetListItem]:
        """Filter datasets based on access control configuration."""
        return [ds for ds in datasets if self._is_dataset_allowed(ds.dataset_id)]

    def _check_write_operation(self, query: str) -> bool:
        """Check if a query contains write operations."""
        if self.config_data.get("read_only", True):
            # Simple check for write operations
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
            ]
            query_upper = query.upper()
            for keyword in write_keywords:
                if keyword in query_upper:
                    return True
        return False

    def register_tools(self):
        """Register tools with the MCP server."""
        self.mcp.tool(self.list_datasets, tags=["datasets", "discovery"])
        self.mcp.tool(self.list_tables, tags=["tables", "discovery"])
        self.mcp.tool(self.describe_table, tags=["schema", "metadata"])
        self.mcp.tool(self.execute_query, tags=["query", "sql"])
        self.mcp.tool(self.get_job_status, tags=["jobs", "status"])
        self.mcp.tool(self.get_dataset_info, tags=["datasets", "metadata"])

    def list_datasets(self) -> Dict[str, Any]:
        """List all accessible BigQuery datasets in the project."""
        try:
            datasets = list(self.client.list_datasets())
            filtered_datasets = self._filter_datasets(datasets)

            result = []
            for dataset in filtered_datasets:
                result.append(
                    {
                        "dataset_id": dataset.dataset_id,
                        "full_dataset_id": dataset.full_dataset_id,
                        "location": getattr(dataset, "location", None),
                        "creation_time": getattr(dataset, "created", None),
                        "last_modified_time": getattr(dataset, "modified", None),
                    }
                )

            return {
                "success": True,
                "datasets": result,
                "total_count": len(result),
                "message": f"Found {len(result)} accessible datasets",
            }

        except Exception as e:
            self.logger.error(f"Error listing datasets: {e}")
            return {"success": False, "error": str(e), "datasets": []}

    def list_tables(self, dataset_id: str) -> Dict[str, Any]:
        """List tables in a specific dataset."""
        if not self._is_dataset_allowed(dataset_id):
            return {
                "success": False,
                "error": f"Access to dataset '{dataset_id}' is not allowed",
                "tables": [],
            }

        try:
            dataset_ref = self.client.dataset(dataset_id)
            tables = list(self.client.list_tables(dataset_ref))

            result = []
            for table in tables:
                result.append(
                    {
                        "table_id": table.table_id,
                        "full_table_id": table.full_table_id,
                        "table_type": table.table_type,
                        "creation_time": getattr(table, "created", None),
                        "last_modified_time": getattr(table, "modified", None),
                    }
                )

            return {
                "success": True,
                "dataset_id": dataset_id,
                "tables": result,
                "total_count": len(result),
                "message": f"Found {len(result)} tables in dataset '{dataset_id}'",
            }

        except Exception as e:
            self.logger.error(f"Error listing tables in dataset '{dataset_id}': {e}")
            return {"success": False, "error": str(e), "tables": []}

    def describe_table(self, dataset_id: str, table_id: str) -> Dict[str, Any]:
        """Get detailed schema information for a table."""
        if not self._is_dataset_allowed(dataset_id):
            return {
                "success": False,
                "error": f"Access to dataset '{dataset_id}' is not allowed",
            }

        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = self.client.get_table(table_ref)

            schema_fields = []
            for field in table.schema:
                field_info = {
                    "name": field.name,
                    "field_type": field.field_type,
                    "mode": field.mode,
                    "description": field.description or "",
                }
                if field.fields:  # Nested/repeated fields
                    field_info["fields"] = self._format_nested_fields(field.fields)
                schema_fields.append(field_info)

            return {
                "success": True,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "full_table_id": table.full_table_id,
                "table_type": table.table_type,
                "num_rows": table.num_rows,
                "num_bytes": table.num_bytes,
                "creation_time": table.created,
                "last_modified_time": table.modified,
                "description": table.description or "",
                "schema": schema_fields,
                "clustering_fields": table.clustering_fields,
                "partitioning": (
                    str(table.time_partitioning) if table.time_partitioning else None
                ),
            }

        except Exception as e:
            self.logger.error(f"Error describing table '{dataset_id}.{table_id}': {e}")
            return {"success": False, "error": str(e)}

    def _format_nested_fields(self, fields) -> List[Dict[str, Any]]:
        """Format nested schema fields recursively."""
        result = []
        for field in fields:
            field_info = {
                "name": field.name,
                "field_type": field.field_type,
                "mode": field.mode,
                "description": field.description or "",
            }
            if field.fields:
                field_info["fields"] = self._format_nested_fields(field.fields)
            result.append(field_info)
        return result

    def execute_query(self, query: str, dry_run: bool = False) -> Dict[str, Any]:
        """Execute a SQL query against BigQuery."""
        # Check for write operations in read-only mode
        if self._check_write_operation(query):
            return {
                "success": False,
                "error": "Write operations are not allowed in read-only mode",
                "query": query,
            }

        try:
            job_config = bigquery.QueryJobConfig()
            job_config.dry_run = dry_run
            job_config.use_query_cache = True

            # Set query timeout and max results from config
            timeout = self.config_data.get("query_timeout", 300)
            max_results = self.config_data.get("max_results", 1000)

            if not dry_run:
                job_config.maximum_bytes_billed = 1000000000  # 1GB limit for safety

            query_job = self.client.query(query, job_config=job_config)

            if dry_run:
                return {
                    "success": True,
                    "dry_run": True,
                    "query": query,
                    "total_bytes_processed": query_job.total_bytes_processed,
                    "message": "Query is valid and would process {} bytes".format(
                        query_job.total_bytes_processed or 0
                    ),
                }

            # Wait for query completion with timeout
            results = query_job.result(timeout=timeout, max_results=max_results)

            # Convert results to list of dictionaries
            rows = []
            for row in results:
                rows.append(dict(row))

            return {
                "success": True,
                "query": query,
                "job_id": query_job.job_id,
                "total_bytes_processed": query_job.total_bytes_processed,
                "total_bytes_billed": query_job.total_bytes_billed,
                "cache_hit": query_job.cache_hit,
                "num_rows": len(rows),
                "rows": rows[:max_results],  # Ensure we don't exceed max_results
                "truncated": len(rows) >= max_results,
            }

        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return {"success": False, "error": str(e), "query": query}

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a BigQuery job."""
        try:
            job = self.client.get_job(job_id)

            return {
                "success": True,
                "job_id": job_id,
                "state": job.state,
                "job_type": job.job_type,
                "created": job.created,
                "started": job.started,
                "ended": job.ended,
                "error_result": job.error_result,
                "errors": job.errors,
                "user_email": job.user_email,
            }

        except Exception as e:
            self.logger.error(f"Error getting job status for '{job_id}': {e}")
            return {"success": False, "error": str(e), "job_id": job_id}

    def get_dataset_info(self, dataset_id: str) -> Dict[str, Any]:
        """Get detailed information about a dataset."""
        if not self._is_dataset_allowed(dataset_id):
            return {
                "success": False,
                "error": f"Access to dataset '{dataset_id}' is not allowed",
            }

        try:
            dataset_ref = self.client.dataset(dataset_id)
            dataset = self.client.get_dataset(dataset_ref)

            return {
                "success": True,
                "dataset_id": dataset_id,
                "full_dataset_id": dataset.full_dataset_id,
                "location": dataset.location,
                "description": dataset.description or "",
                "creation_time": dataset.created,
                "last_modified_time": dataset.modified,
                "default_table_expiration_ms": dataset.default_table_expiration_ms,
                "default_partition_expiration_ms": dataset.default_partition_expiration_ms,
                "labels": dict(dataset.labels) if dataset.labels else {},
                "access_entries": (
                    len(dataset.access_entries) if dataset.access_entries else 0
                ),
            }

        except Exception as e:
            self.logger.error(f"Error getting dataset info for '{dataset_id}': {e}")
            return {"success": False, "error": str(e), "dataset_id": dataset_id}

    def run(self):
        """Run the BigQuery MCP server."""
        self.logger.info("Starting BigQuery MCP server...")
        self.mcp.run()


def create_server(config_dict: dict = None) -> BigQueryMCPServer:
    """Create and configure a BigQuery MCP server instance."""
    return BigQueryMCPServer(config_dict=config_dict or {})


def setup_health_check(server_instance: BigQueryMCPServer):
    """Set up health check endpoint for the server."""

    @server_instance.mcp.custom_route(path="/health", methods=["GET"])
    async def health_check(request: Request):
        """
        Health check endpoint to verify server status.
        """
        try:
            # Test BigQuery connection
            list(server_instance.client.list_datasets(max_results=1))

            return JSONResponse(
                {
                    "status": "healthy",
                    "server": "BigQuery MCP Server",
                    "version": server_instance.template_data.get("version", "1.0.0"),
                    "bigquery_connection": "ok",
                    "read_only_mode": server_instance.config_data.get(
                        "read_only", True
                    ),
                    "project_id": server_instance.config_data.get("project_id"),
                    "auth_method": server_instance.config_data.get(
                        "auth_method", "application_default"
                    ),
                }
            )
        except Exception as e:
            return JSONResponse(
                {
                    "status": "unhealthy",
                    "error": str(e),
                    "server": "BigQuery MCP Server",
                },
                status_code=503,
            )


async def health_check(request: Request):
    """
    Standalone health check function for testing purposes.
    """
    return JSONResponse(
        {
            "status": "healthy",
            "server": "BigQuery MCP Server",
            "version": "1.0.0",
            "bigquery_connection": "ok",
            "test_mode": True,
        }
    )


if __name__ == "__main__":
    # Only create server when running as main module
    server = create_server()
    setup_health_check(server)
    server.run()
