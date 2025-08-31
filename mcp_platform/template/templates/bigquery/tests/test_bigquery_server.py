#!/usr/bin/env python3
"""
Test BigQuery MCP Server Implementation.

Tests the main server functionality including tool registration, query execution,
authentication, and access controls.
"""

import os
import sys
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

# Add the parent directory to sys.path to import server modules
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Mock Google Cloud imports before importing server
mock_bigquery = MagicMock()
mock_service_account = MagicMock()
mock_default = MagicMock()
mock_gcp_exceptions = MagicMock()

# Set up mock exception classes
mock_gcp_exceptions.NotFound = Exception
mock_gcp_exceptions.Forbidden = Exception
mock_gcp_exceptions.BadRequest = Exception

sys.modules["google.cloud"] = MagicMock()
sys.modules["google.cloud.bigquery"] = mock_bigquery
sys.modules["google.oauth2"] = MagicMock()
sys.modules["google.oauth2.service_account"] = mock_service_account
sys.modules["google.auth"] = MagicMock()
sys.modules["google.auth.default"] = mock_default
sys.modules["google.api_core"] = MagicMock()
sys.modules["google.api_core.exceptions"] = mock_gcp_exceptions

# Now import the server
from server import BigQueryMCPServer


class TestBigQueryMCPServer:
    """Test BigQuery MCP Server functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = Mock()
        self.mock_bigquery = mock_bigquery
        self.mock_bigquery.Client.return_value = self.mock_client

        # Basic config for testing
        self.test_config = {
            "project_id": "test-project",
            "auth_method": "application_default",
            "read_only": True,
            "allowed_datasets": "*",
            "query_timeout": 300,
            "max_results": 1000,
        }

    def create_mock_server(self, config=None, **patch_kwargs):
        """
        Helper method to create a properly mocked BigQuery server.

        Args:
            config: Configuration dict to use (defaults to self.test_config)
            **patch_kwargs: Additional patches to apply

        Returns:
            tuple: (server, mock_config, mock_client, patch_context)
        """
        if config is None:
            config = self.test_config

        mock_config_patch = patch("server.BigQueryServerConfig")
        mock_bigquery_patch = patch("server.bigquery")

        patches = [mock_config_patch, mock_bigquery_patch]

        # Add any additional patches
        for patch_path, mock_obj in patch_kwargs.items():
            patches.append(patch(patch_path, mock_obj))

        # Start all patches
        mock_config_class = mock_config_patch.start()
        mock_bigquery_module = mock_bigquery_patch.start()

        # Start additional patches
        additional_mocks = {}
        for i, (patch_path, _) in enumerate(patch_kwargs.items()):
            additional_mocks[patch_path] = patches[2 + i].start()

        # Set up config mock
        mock_config = Mock()
        mock_config.get_template_config.return_value = config
        mock_config.get_template_data.return_value = {
            "name": "test-server",
            "version": "1.0.0",
        }
        mock_config.logger = Mock()
        mock_config_class.return_value = mock_config

        # Set up BigQuery client mock
        mock_client = Mock()
        mock_bigquery_module.Client.return_value = mock_client

        # Create server
        server = BigQueryMCPServer(config_dict=config, skip_validation=True)

        return server, mock_config, mock_client, patches, additional_mocks

    def test_server_initialization(self):
        """Test server initialization with default config."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):

            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            # Create server with skip_validation for tests
            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Verify configuration was loaded
            mock_config_class.assert_called_once()
            assert server.config_data == self.test_config

            # Verify BigQuery client was initialized
            mock_bigquery_module.Client.assert_called_once_with(project="test-project")

    def test_server_initialization_with_service_account(self):
        """Test server initialization with service account authentication."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"type": "service_account", "project_id": "test"}')
            service_account_path = f.name

        try:
            config = self.test_config.copy()
            config.update(
                {
                    "auth_method": "service_account",
                    "service_account_path": service_account_path,
                }
            )

            with (
                patch("server.BigQueryServerConfig") as mock_config_class,
                patch("server.service_account") as mock_service_account_module,
                patch("server.bigquery") as mock_bigquery_module,
            ):

                mock_config = Mock()
                mock_config.get_template_config.return_value = config
                mock_config.get_template_data.return_value = {
                    "name": "test-server",
                    "version": "1.0.0",
                }
                mock_config.logger = Mock()
                mock_config_class.return_value = mock_config

                # Set up mocks
                mock_credentials = Mock()
                mock_service_account_module.Credentials.from_service_account_file.return_value = (
                    mock_credentials
                )
                mock_client = Mock()
                mock_bigquery_module.Client.return_value = mock_client

                BigQueryMCPServer(config_dict=config, skip_validation=True)

                # Verify service account credentials were used
                mock_service_account_module.Credentials.from_service_account_file.assert_called_once_with(
                    service_account_path
                )

        finally:
            os.unlink(service_account_path)

    def test_write_mode_warning(self):
        """Test that write mode displays warning."""
        config = self.test_config.copy()
        config["read_only"] = False

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            with patch("builtins.print"):
                BigQueryMCPServer(config_dict=config, skip_validation=True)

                # Verify warning was logged
                mock_config.logger.warning.assert_called()
                warning_call = mock_config.logger.warning.call_args[0][0]
                assert "WARNING" in warning_call
                assert "write mode" in warning_call.lower()

    def test_is_dataset_allowed_wildcard(self):
        """Test dataset access with wildcard pattern."""
        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # With wildcard, all datasets should be allowed
            assert server._is_dataset_allowed("any_dataset") is True
            assert server._is_dataset_allowed("another_dataset") is True

    def test_is_dataset_allowed_patterns(self):
        """Test dataset access with specific patterns."""
        config = self.test_config.copy()
        config["allowed_datasets"] = "analytics_*,public_data"

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            # Test pattern matching
            assert server._is_dataset_allowed("analytics_prod") is True
            assert server._is_dataset_allowed("analytics_staging") is True
            assert server._is_dataset_allowed("public_data") is True
            assert server._is_dataset_allowed("private_data") is False

    def test_is_dataset_allowed_regex(self):
        """Test dataset access with regex pattern."""
        config = self.test_config.copy()
        config.update(
            {
                "allowed_datasets": "*",  # This should be ignored
                "dataset_regex": "^(prod|staging)_.*$",
            }
        )

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            # Test regex matching (should take precedence over allowed_datasets)
            assert server._is_dataset_allowed("prod_analytics") is True
            assert server._is_dataset_allowed("staging_data") is True
            assert server._is_dataset_allowed("dev_analytics") is False

    def test_check_write_operation(self):
        """Test write operation detection in read-only mode."""
        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Test read operations (should be allowed)
            assert server._check_write_operation("SELECT * FROM table") is False
            assert (
                server._check_write_operation(
                    "WITH cte AS (SELECT 1) SELECT * FROM cte"
                )
                is False
            )

            # Test write operations (should be blocked in read-only mode)
            assert server._check_write_operation("INSERT INTO table VALUES (1)") is True
            assert server._check_write_operation("UPDATE table SET col = 1") is True
            assert (
                server._check_write_operation("DELETE FROM table WHERE id = 1") is True
            )
            assert server._check_write_operation("DROP TABLE table") is True
            assert (
                server._check_write_operation("CREATE TABLE new_table AS SELECT 1")
                is True
            )

    def test_check_write_operation_write_mode(self):
        """Test that write operations are allowed when read_only=False."""
        config = self.test_config.copy()
        config["read_only"] = False

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            # All operations should be allowed in write mode
            assert server._check_write_operation("SELECT * FROM table") is False
            assert (
                server._check_write_operation("INSERT INTO table VALUES (1)") is False
            )
            assert server._check_write_operation("UPDATE table SET col = 1") is False

    def test_list_datasets_success(self):
        """Test successful dataset listing."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):

            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock dataset objects
            mock_dataset1 = Mock()
            mock_dataset1.dataset_id = "analytics_prod"
            mock_dataset1.full_dataset_id = "test-project.analytics_prod"
            mock_dataset1.location = "US"
            mock_dataset1.created = datetime.now()
            mock_dataset1.modified = datetime.now()

            mock_dataset2 = Mock()
            mock_dataset2.dataset_id = "public_data"
            mock_dataset2.full_dataset_id = "test-project.public_data"
            mock_dataset2.location = "EU"
            mock_dataset2.created = datetime.now()
            mock_dataset2.modified = datetime.now()

            mock_client.list_datasets.return_value = [mock_dataset1, mock_dataset2]

            result = server.list_datasets()

            assert result["success"] is True
            assert len(result["datasets"]) == 2
            assert result["total_count"] == 2
            assert result["datasets"][0]["dataset_id"] == "analytics_prod"
            assert result["datasets"][1]["dataset_id"] == "public_data"

    def test_list_datasets_filtered(self):
        """Test dataset listing with access control filtering."""
        config = self.test_config.copy()
        config["allowed_datasets"] = "analytics_*"

        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            # Mock dataset objects
            mock_dataset1 = Mock()
            mock_dataset1.dataset_id = "analytics_prod"
            mock_dataset1.full_dataset_id = "test-project.analytics_prod"

            mock_dataset2 = Mock()
            mock_dataset2.dataset_id = "sensitive_data"
            mock_dataset2.full_dataset_id = "test-project.sensitive_data"

            mock_client.list_datasets.return_value = [mock_dataset1, mock_dataset2]

            result = server.list_datasets()

            # Only analytics_prod should be returned
            assert result["success"] is True
            assert len(result["datasets"]) == 1
            assert result["datasets"][0]["dataset_id"] == "analytics_prod"

    def test_list_datasets_error(self):
        """Test dataset listing error handling."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            mock_client.list_datasets.side_effect = Exception("BigQuery error")

            result = server.list_datasets()

            assert result["success"] is False
            assert "BigQuery error" in result["error"]
            assert result["datasets"] == []

    def test_list_tables_success(self):
        """Test successful table listing."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock table objects
            mock_table1 = Mock()
            mock_table1.table_id = "events"
            mock_table1.full_table_id = "test-project.analytics.events"
            mock_table1.table_type = "TABLE"
            mock_table1.created = datetime.now()
            mock_table1.modified = datetime.now()

            mock_dataset = Mock()
            mock_client.dataset.return_value = mock_dataset
            mock_client.list_tables.return_value = [mock_table1]

            result = server.list_tables("analytics")

            assert result["success"] is True
            assert result["dataset_id"] == "analytics"
            assert len(result["tables"]) == 1
            assert result["tables"][0]["table_id"] == "events"

    def test_list_tables_access_denied(self):
        """Test table listing with access denied."""
        config = self.test_config.copy()
        config["allowed_datasets"] = "public_*"

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            result = server.list_tables("private_data")

            assert result["success"] is False
            assert "not allowed" in result["error"]
            assert result["tables"] == []

    def test_describe_table_success(self):
        """Test successful table description."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock table schema
            mock_field1 = Mock()
            mock_field1.name = "id"
            mock_field1.field_type = "INTEGER"
            mock_field1.mode = "REQUIRED"
            mock_field1.description = "Unique identifier"
            mock_field1.fields = []

            mock_field2 = Mock()
            mock_field2.name = "name"
            mock_field2.field_type = "STRING"
            mock_field2.mode = "NULLABLE"
            mock_field2.description = "User name"
            mock_field2.fields = []

            mock_table = Mock()
            mock_table.full_table_id = "test-project.analytics.events"
            mock_table.table_type = "TABLE"
            mock_table.num_rows = 1000
            mock_table.num_bytes = 50000
            mock_table.created = datetime.now()
            mock_table.modified = datetime.now()
            mock_table.description = "Event tracking table"
            mock_table.schema = [mock_field1, mock_field2]
            mock_table.clustering_fields = ["id"]
            mock_table.time_partitioning = None

            mock_table_ref = Mock()
            mock_client.dataset.return_value.table.return_value = mock_table_ref
            mock_client.get_table.return_value = mock_table

            result = server.describe_table("analytics", "events")

            assert result["success"] is True
            assert result["dataset_id"] == "analytics"
            assert result["table_id"] == "events"
            assert result["num_rows"] == 1000
            assert len(result["schema"]) == 2
            assert result["schema"][0]["name"] == "id"
            assert result["schema"][1]["name"] == "name"

    def test_execute_query_success(self):
        """Test successful query execution."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock query job
            mock_job = Mock()
            mock_job.job_id = "job_123"
            mock_job.total_bytes_processed = 1024
            mock_job.total_bytes_billed = 1024
            mock_job.cache_hit = False

            # Mock query results
            mock_row1 = {"count": 100}
            mock_row2 = {"count": 200}
            mock_results = Mock()
            mock_results.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))

            mock_job.result.return_value = mock_results
            mock_client.query.return_value = mock_job

            result = server.execute_query("SELECT COUNT(*) as count FROM table")

            assert result["success"] is True
            assert result["job_id"] == "job_123"
            assert result["num_rows"] == 2
            assert len(result["rows"]) == 2
            assert result["rows"][0] == {"count": 100}

    def test_execute_query_blocked_in_readonly(self):
        """Test that write queries are blocked in read-only mode."""
        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            result = server.execute_query("INSERT INTO table VALUES (1)")

            assert result["success"] is False
            assert "not allowed in read-only mode" in result["error"]

    def test_execute_query_dry_run(self):
        """Test query dry run functionality."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock dry run job
            mock_job = Mock()
            mock_job.total_bytes_processed = 2048

            mock_client.query.return_value = mock_job

            result = server.execute_query("SELECT * FROM table", dry_run=True)

            assert result["success"] is True
            assert result["dry_run"] is True
            assert result["total_bytes_processed"] == 2048
            assert "would process" in result["message"]

    def test_get_job_status_success(self):
        """Test successful job status retrieval."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock job
            mock_job = Mock()
            mock_job.state = "DONE"
            mock_job.job_type = "QUERY"
            mock_job.created = datetime.now()
            mock_job.started = datetime.now()
            mock_job.ended = datetime.now()
            mock_job.error_result = None
            mock_job.errors = []
            mock_job.user_email = "user@example.com"

            mock_client.get_job.return_value = mock_job

            result = server.get_job_status("job_123")

            assert result["success"] is True
            assert result["job_id"] == "job_123"
            assert result["state"] == "DONE"
            assert result["job_type"] == "QUERY"

    def test_get_dataset_info_success(self):
        """Test successful dataset info retrieval."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock dataset
            mock_dataset = Mock()
            mock_dataset.full_dataset_id = "test-project.analytics"
            mock_dataset.location = "US"
            mock_dataset.description = "Analytics dataset"
            mock_dataset.created = datetime.now()
            mock_dataset.modified = datetime.now()
            mock_dataset.default_table_expiration_ms = None
            mock_dataset.default_partition_expiration_ms = None
            mock_dataset.labels = {"env": "prod"}
            mock_dataset.access_entries = []

            mock_dataset_ref = Mock()
            mock_client.dataset.return_value = mock_dataset_ref
            mock_client.get_dataset.return_value = mock_dataset

            result = server.get_dataset_info("analytics")

            assert result["success"] is True
            assert result["dataset_id"] == "analytics"
            assert result["location"] == "US"
            assert result["description"] == "Analytics dataset"
            assert result["labels"] == {"env": "prod"}

    def test_get_dataset_info_access_denied(self):
        """Test dataset info with access denied."""
        config = self.test_config.copy()
        config["allowed_datasets"] = "public_*"

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            result = server.get_dataset_info("private_data")

            assert result["success"] is False
            assert "not allowed" in result["error"]

    def test_tool_registration(self):
        """Test that all tools are registered with the MCP server."""
        with (
            patch("server.BigQueryServerConfig") as mock_config_class,
            patch("server.bigquery") as mock_bigquery_module,
            patch("server.FastMCP") as mock_fastmcp,
        ):
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            # Set up BigQuery client mock
            mock_client = Mock()
            mock_bigquery_module.Client.return_value = mock_client

            # Set up FastMCP mock
            mock_mcp_instance = Mock()
            mock_fastmcp.return_value = mock_mcp_instance

            BigQueryMCPServer(config_dict=self.test_config, skip_validation=True)

            # Verify that mcp.tool was called for each expected tool
            expected_tools = 6  # list_datasets, list_tables, describe_table, execute_query, get_job_status, get_dataset_info
            assert mock_mcp_instance.tool.call_count == expected_tools

    def test_format_nested_fields(self):
        """Test nested schema field formatting."""
        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = self.test_config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(
                config_dict=self.test_config, skip_validation=True
            )

            # Mock nested fields
            mock_nested_field = Mock()
            mock_nested_field.name = "nested_field"
            mock_nested_field.field_type = "STRING"
            mock_nested_field.mode = "NULLABLE"
            mock_nested_field.description = "Nested field"
            mock_nested_field.fields = []

            mock_parent_field = Mock()
            mock_parent_field.name = "parent_field"
            mock_parent_field.field_type = "RECORD"
            mock_parent_field.mode = "REPEATED"
            mock_parent_field.description = "Parent record"
            mock_parent_field.fields = [mock_nested_field]

            result = server._format_nested_fields([mock_parent_field])

            assert len(result) == 1
            assert result[0]["name"] == "parent_field"
            assert result[0]["field_type"] == "RECORD"
            assert len(result[0]["fields"]) == 1
            assert result[0]["fields"][0]["name"] == "nested_field"

    def test_filter_datasets(self):
        """Test dataset filtering functionality."""
        config = self.test_config.copy()
        config["allowed_datasets"] = "analytics_*"

        with patch("server.BigQueryServerConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_template_config.return_value = config
            mock_config.get_template_data.return_value = {
                "name": "test-server",
                "version": "1.0.0",
            }
            mock_config.logger = Mock()
            mock_config_class.return_value = mock_config

            server = BigQueryMCPServer(config_dict=config, skip_validation=True)

            # Mock datasets
            mock_dataset1 = Mock()
            mock_dataset1.dataset_id = "analytics_prod"

            mock_dataset2 = Mock()
            mock_dataset2.dataset_id = "sensitive_data"

            mock_dataset3 = Mock()
            mock_dataset3.dataset_id = "analytics_staging"

            datasets = [mock_dataset1, mock_dataset2, mock_dataset3]
            filtered = server._filter_datasets(datasets)

            # Only analytics datasets should remain
            assert len(filtered) == 2
            assert filtered[0].dataset_id == "analytics_prod"
            assert filtered[1].dataset_id == "analytics_staging"
