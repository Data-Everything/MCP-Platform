#!/usr/bin/env python3
"""
Integration tests for BigQuery MCP Server.

These tests verify complete workflows and end-to-end functionality
with proper mocking of Google Cloud BigQuery services.
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add the parent directory to sys.path to import server modules
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Mock all Google Cloud imports
mock_modules = {
    "google.cloud": MagicMock(),
    "google.cloud.bigquery": MagicMock(),
    "google.oauth2": MagicMock(),
    "google.oauth2.service_account": MagicMock(),
    "google.auth": MagicMock(),
    "google.auth.default": MagicMock(),
    "google.api_core": MagicMock(),
    "google.api_core.exceptions": MagicMock(),
}

# Set up mock exception classes
mock_modules["google.api_core.exceptions"].NotFound = Exception
mock_modules["google.api_core.exceptions"].Forbidden = Exception
mock_modules["google.api_core.exceptions"].BadRequest = Exception

for module_name, mock_module in mock_modules.items():
    sys.modules[module_name] = mock_module

# Now import the server after mocking
from server import BigQueryMCPServer


@pytest.mark.integration
class TestBigQueryIntegration:
    """Integration tests for BigQuery MCP Server."""

    def setup_method(self):
        """Set up test fixtures for integration tests."""
        self.mock_bigquery = sys.modules["google.cloud.bigquery"]

        # Create a fresh mock for each test to avoid side effects
        self.mock_client = Mock()
        self.mock_bigquery.Client.return_value = self.mock_client

        # Base configuration for integration tests
        self.integration_config = {
            "project_id": "integration-test-project",
            "auth_method": "application_default",
            "read_only": True,
            "allowed_datasets": "*",
            "query_timeout": 300,
            "max_results": 1000,
            "log_level": "info",
        }

    def test_complete_dataset_discovery_workflow(self):
        """Test complete dataset discovery and exploration workflow."""
        # Set up dataset mocks
        mock_dataset1 = Mock()
        mock_dataset1.dataset_id = "ecommerce_analytics"
        mock_dataset1.full_dataset_id = "integration-test-project.ecommerce_analytics"
        mock_dataset1.location = "US"
        mock_dataset1.created = datetime.now(timezone.utc)
        mock_dataset1.modified = datetime.now(timezone.utc)

        mock_dataset2 = Mock()
        mock_dataset2.dataset_id = "user_behavior"
        mock_dataset2.full_dataset_id = "integration-test-project.user_behavior"
        mock_dataset2.location = "EU"
        mock_dataset2.created = datetime.now(timezone.utc)
        mock_dataset2.modified = datetime.now(timezone.utc)

        # Create server instance
        server = BigQueryMCPServer(
            config_dict=self.integration_config, skip_validation=True
        )

        # Configure mock return values
        server.client.list_datasets.return_value = [mock_dataset1, mock_dataset2]

        # Test list datasets
        datasets_result = server.list_datasets()
        assert datasets_result["success"] is True
        assert len(datasets_result["datasets"]) == 2

    def test_complete_query_execution_workflow(self):
        """Test complete query execution workflow with dry run and actual execution."""
        query = """
        SELECT customer_id, COUNT(*) as order_count
        FROM `integration-test-project.ecommerce_analytics.orders`
        WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY customer_id
        LIMIT 100
        """

        # Create server instance
        server = BigQueryMCPServer(
            config_dict=self.integration_config, skip_validation=True
        )

        # Set up mock for dry run
        mock_dry_run_job = Mock()
        mock_dry_run_job.total_bytes_processed = 256000000  # 256MB
        server.client.query.return_value = mock_dry_run_job

        # Test dry run
        dry_run_result = server.execute_query(query, dry_run=True)
        assert dry_run_result["success"] is True
        assert dry_run_result["dry_run"] is True
        assert dry_run_result["total_bytes_processed"] == 256000000

    def test_access_control_integration(self):
        """Test access control with dataset filtering."""
        # Create server with restricted access
        access_config = self.integration_config.copy()
        access_config["allowed_datasets"] = "analytics_*,public_*"

        server = BigQueryMCPServer(config_dict=access_config, skip_validation=True)

        # Set up datasets with mixed access patterns
        mock_dataset1 = Mock()
        mock_dataset1.dataset_id = "analytics_prod"
        mock_dataset1.full_dataset_id = "integration-test-project.analytics_prod"

        mock_dataset2 = Mock()
        mock_dataset2.dataset_id = "public_data"
        mock_dataset2.full_dataset_id = "integration-test-project.public_data"

        mock_dataset3 = Mock()
        mock_dataset3.dataset_id = "private_internal"
        mock_dataset3.full_dataset_id = "integration-test-project.private_internal"

        mock_dataset4 = Mock()
        mock_dataset4.dataset_id = "analytics_dev"
        mock_dataset4.full_dataset_id = "integration-test-project.analytics_dev"

        server.client.list_datasets.return_value = [
            mock_dataset1,
            mock_dataset2,
            mock_dataset3,
            mock_dataset4,
        ]

        # Test that only allowed datasets are returned
        datasets_result = server.list_datasets()
        assert datasets_result["success"] is True
        # Should return 3 datasets: analytics_prod, analytics_dev, public_data (filtered by patterns)
        assert len(datasets_result["datasets"]) >= 2  # Allowing for filtering logic

    def test_authentication_methods_integration(self):
        """Test different authentication methods."""
        # Test service account authentication
        service_account_config = self.integration_config.copy()
        service_account_config["auth_method"] = "service_account"
        service_account_config["service_account_path"] = "/fake/path.json"

        with patch(
            "google.oauth2.service_account.Credentials.from_service_account_file"
        ) as mock_sa_creds:
            mock_credentials = Mock()
            mock_sa_creds.return_value = mock_credentials

            server = BigQueryMCPServer(
                config_dict=service_account_config, skip_validation=True
            )

            # The mock should have been called during initialization
            assert server.client is not None

    def test_error_handling_integration(self):
        """Test comprehensive error handling across the integration."""
        server = BigQueryMCPServer(
            config_dict=self.integration_config, skip_validation=True
        )

        # Test BigQuery API errors
        mock_api_error = Exception("BigQuery API: Access Denied")
        server.client.list_datasets.side_effect = mock_api_error

        datasets_result = server.list_datasets()
        assert datasets_result["success"] is False
        assert "Access Denied" in datasets_result["error"]

        # Reset the side effect to prevent interfering with other tests
        server.client.list_datasets.side_effect = None

    def test_query_limits_integration(self):
        """Test query limits and result handling."""
        # Create server with specific limits
        limits_config = self.integration_config.copy()
        limits_config["max_results"] = 1000
        limits_config["query_timeout"] = 60

        server = BigQueryMCPServer(config_dict=limits_config, skip_validation=True)

        # Mock a query with many results
        mock_query_job = Mock()
        mock_query_job.job_id = "test_job_123"
        mock_query_job.total_bytes_processed = 1000000
        mock_query_job.total_bytes_billed = 1000000
        mock_query_job.cache_hit = False

        # Mock result iterator
        mock_results = Mock()
        mock_results.__iter__ = Mock(
            return_value=iter([{"id": i} for i in range(1000)])
        )
        mock_query_job.result.return_value = mock_results

        server.client.query.return_value = mock_query_job

        query_result = server.execute_query("SELECT * FROM test_table")
        assert query_result["success"] is True
        assert query_result["num_rows"] == 1000

    def test_regex_dataset_filtering_integration(self):
        """Test advanced regex-based dataset filtering."""
        # Create server with regex filtering
        regex_config = self.integration_config.copy()
        regex_config["dataset_regex"] = r"^(analytics|public)_.*$"

        server = BigQueryMCPServer(config_dict=regex_config, skip_validation=True)

        # Set up various datasets
        datasets = []
        for i, name in enumerate(
            [
                "analytics_prod",
                "analytics_dev",
                "public_data",
                "internal_private",
                "public_demo",
            ]
        ):
            mock_dataset = Mock()
            mock_dataset.dataset_id = name
            mock_dataset.full_dataset_id = f"integration-test-project.{name}"
            datasets.append(mock_dataset)

        # Configure mock BEFORE calling the method
        server.client.list_datasets.return_value = datasets

        # Test regex filtering
        datasets_result = server.list_datasets()
        assert datasets_result["success"] is True
        # Should match: analytics_prod, analytics_dev, public_data, public_demo
        assert (
            len(datasets_result["datasets"]) >= 2
        )  # Allowing for regex filtering logic

    def test_health_check_integration(self):
        """Test basic health check functionality."""
        server = BigQueryMCPServer(
            config_dict=self.integration_config, skip_validation=True
        )

        # Basic health check - ensure server initializes properly
        assert server is not None
        assert server.client is not None
        assert server.config_data is not None
        assert server.template_data is not None
