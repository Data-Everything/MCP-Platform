#!/usr/bin/env python3
"""
Isolated test runner for Snowflake MCP Server tests.
This runs the tests without pytest interference from parent directories.
"""

import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch

# Add current directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Mock all external dependencies
mock_fastmcp = Mock()
mock_snowflake = Mock()
mock_snowflake.connector = Mock()

sys.modules['fastmcp'] = mock_fastmcp
sys.modules['snowflake'] = mock_snowflake
sys.modules['snowflake.connector'] = mock_snowflake.connector
sys.modules['starlette'] = Mock()
sys.modules['starlette.requests'] = Mock()
sys.modules['starlette.responses'] = Mock()
sys.modules['cryptography'] = Mock()
sys.modules['cryptography.hazmat'] = Mock()
sys.modules['cryptography.hazmat.primitives'] = Mock()
sys.modules['cryptography.hazmat.primitives.serialization'] = Mock()

# Import test modules
from tests.test_server import TestSnowflakeMCPServer
import traceback

def run_test(test_class, method_name):
    """Run a single test method."""
    print(f"Running {test_class.__name__}.{method_name}...")
    try:
        # Create test instance
        test_instance = test_class()
        
        # Set up fixtures manually
        mock_connector = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        # Set up default realistic mock data
        default_databases = [
            ("2023-01-01 00:00:00", "PROD_DB", "owner", "Production database"),
            ("2023-01-01 00:00:00", "DEV_DB", "owner", "Development database"),
            ("2023-01-01 00:00:00", "TEST_DB", "owner", "Test database"),
        ]
        mock_cursor.fetchall.return_value = default_databases
        mock_cursor.fetchmany.return_value = default_databases[:1]
        mock_cursor.description = [("created_on",), ("name",), ("owner",), ("comment",)]
        mock_cursor.execute = Mock()
        mock_cursor.fetchone = Mock()
        mock_cursor.close = Mock()
        
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.is_closed.return_value = False
        mock_connection.close = Mock()
        
        mock_connector.connect.return_value = mock_connection
        
        basic_config = {
            "snowflake_account": "testaccount",
            "snowflake_user": "testuser",
            "snowflake_password": "testpass",
            "snowflake_warehouse": "TEST_WH",
        }
        
        # Run the test method - note that the test method will set up its own mock data if needed
        with patch('server.snowflake.connector', mock_connector):
            with patch('server.FastMCP'):
                # Create mock fixture function for the test
                def mock_snowflake_connector_fixture():
                    return (mock_connector, mock_connection, mock_cursor)
                
                # Create mock fastmcp fixture 
                def mock_fastmcp_fixture():
                    return (Mock(), Mock())
                
                method = getattr(test_instance, method_name)
                method(mock_snowflake_connector_fixture(), basic_config)
                
        print(f"✓ {method_name} PASSED")
        return True
        
    except Exception as e:
        print(f"✗ {method_name} FAILED: {e}")
        traceback.print_exc()
        return False

def main():
    """Run selected tests."""
    print("Running Snowflake MCP Server Tests")
    print("=" * 50)
    
    test_methods = [
        'test_server_initialization',
        'test_read_only_warning',
        'test_filter_pattern_compilation',
        'test_connection_creation_username_password',
        'test_list_databases',
        'test_execute_query',
    ]
    
    passed = 0
    total = len(test_methods)
    
    for method in test_methods:
        if run_test(TestSnowflakeMCPServer, method):
            passed += 1
        print()
    
    print("=" * 50)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed!")
        return 0
    else:
        print("✗ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())