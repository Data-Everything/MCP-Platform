"""
Snowflake template specific test configuration.
"""
import sys
from unittest.mock import Mock

# Mock all external dependencies before pytest tries to import anything
def mock_external_dependencies():
    """Mock external dependencies to allow testing without installing them."""
    
    # Mock FastMCP and related
    mock_fastmcp = Mock()
    sys.modules['fastmcp'] = mock_fastmcp
    
    # Mock Snowflake
    mock_snowflake = Mock()
    mock_snowflake.connector = Mock()
    sys.modules['snowflake'] = mock_snowflake
    sys.modules['snowflake.connector'] = mock_snowflake.connector
    
    # Mock Starlette
    sys.modules['starlette'] = Mock()
    sys.modules['starlette.requests'] = Mock()
    sys.modules['starlette.responses'] = Mock()
    
    # Mock Cryptography
    sys.modules['cryptography'] = Mock()
    sys.modules['cryptography.hazmat'] = Mock()
    sys.modules['cryptography.hazmat.primitives'] = Mock()
    sys.modules['cryptography.hazmat.primitives.serialization'] = Mock()

# Apply mocking immediately
mock_external_dependencies()