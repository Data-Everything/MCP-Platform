"""
Unit tests for gateway authentication module.

Tests password hashing, JWT tokens, API key management, and the AuthManager class.
"""

import string
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from mcp_platform.gateway.auth import AuthenticationError, AuthManager
from mcp_platform.gateway.database import DatabaseManager
from mcp_platform.gateway.models import APIKey, AuthConfig, User

pytestmark = pytest.mark.unit


class TestAuthManager:
    """Test AuthManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.auth_config = AuthConfig(secret_key="test_secret_key_123456789")
        self.mock_db = Mock(spec=DatabaseManager)
        self.auth_manager = AuthManager(self.auth_config, self.mock_db)

    def test_auth_manager_initialization(self):
        """Test AuthManager initialization."""
        assert self.auth_manager.config == self.auth_config
        assert self.auth_manager.db == self.mock_db
        assert self.auth_manager.user_crud is not None
        assert self.auth_manager.api_key_crud is not None

    def test_password_hashing(self):
        """Test password hashing methods."""
        password = "test_password123"
        hashed = self.auth_manager.get_password_hash(password)

        # Should be different from original
        assert hashed != password
        # Should be a string
        assert isinstance(hashed, str)
        # Should have reasonable length (bcrypt hashes are ~60 chars)
        assert len(hashed) > 50

        # Test verification
        assert self.auth_manager.verify_password(password, hashed) is True
        assert self.auth_manager.verify_password("wrong_password", hashed) is False

    def test_password_salt_uniqueness(self):
        """Test that same password produces different hashes due to salt."""
        password = "test_password"

        hash1 = self.auth_manager.get_password_hash(password)
        hash2 = self.auth_manager.get_password_hash(password)

        # Different salts should produce different hashes
        assert hash1 != hash2
        # But both should verify correctly
        assert self.auth_manager.verify_password(password, hash1) is True
        assert self.auth_manager.verify_password(password, hash2) is True

    def test_create_access_token(self):
        """Test access token creation."""
        data = {"sub": "testuser", "user_id": 1}

        token = self.auth_manager.create_access_token(data)

        assert isinstance(token, str)
        # JWT tokens have 3 parts separated by dots
        assert len(token.split(".")) == 3

    def test_verify_access_token(self):
        """Test access token verification."""
        data = {"sub": "testuser", "user_id": 1}

        token = self.auth_manager.create_access_token(data)
        payload = self.auth_manager.verify_token(token)

        assert payload is not None
        assert payload["sub"] == "testuser"
        assert payload["user_id"] == 1
        assert "exp" in payload

    def test_verify_invalid_token(self):
        """Test verification of invalid token."""
        invalid_token = "invalid.token.here"

        with pytest.raises(AuthenticationError, match="Invalid token"):
            self.auth_manager.verify_token(invalid_token)

    def test_custom_token_expiration(self):
        """Test token with custom expiration."""
        data = {"sub": "testuser"}
        expires_delta = timedelta(hours=2)

        token = self.auth_manager.create_access_token(data, expires_delta=expires_delta)
        payload = self.auth_manager.verify_token(token)

        assert payload is not None
        # Check that expiration is roughly 2 hours from now
        exp_time = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        expected_exp = datetime.now(timezone.utc) + expires_delta
        # Allow 1 minute tolerance
        assert abs((exp_time - expected_exp).total_seconds()) < 60

    def test_generate_api_key(self):
        """Test API key generation."""
        api_key = self.auth_manager.generate_api_key()

        assert isinstance(api_key, str)
        # Should start with "mcp_"
        assert api_key.startswith("mcp_")
        # Should be reasonably long for security
        assert len(api_key) >= 36  # "mcp_" + 32 chars

    def test_api_key_uniqueness(self):
        """Test that generated API keys are unique."""
        key1 = self.auth_manager.generate_api_key()
        key2 = self.auth_manager.generate_api_key()

        assert key1 != key2

    def test_api_key_hashing(self):
        """Test API key hashing and verification."""
        api_key = "mcp_test_api_key_123"
        hashed = self.auth_manager.hash_api_key(api_key)

        assert hashed != api_key
        assert isinstance(hashed, str)
        assert len(hashed) > 50  # bcrypt hash length

        # Test verification
        assert self.auth_manager.verify_api_key(api_key, hashed) is True
        assert self.auth_manager.verify_api_key("wrong_key", hashed) is False

    @pytest.mark.asyncio
    async def test_authenticate_user_success(self):
        """Test successful user authentication."""
        # Create a user with hashed password
        password = "test_password"
        hashed_password = self.auth_manager.get_password_hash(password)
        mock_user = User(
            id=1, username="testuser", hashed_password=hashed_password, is_active=True
        )

        # Mock the database call
        self.auth_manager.user_crud.get_by_username = AsyncMock(return_value=mock_user)

        result = await self.auth_manager.authenticate_user("testuser", password)

        assert result == mock_user
        self.auth_manager.user_crud.get_by_username.assert_called_once_with("testuser")

    @pytest.mark.asyncio
    async def test_authenticate_user_wrong_password(self):
        """Test user authentication with wrong password."""
        password = "test_password"
        wrong_password = "wrong_password"
        hashed_password = self.auth_manager.get_password_hash(password)
        mock_user = User(
            id=1, username="testuser", hashed_password=hashed_password, is_active=True
        )

        self.auth_manager.user_crud.get_by_username = AsyncMock(return_value=mock_user)

        result = await self.auth_manager.authenticate_user("testuser", wrong_password)

        assert result is None

    @pytest.mark.asyncio
    async def test_authenticate_nonexistent_user(self):
        """Test authentication of non-existent user."""
        self.auth_manager.user_crud.get_by_username = AsyncMock(return_value=None)

        result = await self.auth_manager.authenticate_user("nonexistent", "password")

        assert result is None

    @pytest.mark.asyncio
    async def test_authenticate_api_key_success(self):
        """Test successful API key authentication."""
        # Create a secret and token in the new format mcp_{id}.{secret}
        secret = "secret_value"
        token = "mcp_1." + secret
        hashed_key = self.auth_manager.hash_api_key(secret)
        mock_api_key = APIKey(
            id=1,
            name="Test Key",
            key_hash=hashed_key,
            user_id=1,
            is_active=True,
            expires_at=None,
        )

        # Mock the database calls to return the API key by id
        self.auth_manager.api_key_crud.get_api_key = AsyncMock(
            return_value=mock_api_key
        )
        self.auth_manager.api_key_crud.update = AsyncMock()

        result = await self.auth_manager.authenticate_api_key(token)

        assert result == mock_api_key
        # Verify that last_used was updated
        self.auth_manager.api_key_crud.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_authenticate_expired_api_key(self):
        """Test authentication with expired API key."""
        secret = "secret_value"
        token = "mcp_1." + secret
        hashed_key = self.auth_manager.hash_api_key(secret)
        past_time = datetime.now(timezone.utc) - timedelta(days=1)
        mock_api_key = APIKey(
            id=1,
            name="Expired Key",
            key_hash=hashed_key,
            user_id=1,
            is_active=True,
            expires_at=past_time,
        )

        # Mock the database calls
        self.auth_manager.api_key_crud.get_api_key = AsyncMock(
            return_value=mock_api_key
        )
        self.auth_manager.api_key_crud.update = AsyncMock()

        result = await self.auth_manager.authenticate_api_key(token)

        assert result is None
        # Should not update last_used for expired keys
        self.auth_manager.api_key_crud.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_authenticate_invalid_api_key_format(self):
        """Test authentication with invalid API key format."""
        invalid_key = "invalid_key_format"

        result = await self.auth_manager.authenticate_api_key(invalid_key)

        assert result is None

    @pytest.mark.asyncio
    async def test_authenticate_api_key_not_found(self):
        """Test authentication with API key that doesn't exist."""
        token = "mcp_999.nonexistent"

        # Mock DB returning no record for id
        self.auth_manager.api_key_crud.get_api_key = AsyncMock(return_value=None)

        result = await self.auth_manager.authenticate_api_key(token)

        assert result is None

    @pytest.mark.asyncio
    async def test_authenticate_inactive_api_key(self):
        """Test authentication with inactive API key."""
        secret = "secret_value"
        token = "mcp_1." + secret
        hashed_key = self.auth_manager.hash_api_key(secret)
        mock_api_key = APIKey(
            id=1,
            name="Inactive Key",
            key_hash=hashed_key,
            user_id=1,
            is_active=False,  # Inactive key
            expires_at=None,
        )

        # Mock the database calls
        self.auth_manager.api_key_crud.get_api_key = AsyncMock(
            return_value=mock_api_key
        )

        result = await self.auth_manager.authenticate_api_key(token)

        assert result is None


class TestAuthConfig:
    """Test authentication configuration."""

    def test_valid_auth_config(self):
        """Test valid auth configuration."""
        config = AuthConfig(secret_key="my_secret_key")

        assert config.secret_key == "my_secret_key"
        assert config.algorithm == "HS256"
        assert config.access_token_expire_minutes == 30
        assert config.api_key_expire_days == 30

    def test_auth_config_custom_values(self):
        """Test auth configuration with custom values."""
        config = AuthConfig(
            secret_key="test",
            algorithm="HS512",
            access_token_expire_minutes=60,
            api_key_expire_days=90,
        )

        assert config.algorithm == "HS512"
        assert config.access_token_expire_minutes == 60
        assert config.api_key_expire_days == 90


class TestAuthenticationError:
    """Test AuthenticationError exception."""

    def test_authentication_error_creation(self):
        """Test creating AuthenticationError."""
        error = AuthenticationError("Test error message")
        assert str(error) == "Test error message"

    def test_authentication_error_inheritance(self):
        """Test that AuthenticationError inherits from Exception."""
        error = AuthenticationError("Test")
        assert isinstance(error, Exception)


class TestSecurityBestPractices:
    """Test security best practices."""

    def test_password_hash_strength(self):
        """Test that password hashes are strong."""
        auth_config = AuthConfig(secret_key="test_secret")
        auth_manager = AuthManager(auth_config, Mock())

        password = "test_password"
        hash1 = auth_manager.get_password_hash(password)
        hash2 = auth_manager.get_password_hash(password)

        # Different salts should produce different hashes
        assert hash1 != hash2
        # Both should be long enough
        assert len(hash1) > 50
        assert len(hash2) > 50

    def test_api_key_format(self):
        """Test that API keys follow the expected format."""
        auth_config = AuthConfig(secret_key="test_secret")
        auth_manager = AuthManager(auth_config, Mock())

        api_key = auth_manager.generate_api_key()

        # Should start with "mcp_"
        assert api_key.startswith("mcp_")
        # Should be URL-safe (no special characters that need encoding)

        allowed_chars = string.ascii_letters + string.digits + "-_"
        assert all(c in allowed_chars for c in api_key)

    def test_jwt_includes_expiration(self):
        """Test that JWT tokens include expiration."""
        auth_config = AuthConfig(secret_key="test_secret_key_123456789")
        auth_manager = AuthManager(auth_config, Mock())

        data = {"sub": "testuser"}

        token = auth_manager.create_access_token(data)
        payload = auth_manager.verify_token(token)

        assert "exp" in payload
        assert payload["exp"] > datetime.now(timezone.utc).timestamp()

    def test_secret_key_minimum_length(self):
        """Test that secret keys should be reasonably long."""
        # This test ensures we're using good practices
        short_key = "short"
        good_key = "this_is_a_much_longer_secret_key_for_security"

        # We can still create configs with short keys, but it's not recommended
        config1 = AuthConfig(secret_key=short_key)
        config2 = AuthConfig(secret_key=good_key)

        assert len(config1.secret_key) < 20  # Short key
        assert len(config2.secret_key) > 20  # Better key
