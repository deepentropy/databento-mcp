"""Tests for the enhanced cache module."""
import json
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from cache import Cache, CacheInfo


class TestCacheInfo:
    """Tests for CacheInfo dataclass."""

    def test_age_seconds(self):
        """Test age calculation."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now - timedelta(seconds=120),
            expires_at=now + timedelta(seconds=300),
            ttl_seconds=420,
        )
        # Age should be approximately 120 seconds
        assert 115 < info.age_seconds < 125

    def test_expires_in_seconds(self):
        """Test expiration time calculation."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now - timedelta(seconds=60),
            expires_at=now + timedelta(seconds=300),
            ttl_seconds=360,
        )
        # Should expire in approximately 300 seconds
        assert 295 < info.expires_in_seconds < 305

    def test_is_expired_false(self):
        """Test is_expired returns False for valid cache."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now,
            expires_at=now + timedelta(seconds=300),
            ttl_seconds=300,
        )
        assert info.is_expired is False

    def test_is_expired_true(self):
        """Test is_expired returns True for expired cache."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now - timedelta(seconds=600),
            expires_at=now - timedelta(seconds=300),
            ttl_seconds=300,
        )
        assert info.is_expired is True

    def test_format_age_seconds(self):
        """Test age formatting for seconds."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now - timedelta(seconds=30),
            expires_at=now + timedelta(seconds=300),
            ttl_seconds=330,
        )
        assert "seconds ago" in info.format_age()

    def test_format_age_minutes(self):
        """Test age formatting for minutes."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now - timedelta(minutes=5),
            expires_at=now + timedelta(seconds=300),
            ttl_seconds=600,
        )
        assert "minutes ago" in info.format_age()

    def test_format_expires_minutes(self):
        """Test expires formatting for minutes."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now,
            expires_at=now + timedelta(minutes=10),
            ttl_seconds=600,
        )
        assert "minutes" in info.format_expires()

    def test_format_feedback(self):
        """Test complete feedback formatting."""
        now = datetime.now()
        info = CacheInfo(
            value="test",
            cached_at=now - timedelta(minutes=3),
            expires_at=now + timedelta(minutes=7),
            ttl_seconds=600,
        )
        feedback = info.format_feedback()
        assert "[Cached" in feedback
        assert "ago" in feedback
        assert "expires" in feedback


class TestCache:
    """Tests for Cache class with enhanced features."""

    @pytest.fixture
    def temp_cache(self):
        """Create a temporary cache directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Cache(cache_dir=tmpdir, default_ttl=300)

    def test_get_with_force_refresh_returns_none(self, temp_cache):
        """Test that force_refresh bypasses cache."""
        temp_cache.set("key1", "value1")
        
        # Without force_refresh, should return value
        assert temp_cache.get("key1") == "value1"
        
        # With force_refresh, should return None
        assert temp_cache.get("key1", force_refresh=True) is None

    def test_get_with_info_returns_tuple(self, temp_cache):
        """Test get_with_info returns value and info."""
        temp_cache.set("key1", "value1")
        
        value, info = temp_cache.get_with_info("key1")
        assert value == "value1"
        assert info is not None
        assert isinstance(info, CacheInfo)
        assert info.value == "value1"

    def test_get_with_info_force_refresh(self, temp_cache):
        """Test get_with_info with force_refresh."""
        temp_cache.set("key1", "value1")
        
        value, info = temp_cache.get_with_info("key1", force_refresh=True)
        assert value is None
        assert info is None

    def test_get_with_info_missing_key(self, temp_cache):
        """Test get_with_info for missing key."""
        value, info = temp_cache.get_with_info("nonexistent")
        assert value is None
        assert info is None

    def test_get_cache_status_hit(self, temp_cache):
        """Test cache status for valid entry."""
        temp_cache.set("key1", "value1")
        assert temp_cache.get_cache_status("key1") == "hit"

    def test_get_cache_status_miss(self, temp_cache):
        """Test cache status for missing entry."""
        assert temp_cache.get_cache_status("nonexistent") == "miss"

    def test_cache_info_ttl(self, temp_cache):
        """Test that CacheInfo contains correct TTL."""
        temp_cache.set("key1", "value1", ttl=600)
        
        value, info = temp_cache.get_with_info("key1")
        # Allow for small timing differences (TTL could be 599 or 600)
        assert 599 <= info.ttl_seconds <= 600

    def test_set_and_get_basic(self, temp_cache):
        """Test basic set and get operations."""
        temp_cache.set("test_key", {"data": "test_value"})
        result = temp_cache.get("test_key")
        assert result == {"data": "test_value"}

    def test_clear(self, temp_cache):
        """Test cache clearing."""
        temp_cache.set("key1", "value1")
        temp_cache.set("key2", "value2")
        
        temp_cache.clear()
        
        assert temp_cache.get("key1") is None
        assert temp_cache.get("key2") is None

    def test_clear_expired(self, temp_cache):
        """Test clearing only expired entries."""
        # Set one with very short TTL
        temp_cache.set("short_ttl", "value1", ttl=0)  # Already expired
        temp_cache.set("long_ttl", "value2", ttl=3600)
        
        # Wait a moment to ensure the short TTL is definitely expired
        time.sleep(0.1)
        
        temp_cache.clear_expired()
        
        # The expired entry should be gone
        # But the long TTL entry should remain
        assert temp_cache.get("long_ttl") == "value2"
