"""Simple file-based cache for API responses with enhanced feedback."""
import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Tuple


@dataclass
class CacheInfo:
    """Information about a cached item."""

    value: Any
    cached_at: datetime
    expires_at: datetime
    ttl_seconds: int

    @property
    def age_seconds(self) -> float:
        """Get the age of the cache entry in seconds."""
        return (datetime.now() - self.cached_at).total_seconds()

    @property
    def expires_in_seconds(self) -> float:
        """Get seconds until expiration."""
        return max(0, (self.expires_at - datetime.now()).total_seconds())

    @property
    def is_expired(self) -> bool:
        """Check if the cache entry is expired."""
        return datetime.now() > self.expires_at

    def format_age(self) -> str:
        """Format cache age as human-readable string."""
        age = self.age_seconds
        if age < 60:
            return f"{int(age)} seconds ago"
        elif age < 3600:
            return f"{int(age / 60)} minutes ago"
        elif age < 86400:
            return f"{int(age / 3600)} hours ago"
        else:
            return f"{int(age / 86400)} days ago"

    def format_expires(self) -> str:
        """Format expiration time as human-readable string."""
        expires = self.expires_in_seconds
        if expires <= 0:
            return "expired"
        elif expires < 60:
            return f"expires in {int(expires)} seconds"
        elif expires < 3600:
            return f"expires in {int(expires / 60)} minutes"
        elif expires < 86400:
            return f"expires in {int(expires / 3600)} hours"
        else:
            return f"expires in {int(expires / 86400)} days"

    def format_feedback(self) -> str:
        """Generate complete cache feedback string."""
        return f"[Cached {self.format_age()} | {self.format_expires()}]"


class Cache:
    """Simple file-based cache with expiration and enhanced feedback."""

    def __init__(self, cache_dir: str = "cache", default_ttl: int = 3600):
        """
        Initialize the cache.

        Args:
            cache_dir: Directory to store cache files
            default_ttl: Default time-to-live in seconds (default: 1 hour)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.default_ttl = default_ttl

    def _get_cache_key(self, key: str) -> str:
        """Generate a cache key hash."""
        return hashlib.sha256(key.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the path to a cache file."""
        return self.cache_dir / f"{cache_key}.json"

    def get(self, key: str, force_refresh: bool = False) -> Optional[Any]:
        """
        Retrieve a value from the cache.

        Args:
            key: Cache key
            force_refresh: If True, bypass cache and return None

        Returns:
            Cached value or None if not found, expired, or force_refresh is True
        """
        if force_refresh:
            return None

        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            return None

        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)

            # Check if expired
            expires_at = datetime.fromisoformat(cache_data['expires_at'])
            if datetime.now() > expires_at:
                cache_path.unlink()  # Delete expired cache
                return None

            return cache_data['value']
        except (json.JSONDecodeError, KeyError, ValueError):
            # Invalid cache file, delete it
            cache_path.unlink(missing_ok=True)
            return None

    def get_with_info(self, key: str, force_refresh: bool = False) -> Tuple[Optional[Any], Optional[CacheInfo]]:
        """
        Retrieve a value from the cache with metadata.

        Args:
            key: Cache key
            force_refresh: If True, bypass cache and return None

        Returns:
            Tuple of (cached_value, CacheInfo) or (None, None) if not found
        """
        if force_refresh:
            return None, None

        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            return None, None

        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)

            expires_at = datetime.fromisoformat(cache_data['expires_at'])
            created_at = datetime.fromisoformat(cache_data['created_at'])

            # Check if expired
            if datetime.now() > expires_at:
                cache_path.unlink()  # Delete expired cache
                return None, None

            # Calculate TTL from timestamps
            ttl = int((expires_at - created_at).total_seconds())

            cache_info = CacheInfo(
                value=cache_data['value'],
                cached_at=created_at,
                expires_at=expires_at,
                ttl_seconds=ttl,
            )

            return cache_data['value'], cache_info
        except (json.JSONDecodeError, KeyError, ValueError):
            # Invalid cache file, delete it
            cache_path.unlink(missing_ok=True)
            return None, None

    def get_cache_status(self, key: str) -> str:
        """
        Get cache status for a key without retrieving the value.

        Args:
            key: Cache key

        Returns:
            Status string: "hit", "miss", or "expired"
        """
        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            return "miss"

        try:
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)

            expires_at = datetime.fromisoformat(cache_data['expires_at'])
            if datetime.now() > expires_at:
                return "expired"

            return "hit"
        except (json.JSONDecodeError, KeyError, ValueError):
            return "miss"

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in the cache.

        Args:
            key: Cache key
            value: Value to cache (must be JSON serializable)
            ttl: Time-to-live in seconds (uses default if not specified)
        """
        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        if ttl is None:
            ttl = self.default_ttl

        expires_at = datetime.now() + timedelta(seconds=ttl)

        cache_data = {
            'value': value,
            'expires_at': expires_at.isoformat(),
            'created_at': datetime.now().isoformat()
        }

        with open(cache_path, 'w') as f:
            json.dump(cache_data, f, indent=2)

    def clear(self) -> None:
        """Clear all cached data."""
        for cache_file in self.cache_dir.glob("*.json"):
            cache_file.unlink()

    def delete(self, key: str) -> bool:
        """
        Delete a specific cache entry.

        Args:
            key: Cache key to delete

        Returns:
            True if the entry was deleted, False if it didn't exist
        """
        cache_key = self._get_cache_key(key)
        cache_path = self._get_cache_path(cache_key)

        if cache_path.exists():
            cache_path.unlink()
            return True
        return False

    def clear_expired(self) -> None:
        """Clear expired cache entries."""
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)

                expires_at = datetime.fromisoformat(cache_data['expires_at'])
                if datetime.now() > expires_at:
                    cache_file.unlink()
            except (json.JSONDecodeError, KeyError, ValueError):
                cache_file.unlink(missing_ok=True)


