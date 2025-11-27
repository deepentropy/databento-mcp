"""Simple file-based cache for API responses."""
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


class Cache:
    """Simple file-based cache with expiration."""

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

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from the cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found or expired
        """
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

