"""Connection pool for Databento clients.

Provides singleton connection pooling for better performance by reusing
Databento client instances across tool calls.
"""
import os
import threading
from typing import Optional

import databento as db


class DatabentoConnectionPool:
    """
    Singleton connection pool for Databento clients.
    Reuses connections across tool calls for better performance.
    """

    _instance: Optional["DatabentoConnectionPool"] = None
    _lock = threading.Lock()

    def __init__(self):
        """Initialize the connection pool."""
        self._api_key = os.getenv("DATABENTO_API_KEY")
        self._historical_client: Optional[db.Historical] = None
        self._client_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "DatabentoConnectionPool":
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_historical_client(self) -> db.Historical:
        """Get or create Historical client."""
        if self._historical_client is None:
            with self._client_lock:
                if self._historical_client is None:
                    self._historical_client = db.Historical(self._api_key)
        return self._historical_client

    def get_live_client(self) -> db.Live:
        """Create new Live client (Live clients are not reusable after stop)."""
        return db.Live(key=self._api_key)

    def reset(self):
        """Reset all connections (useful for error recovery)."""
        with self._client_lock:
            self._historical_client = None


def get_pool() -> DatabentoConnectionPool:
    """Global accessor for the connection pool."""
    return DatabentoConnectionPool.get_instance()
