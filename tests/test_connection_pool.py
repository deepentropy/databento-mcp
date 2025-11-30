"""Tests for the connection pool module."""
import os
import threading
from unittest.mock import MagicMock, patch


from databento_mcp.connection_pool import DatabentoConnectionPool, get_pool


class TestDatabentoConnectionPool:
    """Tests for DatabentoConnectionPool class."""

    def setup_method(self):
        """Reset singleton before each test."""
        DatabentoConnectionPool._instance = None

    def test_singleton_instance(self):
        """Test that get_instance returns singleton."""
        pool1 = DatabentoConnectionPool.get_instance()
        pool2 = DatabentoConnectionPool.get_instance()
        assert pool1 is pool2

    def test_get_pool_returns_singleton(self):
        """Test that get_pool returns the singleton instance."""
        pool1 = get_pool()
        pool2 = get_pool()
        assert pool1 is pool2
        assert pool1 is DatabentoConnectionPool.get_instance()

    @patch("databento_mcp.connection_pool.db.Historical")
    def test_get_historical_client_creates_once(self, mock_historical):
        """Test that historical client is created only once."""
        mock_client = MagicMock()
        mock_historical.return_value = mock_client

        pool = get_pool()
        client1 = pool.get_historical_client()
        client2 = pool.get_historical_client()

        assert client1 is client2
        assert mock_historical.call_count == 1

    @patch("databento_mcp.connection_pool.db.Live")
    def test_get_live_client_creates_new_each_time(self, mock_live):
        """Test that live client is created new each time."""
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_live.side_effect = [mock_client1, mock_client2]

        pool = get_pool()
        client1 = pool.get_live_client()
        client2 = pool.get_live_client()

        assert client1 is not client2
        assert mock_live.call_count == 2

    @patch("databento_mcp.connection_pool.db.Historical")
    def test_reset_clears_historical_client(self, mock_historical):
        """Test that reset clears the historical client."""
        mock_client1 = MagicMock()
        mock_client2 = MagicMock()
        mock_historical.side_effect = [mock_client1, mock_client2]

        pool = get_pool()
        client1 = pool.get_historical_client()
        pool.reset()
        client2 = pool.get_historical_client()

        assert client1 is not client2
        assert mock_historical.call_count == 2

    def test_thread_safety_singleton(self):
        """Test that singleton is thread-safe."""
        instances = []
        errors = []

        def get_instance():
            try:
                instance = DatabentoConnectionPool.get_instance()
                instances.append(instance)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=get_instance) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(instances) == 10
        # All instances should be the same
        assert all(i is instances[0] for i in instances)

    @patch.dict(os.environ, {"DATABENTO_API_KEY": "test_key"})
    def test_uses_environment_api_key(self):
        """Test that pool uses DATABENTO_API_KEY from environment."""
        pool = get_pool()
        assert pool._api_key == "test_key"
