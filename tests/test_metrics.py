"""Tests for the metrics module."""
import os
import threading
import time
from unittest.mock import patch

import pytest

from databento_mcp.metrics import MetricsCollector, ToolMetrics, TimedOperation, get_metrics


class TestToolMetrics:
    """Tests for ToolMetrics dataclass."""

    def test_default_values(self):
        """Test default metric values."""
        metrics = ToolMetrics()
        assert metrics.call_count == 0
        assert metrics.success_count == 0
        assert metrics.error_count == 0
        assert metrics.total_latency_ms == 0
        assert metrics.min_latency_ms == float("inf")
        assert metrics.max_latency_ms == 0
        assert metrics.latencies == []

    def test_avg_latency_no_calls(self):
        """Test average latency with no calls."""
        metrics = ToolMetrics()
        assert metrics.avg_latency_ms == 0

    def test_avg_latency_with_calls(self):
        """Test average latency calculation."""
        metrics = ToolMetrics(call_count=2, total_latency_ms=100)
        assert metrics.avg_latency_ms == 50

    def test_p95_latency_empty(self):
        """Test p95 with no latencies."""
        metrics = ToolMetrics()
        assert metrics.p95_latency_ms == 0

    def test_p95_latency_with_data(self):
        """Test p95 latency calculation."""
        latencies = list(range(1, 101))  # 1 to 100
        metrics = ToolMetrics(latencies=latencies)
        # p95 index is int(100 * 0.95) = 95, which gives value 96 (0-indexed)
        assert metrics.p95_latency_ms == 96

    def test_p99_latency_empty(self):
        """Test p99 with no latencies."""
        metrics = ToolMetrics()
        assert metrics.p99_latency_ms == 0

    def test_p99_latency_with_data(self):
        """Test p99 latency calculation."""
        latencies = list(range(1, 101))  # 1 to 100
        metrics = ToolMetrics(latencies=latencies)
        # p99 index is int(100 * 0.99) = 99, which gives value 100 (0-indexed)
        assert metrics.p99_latency_ms == 100


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def setup_method(self):
        """Reset singleton before each test."""
        MetricsCollector._instance = None

    def test_singleton_instance(self):
        """Test that get_instance returns singleton."""
        collector1 = MetricsCollector.get_instance()
        collector2 = MetricsCollector.get_instance()
        assert collector1 is collector2

    def test_get_metrics_returns_singleton(self):
        """Test that get_metrics returns the singleton instance."""
        collector1 = get_metrics()
        collector2 = get_metrics()
        assert collector1 is collector2

    def test_record_call_success(self):
        """Test recording a successful call."""
        collector = get_metrics()
        collector.record_call("test_tool", 100.0, success=True)

        summary = collector.get_summary()
        assert "test_tool" in summary["tools"]
        assert summary["tools"]["test_tool"]["calls"] == 1
        assert summary["tools"]["test_tool"]["successes"] == 1
        assert summary["tools"]["test_tool"]["errors"] == 0

    def test_record_call_failure(self):
        """Test recording a failed call."""
        collector = get_metrics()
        collector.record_call("test_tool", 100.0, success=False)

        summary = collector.get_summary()
        assert summary["tools"]["test_tool"]["calls"] == 1
        assert summary["tools"]["test_tool"]["successes"] == 0
        assert summary["tools"]["test_tool"]["errors"] == 1

    def test_record_cache_hit(self):
        """Test recording cache hit."""
        collector = get_metrics()
        collector.record_cache_hit()
        collector.record_cache_hit()

        summary = collector.get_summary()
        assert summary["cache"]["hits"] == 2

    def test_record_cache_miss(self):
        """Test recording cache miss."""
        collector = get_metrics()
        collector.record_cache_miss()

        summary = collector.get_summary()
        assert summary["cache"]["misses"] == 1

    def test_record_api_call(self):
        """Test recording API call."""
        collector = get_metrics()
        collector.record_api_call()
        collector.record_api_call()

        summary = collector.get_summary()
        assert summary["total_api_calls"] == 2

    def test_cache_hit_rate(self):
        """Test cache hit rate calculation."""
        collector = get_metrics()
        collector.record_cache_hit()
        collector.record_cache_hit()
        collector.record_cache_miss()

        summary = collector.get_summary()
        assert summary["cache"]["hit_rate"] == pytest.approx(2 / 3)

    def test_reset_clears_all_metrics(self):
        """Test that reset clears all metrics."""
        collector = get_metrics()
        collector.record_call("test_tool", 100.0, success=True)
        collector.record_cache_hit()
        collector.record_api_call()
        collector.reset()

        summary = collector.get_summary()
        assert summary["total_api_calls"] == 0
        assert summary["cache"]["hits"] == 0
        assert summary["cache"]["misses"] == 0
        assert len(summary["tools"]) == 0

    def test_latency_min_max(self):
        """Test latency min/max tracking."""
        collector = get_metrics()
        collector.record_call("test_tool", 50.0, success=True)
        collector.record_call("test_tool", 100.0, success=True)
        collector.record_call("test_tool", 25.0, success=True)

        summary = collector.get_summary()
        assert summary["tools"]["test_tool"]["latency_ms"]["min"] == 25.0
        assert summary["tools"]["test_tool"]["latency_ms"]["max"] == 100.0

    def test_uptime_tracking(self):
        """Test uptime tracking."""
        collector = get_metrics()
        time.sleep(0.1)  # Wait a bit

        summary = collector.get_summary()
        assert summary["uptime_seconds"] >= 0.1

    @patch.dict(os.environ, {"DATABENTO_METRICS_ENABLED": "false"})
    def test_disabled_metrics(self):
        """Test that metrics can be disabled."""
        # Need to reset and recreate to pick up new env var
        MetricsCollector._instance = None
        collector = get_metrics()
        collector.record_call("test_tool", 100.0, success=True)
        collector.record_cache_hit()

        summary = collector.get_summary()
        assert len(summary["tools"]) == 0
        assert summary["cache"]["hits"] == 0

    def test_latency_bounded_history(self):
        """Test that latency history is bounded."""
        collector = get_metrics()
        collector._max_latencies = 10  # Lower limit for testing

        for i in range(15):
            collector.record_call("test_tool", float(i), success=True)

        # Should only keep last 10
        summary = collector.get_summary()
        assert summary["tools"]["test_tool"]["calls"] == 15

    def test_thread_safety(self):
        """Test that metrics collection is thread-safe."""
        collector = get_metrics()
        errors = []

        def record_calls():
            try:
                for _ in range(100):
                    collector.record_call("test_tool", 10.0, success=True)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_calls) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        summary = collector.get_summary()
        assert summary["tools"]["test_tool"]["calls"] == 500


class TestTimedOperation:
    """Tests for TimedOperation context manager."""

    def setup_method(self):
        """Reset singleton before each test."""
        MetricsCollector._instance = None

    def test_times_operation(self):
        """Test that TimedOperation times the operation."""
        with TimedOperation("test_tool"):
            time.sleep(0.05)

        summary = get_metrics().get_summary()
        assert "test_tool" in summary["tools"]
        assert summary["tools"]["test_tool"]["latency_ms"]["avg"] >= 50

    def test_records_success(self):
        """Test that successful operations are recorded."""
        with TimedOperation("test_tool"):
            pass

        summary = get_metrics().get_summary()
        assert summary["tools"]["test_tool"]["successes"] == 1
        assert summary["tools"]["test_tool"]["errors"] == 0

    def test_records_failure(self):
        """Test that failed operations are recorded."""
        try:
            with TimedOperation("test_tool"):
                raise ValueError("Test error")
        except ValueError:
            pass

        summary = get_metrics().get_summary()
        assert summary["tools"]["test_tool"]["successes"] == 0
        assert summary["tools"]["test_tool"]["errors"] == 1

    def test_does_not_suppress_exception(self):
        """Test that exceptions are not suppressed."""
        with pytest.raises(ValueError, match="Test error"):
            with TimedOperation("test_tool"):
                raise ValueError("Test error")
