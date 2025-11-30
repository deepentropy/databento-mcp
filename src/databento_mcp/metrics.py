"""Metrics collection module for the Databento MCP server.

Provides telemetry and performance metrics collection for monitoring
tool calls, cache performance, and API usage.
"""
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class ToolMetrics:
    """Metrics for a single tool."""

    call_count: int = 0
    success_count: int = 0
    error_count: int = 0
    total_latency_ms: float = 0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0
    latencies: List[float] = field(default_factory=list)

    @property
    def avg_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        return self.total_latency_ms / self.call_count if self.call_count > 0 else 0

    @property
    def p95_latency_ms(self) -> float:
        """Calculate 95th percentile latency in milliseconds."""
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]

    @property
    def p99_latency_ms(self) -> float:
        """Calculate 99th percentile latency in milliseconds."""
        if not self.latencies:
            return 0
        sorted_latencies = sorted(self.latencies)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]


class MetricsCollector:
    """Collects and reports metrics for the MCP server."""

    _instance: Optional["MetricsCollector"] = None
    _lock = threading.Lock()

    def __init__(self):
        """Initialize the metrics collector."""
        self._metrics: Dict[str, ToolMetrics] = defaultdict(ToolMetrics)
        self._cache_hits = 0
        self._cache_misses = 0
        self._api_calls = 0
        self._start_time = datetime.now()
        self._metrics_lock = threading.Lock()
        self._enabled = os.getenv("DATABENTO_METRICS_ENABLED", "true").lower() == "true"
        # Keep only last 1000 latencies per tool to limit memory
        self._max_latencies = 1000

    @classmethod
    def get_instance(cls) -> "MetricsCollector":
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def record_call(self, tool_name: str, latency_ms: float, success: bool):
        """Record a tool call."""
        if not self._enabled:
            return

        with self._metrics_lock:
            metrics = self._metrics[tool_name]
            metrics.call_count += 1
            if success:
                metrics.success_count += 1
            else:
                metrics.error_count += 1
            metrics.total_latency_ms += latency_ms
            metrics.min_latency_ms = min(metrics.min_latency_ms, latency_ms)
            metrics.max_latency_ms = max(metrics.max_latency_ms, latency_ms)

            # Keep bounded latency history
            if len(metrics.latencies) >= self._max_latencies:
                metrics.latencies.pop(0)
            metrics.latencies.append(latency_ms)

    def record_cache_hit(self):
        """Record a cache hit."""
        if self._enabled:
            with self._metrics_lock:
                self._cache_hits += 1

    def record_cache_miss(self):
        """Record a cache miss."""
        if self._enabled:
            with self._metrics_lock:
                self._cache_misses += 1

    def record_api_call(self):
        """Record an API call."""
        if self._enabled:
            with self._metrics_lock:
                self._api_calls += 1

    def get_summary(self) -> Dict:
        """Get metrics summary."""
        with self._metrics_lock:
            uptime = (datetime.now() - self._start_time).total_seconds()
            cache_total = self._cache_hits + self._cache_misses

            return {
                "uptime_seconds": uptime,
                "total_api_calls": self._api_calls,
                "cache": {
                    "hits": self._cache_hits,
                    "misses": self._cache_misses,
                    "hit_rate": self._cache_hits / cache_total if cache_total > 0 else 0,
                },
                "tools": {
                    name: {
                        "calls": m.call_count,
                        "successes": m.success_count,
                        "errors": m.error_count,
                        "success_rate": m.success_count / m.call_count if m.call_count > 0 else 0,
                        "latency_ms": {
                            "avg": round(m.avg_latency_ms, 2),
                            "min": round(m.min_latency_ms, 2)
                            if m.min_latency_ms != float("inf")
                            else 0,
                            "max": round(m.max_latency_ms, 2),
                            "p95": round(m.p95_latency_ms, 2),
                            "p99": round(m.p99_latency_ms, 2),
                        },
                    }
                    for name, m in self._metrics.items()
                },
            }

    def reset(self):
        """Reset all metrics."""
        with self._metrics_lock:
            self._metrics.clear()
            self._cache_hits = 0
            self._cache_misses = 0
            self._api_calls = 0
            self._start_time = datetime.now()


def get_metrics() -> MetricsCollector:
    """Global accessor for the metrics collector."""
    return MetricsCollector.get_instance()


class TimedOperation:
    """Context manager for timing tool calls."""

    def __init__(self, tool_name: str):
        """Initialize timed operation for a tool."""
        self.tool_name = tool_name
        self.start_time: Optional[float] = None
        self.success = True

    def __enter__(self):
        """Start timing the operation."""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timing and record the call."""
        latency_ms = (time.time() - self.start_time) * 1000
        self.success = exc_type is None
        get_metrics().record_call(self.tool_name, latency_ms, self.success)
        return False  # Don't suppress exceptions
