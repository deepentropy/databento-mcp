"""Tests for the query_warnings module."""
import pytest
from query_warnings import (
    estimate_query_size,
    check_query_warnings,
    generate_alternatives,
    format_query_warning,
    estimate_date_range_days,
    generate_explain_output,
    SCHEMA_RECORD_SIZES,
    WARNING_THRESHOLDS,
)


class TestEstimateQuerySize:
    """Tests for estimate_query_size function."""

    def test_trades_estimate(self):
        """Test size estimation for trades schema."""
        result = estimate_query_size(1000, "trades")
        expected_bytes = 1000 * SCHEMA_RECORD_SIZES["trades"]
        assert result["record_count"] == 1000
        assert result["estimated_bytes"] == expected_bytes
        assert result["estimated_mb"] == expected_bytes / (1024 * 1024)

    def test_mbp10_estimate(self):
        """Test size estimation for mbp-10 schema (larger records)."""
        result = estimate_query_size(1000, "mbp-10")
        expected_bytes = 1000 * SCHEMA_RECORD_SIZES["mbp-10"]
        assert result["estimated_bytes"] == expected_bytes

    def test_unknown_schema_uses_default(self):
        """Test unknown schema uses default record size."""
        result = estimate_query_size(1000, "unknown_schema")
        assert result["estimated_bytes"] == 1000 * 64  # Default size


class TestCheckQueryWarnings:
    """Tests for check_query_warnings function."""

    def test_no_warnings_small_query(self):
        """Test no warnings for small query."""
        warnings = check_query_warnings(
            record_count=1000,
            size_bytes=1024 * 1024,  # 1 MB
            cost_usd=0.50,
        )
        assert len(warnings) == 0

    def test_record_count_warning(self):
        """Test warning for high record count."""
        warnings = check_query_warnings(
            record_count=2_000_000,  # Above threshold
            size_bytes=1024 * 1024,
            cost_usd=0.50,
        )
        assert len(warnings) >= 1
        assert any("records" in w.lower() for w in warnings)

    def test_size_warning(self):
        """Test warning for large data size."""
        warnings = check_query_warnings(
            record_count=1000,
            size_bytes=200 * 1024 * 1024,  # 200 MB, above threshold
            cost_usd=0.50,
        )
        assert len(warnings) >= 1
        assert any("MB" in w for w in warnings)

    def test_cost_warning(self):
        """Test warning for high estimated cost."""
        warnings = check_query_warnings(
            record_count=1000,
            size_bytes=1024 * 1024,
            cost_usd=50.00,  # Above threshold
        )
        assert len(warnings) >= 1
        assert any("cost" in w.lower() for w in warnings)


class TestGenerateAlternatives:
    """Tests for generate_alternatives function."""

    def test_aggregated_data_suggestion(self):
        """Test suggestion to use aggregated data."""
        alternatives = generate_alternatives(
            record_count=500_000,
            schema="trades",
            date_range_days=1,
        )
        assert any("aggregated" in alt.lower() for alt in alternatives)

    def test_batch_job_suggestion(self):
        """Test suggestion to use batch jobs."""
        alternatives = generate_alternatives(
            record_count=5_000_000,
            schema="trades",
            date_range_days=1,
        )
        assert any("batch" in alt.lower() for alt in alternatives)

    def test_smaller_date_range_suggestion(self):
        """Test suggestion to use smaller date ranges."""
        alternatives = generate_alternatives(
            record_count=1000,
            schema="trades",
            date_range_days=60,  # Two months
        )
        assert any("smaller date range" in alt.lower() for alt in alternatives)


class TestFormatQueryWarning:
    """Tests for format_query_warning function."""

    def test_no_warning_returns_none(self):
        """Test that small queries return None."""
        result = format_query_warning(
            record_count=100,
            size_bytes=1024,
            cost_usd=0.01,
            schema="trades",
            date_range_days=1,
        )
        assert result is None

    def test_large_query_returns_warning(self):
        """Test that large queries return formatted warning."""
        result = format_query_warning(
            record_count=5_000_000,
            size_bytes=500 * 1024 * 1024,
            cost_usd=50.00,
            schema="trades",
            date_range_days=30,
        )
        assert result is not None
        assert "Query Warnings" in result
        assert "Estimates" in result


class TestEstimateDateRangeDays:
    """Tests for estimate_date_range_days function."""

    def test_same_day(self):
        """Test same day returns 1."""
        days = estimate_date_range_days("2024-01-15", "2024-01-15")
        assert days == 1

    def test_one_week(self):
        """Test one week returns 8 days (inclusive)."""
        days = estimate_date_range_days("2024-01-01", "2024-01-07")
        assert days == 7

    def test_one_month(self):
        """Test one month."""
        days = estimate_date_range_days("2024-01-01", "2024-01-31")
        assert days == 31

    def test_invalid_dates_returns_default(self):
        """Test invalid dates return 1."""
        days = estimate_date_range_days("invalid", "also-invalid")
        assert days == 1


class TestGenerateExplainOutput:
    """Tests for generate_explain_output function."""

    def test_basic_explain_output(self):
        """Test basic explain output generation."""
        result = generate_explain_output(
            dataset="GLBX.MDP3",
            symbols=["ES.FUT"],
            schema="trades",
            start="2024-01-15",
            end="2024-01-15",
            record_count=100_000,
            size_bytes=10 * 1024 * 1024,
            cost_usd=0.50,
            cache_status="miss",
        )
        assert "Query Explain Mode" in result
        assert "GLBX.MDP3" in result
        assert "ES.FUT" in result
        assert "trades" in result
        assert "100,000" in result
        assert "$0.50" in result
        assert "miss" in result

    def test_explain_with_warnings(self):
        """Test explain output with warnings."""
        result = generate_explain_output(
            dataset="GLBX.MDP3",
            symbols=["ES.FUT"],
            schema="trades",
            start="2024-01-01",
            end="2024-01-31",
            record_count=5_000_000,
            size_bytes=500 * 1024 * 1024,
            cost_usd=50.00,
            cache_status="miss",
        )
        assert "Warnings" in result
