"""Tests for the summaries module."""
import pytest
import pandas as pd
import numpy as np
from summaries import (
    generate_data_summary,
    generate_quick_stats,
    _summarize_trades,
    _summarize_ohlcv,
    _summarize_orderbook,
)


class TestGenerateDataSummary:
    """Tests for generate_data_summary function."""

    def test_empty_dataframe(self):
        """Test summary for empty DataFrame."""
        df = pd.DataFrame()
        result = generate_data_summary(df)
        assert "No data to summarize" in result

    def test_trades_summary(self):
        """Test summary for trade data."""
        df = pd.DataFrame({
            "price": [100.5, 101.0, 100.75, 101.25, 100.25],
            "size": [10, 20, 15, 25, 30],
            "side": ["B", "S", "B", "S", "B"],
        })
        result = generate_data_summary(df, "trades")
        assert "Data Summary" in result
        assert "Records: 5" in result
        assert "Price Range" in result
        assert "Volume" in result

    def test_ohlcv_summary(self):
        """Test summary for OHLCV data."""
        df = pd.DataFrame({
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [1000, 1500, 2000],
        })
        result = generate_data_summary(df, "ohlcv-1m")
        assert "Price Statistics" in result
        assert "Volume" in result

    def test_generic_summary(self):
        """Test summary for generic data."""
        df = pd.DataFrame({
            "value1": [1, 2, 3, 4, 5],
            "value2": [10, 20, 30, 40, 50],
        })
        result = generate_data_summary(df, "unknown_schema")
        assert "Column Statistics" in result


class TestSummarizeTrades:
    """Tests for _summarize_trades function."""

    def test_with_price_and_size(self):
        """Test trade summary with price and size columns."""
        df = pd.DataFrame({
            "price": [100.0, 105.0, 102.5],
            "size": [10, 20, 30],
        })
        summary = _summarize_trades(df)
        assert any("Price Range" in s for s in summary)
        assert any("Volume" in s for s in summary)

    def test_with_side_column(self):
        """Test trade summary with side column."""
        df = pd.DataFrame({
            "price": [100.0, 105.0],
            "size": [10, 20],
            "side": ["B", "S"],
        })
        summary = _summarize_trades(df)
        assert any("Buy:" in s for s in summary)
        assert any("Sell:" in s for s in summary)

    def test_fixed_point_prices(self):
        """Test handling of fixed-point integer prices."""
        # Databento uses 1e-9 scaling for fixed-point prices
        df = pd.DataFrame({
            "price": [100_000_000_000, 105_000_000_000],  # 100.0 and 105.0
            "size": [10, 20],
        })
        summary = _summarize_trades(df)
        assert any("Price Range" in s for s in summary)


class TestSummarizeOhlcv:
    """Tests for _summarize_ohlcv function."""

    def test_full_ohlcv_data(self):
        """Test OHLCV summary with all columns."""
        df = pd.DataFrame({
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000, 2000],
        })
        summary = _summarize_ohlcv(df)
        assert any("Open" in s for s in summary)
        assert any("High" in s for s in summary)
        assert any("Low" in s for s in summary)
        assert any("Close" in s for s in summary)
        assert any("Volume" in s for s in summary)
        assert any("Overall Change" in s for s in summary)


class TestSummarizeOrderbook:
    """Tests for _summarize_orderbook function."""

    def test_bid_ask_data(self):
        """Test orderbook summary with bid/ask data."""
        df = pd.DataFrame({
            "bid_price_0": [99.5, 99.6, 99.7],
            "ask_price_0": [100.5, 100.6, 100.7],
        })
        summary = _summarize_orderbook(df)
        assert any("Best Bid" in s for s in summary)
        assert any("Best Ask" in s for s in summary)
        assert any("Spread" in s for s in summary)


class TestGenerateQuickStats:
    """Tests for generate_quick_stats function."""

    def test_basic_stats(self):
        """Test basic statistics generation."""
        df = pd.DataFrame({
            "price": [100.0, 105.0, 102.5],
            "size": [10, 20, 30],
        })
        stats = generate_quick_stats(df)
        assert stats["record_count"] == 3
        assert "price" in stats
        assert stats["price"]["min"] == 100.0
        assert stats["price"]["max"] == 105.0

    def test_volume_stats(self):
        """Test volume statistics."""
        df = pd.DataFrame({
            "size": [10, 20, 30],
        })
        stats = generate_quick_stats(df)
        assert "volume" in stats
        assert stats["volume"]["total"] == 60
