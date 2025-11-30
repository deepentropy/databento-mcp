"""Tests for the data_quality module."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data_quality import (
    DataQualityReport,
    analyze_data_quality,
    detect_time_gaps,
    detect_price_outliers,
    detect_null_values,
    detect_duplicates,
    get_quality_score_explanation,
)


class TestDataQualityReport:
    """Tests for DataQualityReport class."""

    def test_to_string_excellent_score(self):
        """Test string output for excellent score."""
        report = DataQualityReport(score=95, record_count=1000)
        result = report.to_string()
        assert "Quality Score: 95/100" in result
        assert "Excellent" in result

    def test_to_string_with_issues(self):
        """Test string output with issues."""
        report = DataQualityReport(
            score=60,
            record_count=1000,
            issues=["Critical column missing"],
            warnings=["Minor issue found"],
        )
        result = report.to_string()
        assert "Issues Found" in result
        assert "Warnings" in result
        assert "Critical column missing" in result

    def test_to_string_with_time_gaps(self):
        """Test string output with time gaps."""
        report = DataQualityReport(
            score=80,
            record_count=1000,
            time_gaps=[
                {"start": "2024-01-15 10:00:00", "end": "2024-01-15 10:05:00", "duration": "0:05:00"}
            ],
        )
        result = report.to_string()
        assert "Time Gaps" in result

    def test_to_string_with_outliers(self):
        """Test string output with price outliers."""
        report = DataQualityReport(
            score=75,
            record_count=1000,
            price_outliers=[
                {"price": 999.99, "deviation": 5.5}
            ],
        )
        result = report.to_string()
        assert "Price Outliers" in result


class TestAnalyzeDataQuality:
    """Tests for analyze_data_quality function."""

    def test_empty_dataframe(self):
        """Test analysis of empty DataFrame."""
        df = pd.DataFrame()
        report = analyze_data_quality(df)
        assert report.score == 0
        assert len(report.issues) > 0

    def test_perfect_data(self):
        """Test analysis of perfect data."""
        df = pd.DataFrame({
            "ts_event": pd.date_range("2024-01-15 09:30:00", periods=100, freq="1s"),
            "price": np.linspace(100, 101, 100),
            "size": np.ones(100) * 10,
        })
        report = analyze_data_quality(df, "trades")
        assert report.score >= 80  # Should be high quality

    def test_data_with_duplicates(self):
        """Test analysis of data with duplicates."""
        df = pd.DataFrame({
            "price": [100.0, 100.0, 100.0, 101.0, 102.0],
            "size": [10, 10, 10, 20, 30],
        })
        report = analyze_data_quality(df, "trades")
        assert report.duplicate_count > 0

    def test_data_with_null_values(self):
        """Test analysis of data with null values."""
        df = pd.DataFrame({
            "price": [100.0, None, 102.0],
            "size": [10, 20, None],
        })
        report = analyze_data_quality(df, "trades")
        assert len(report.null_columns) > 0


class TestDetectTimeGaps:
    """Tests for detect_time_gaps function."""

    def test_no_timestamp_column(self):
        """Test with no timestamp column."""
        df = pd.DataFrame({"price": [100, 101, 102]})
        gaps = detect_time_gaps(df)
        assert len(gaps) == 0

    def test_continuous_timestamps(self):
        """Test with continuous timestamps (no gaps)."""
        df = pd.DataFrame({
            "ts_event": pd.date_range("2024-01-15 09:30:00", periods=10, freq="1s")
        })
        gaps = detect_time_gaps(df, threshold_seconds=60)
        assert len(gaps) == 0

    def test_gap_detected(self):
        """Test that gaps are detected."""
        timestamps = pd.to_datetime([
            "2024-01-15 09:30:00",
            "2024-01-15 09:30:01",
            "2024-01-15 09:35:00",  # 5 minute gap
            "2024-01-15 09:35:01",
        ])
        df = pd.DataFrame({"ts_event": timestamps})
        gaps = detect_time_gaps(df, threshold_seconds=60)
        assert len(gaps) == 1
        assert gaps[0]["seconds"] > 60


class TestDetectPriceOutliers:
    """Tests for detect_price_outliers function."""

    def test_no_outliers(self):
        """Test data with no outliers."""
        df = pd.DataFrame({
            "price": np.linspace(100, 101, 100)  # Smooth price range
        })
        outliers = detect_price_outliers(df)
        assert len(outliers) == 0

    def test_outliers_detected(self):
        """Test that outliers are detected."""
        prices = [100.0] * 50 + [999.0] + [100.0] * 50  # One obvious outlier
        df = pd.DataFrame({"price": prices})
        outliers = detect_price_outliers(df, std_threshold=3.0)
        assert len(outliers) > 0
        assert any(o["price"] == 999.0 for o in outliers)

    def test_no_price_column(self):
        """Test with no price column."""
        df = pd.DataFrame({"other": [1, 2, 3]})
        outliers = detect_price_outliers(df)
        assert len(outliers) == 0


class TestDetectNullValues:
    """Tests for detect_null_values function."""

    def test_no_nulls(self):
        """Test data with no null values."""
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        })
        null_cols = detect_null_values(df)
        assert len(null_cols) == 0

    def test_nulls_detected(self):
        """Test that null values are detected."""
        df = pd.DataFrame({
            "a": [1, None, 3],
            "b": [4, 5, 6],
            "c": [None, None, None],
        })
        null_cols = detect_null_values(df)
        assert len(null_cols) == 2
        assert "a" in null_cols
        assert "c" in null_cols


class TestDetectDuplicates:
    """Tests for detect_duplicates function."""

    def test_no_duplicates(self):
        """Test data with no duplicates."""
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        })
        dup_count = detect_duplicates(df)
        assert dup_count == 0

    def test_duplicates_detected(self):
        """Test that duplicates are detected."""
        df = pd.DataFrame({
            "a": [1, 1, 2],
            "b": [4, 4, 5],
        })
        dup_count = detect_duplicates(df)
        assert dup_count == 1


class TestGetQualityScoreExplanation:
    """Tests for get_quality_score_explanation function."""

    def test_excellent_score(self):
        """Test explanation for excellent score."""
        explanation = get_quality_score_explanation(95)
        assert "Excellent" in explanation

    def test_good_score(self):
        """Test explanation for good score."""
        explanation = get_quality_score_explanation(75)
        assert "Good" in explanation

    def test_fair_score(self):
        """Test explanation for fair score."""
        explanation = get_quality_score_explanation(55)
        assert "Fair" in explanation

    def test_poor_score(self):
        """Test explanation for poor score."""
        explanation = get_quality_score_explanation(30)
        assert "Poor" in explanation

    def test_very_poor_score(self):
        """Test explanation for very poor score."""
        explanation = get_quality_score_explanation(10)
        assert "Very poor" in explanation
