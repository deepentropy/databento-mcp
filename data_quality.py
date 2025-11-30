"""Data Quality Alerts module for detecting issues in market data.

Provides functions to detect time gaps, price outliers, null values,
duplicates, and generate a data quality score.
"""
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class DataQualityReport:
    """Data quality report containing all detected issues."""

    score: int  # 0-100 quality score
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    time_gaps: List[Dict[str, Any]] = field(default_factory=list)
    price_outliers: List[Dict[str, Any]] = field(default_factory=list)
    null_columns: List[str] = field(default_factory=list)
    duplicate_count: int = 0
    record_count: int = 0

    def to_string(self) -> str:
        """Format report as human-readable string."""
        parts = [f"📊 **Data Quality Report**\n"]
        parts.append(f"🎯 Quality Score: {self.score}/100")

        if self.score >= 90:
            parts.append(" ✅ Excellent")
        elif self.score >= 70:
            parts.append(" 🟡 Good")
        elif self.score >= 50:
            parts.append(" 🟠 Fair")
        else:
            parts.append(" 🔴 Poor")

        parts.append(f"\n📈 Records Analyzed: {self.record_count:,}\n")

        # Issues
        if self.issues:
            parts.append("\n❌ **Issues Found**")
            for issue in self.issues:
                parts.append(f"  • {issue}")

        # Warnings
        if self.warnings:
            parts.append("\n⚠️ **Warnings**")
            for warning in self.warnings:
                parts.append(f"  • {warning}")

        # Time gaps
        if self.time_gaps:
            parts.append(f"\n⏰ **Time Gaps** ({len(self.time_gaps)} found)")
            for i, gap in enumerate(self.time_gaps[:5]):  # Show first 5
                parts.append(f"  {i + 1}. {gap['start']} to {gap['end']} ({gap['duration']})")
            if len(self.time_gaps) > 5:
                parts.append(f"  ... and {len(self.time_gaps) - 5} more gaps")

        # Price outliers
        if self.price_outliers:
            parts.append(f"\n📉 **Price Outliers** ({len(self.price_outliers)} found)")
            for i, outlier in enumerate(self.price_outliers[:5]):  # Show first 5
                parts.append(
                    f"  {i + 1}. Price: ${outlier['price']:,.4f} "
                    f"({outlier['deviation']:.1f} std devs from mean)"
                )
            if len(self.price_outliers) > 5:
                parts.append(f"  ... and {len(self.price_outliers) - 5} more outliers")

        # Null values
        if self.null_columns:
            parts.append(f"\n🔲 **Columns with Null Values**")
            for col in self.null_columns:
                parts.append(f"  • {col}")

        # Duplicates
        if self.duplicate_count > 0:
            parts.append(f"\n🔁 **Duplicates**: {self.duplicate_count:,} records")

        # Summary
        if not self.issues and not self.warnings:
            parts.append("\n✅ No significant data quality issues detected.")

        return "\n".join(parts)


def analyze_data_quality(
    df: pd.DataFrame,
    schema: str = "trades",
    time_gap_threshold_seconds: int = 60,
    outlier_std_threshold: float = 3.0,
) -> DataQualityReport:
    """
    Analyze data quality and generate a report.

    Args:
        df: DataFrame containing market data
        schema: Data schema type
        time_gap_threshold_seconds: Threshold for detecting time gaps
        outlier_std_threshold: Number of standard deviations for outlier detection

    Returns:
        DataQualityReport with all findings
    """
    report = DataQualityReport(score=100, record_count=len(df))

    if df.empty:
        report.score = 0
        report.issues.append("No data to analyze")
        return report

    # 1. Check for time gaps
    time_gaps = detect_time_gaps(df, time_gap_threshold_seconds)
    if time_gaps:
        report.time_gaps = time_gaps
        # Deduct points based on number of gaps
        gap_penalty = min(len(time_gaps) * 2, 20)
        report.score -= gap_penalty
        report.warnings.append(
            f"Found {len(time_gaps)} time gap(s) exceeding {time_gap_threshold_seconds}s"
        )

    # 2. Check for price outliers
    if schema in ["trades", "tbbo", "mbp-1", "mbp-10"]:
        outliers = detect_price_outliers(df, outlier_std_threshold)
        if outliers:
            report.price_outliers = outliers
            # Deduct points based on outlier percentage
            outlier_pct = (len(outliers) / len(df)) * 100
            if outlier_pct > 1:
                report.score -= min(int(outlier_pct * 5), 25)
                report.warnings.append(
                    f"Found {len(outliers)} price outlier(s) ({outlier_pct:.2f}% of data)"
                )

    # 3. Check for null values
    null_columns = detect_null_values(df)
    if null_columns:
        report.null_columns = null_columns
        # Deduct points based on critical columns
        critical_nulls = [c for c in null_columns if c in ["price", "size", "volume", "ts_event"]]
        if critical_nulls:
            report.score -= len(critical_nulls) * 10
            report.issues.append(f"Critical columns with null values: {', '.join(critical_nulls)}")
        else:
            report.score -= len(null_columns) * 2
            report.warnings.append(f"Found null values in {len(null_columns)} column(s)")

    # 4. Check for duplicates
    duplicate_count = detect_duplicates(df)
    if duplicate_count > 0:
        report.duplicate_count = duplicate_count
        dup_pct = (duplicate_count / len(df)) * 100
        if dup_pct > 5:
            report.score -= min(int(dup_pct), 20)
            report.issues.append(f"High duplicate rate: {duplicate_count:,} ({dup_pct:.1f}%)")
        else:
            report.warnings.append(f"Found {duplicate_count:,} duplicate record(s)")

    # Ensure score is within bounds
    report.score = max(0, min(100, report.score))

    return report


def detect_time_gaps(
    df: pd.DataFrame,
    threshold_seconds: int = 60,
) -> List[Dict[str, Any]]:
    """
    Detect time gaps in the data.

    Args:
        df: DataFrame containing market data
        threshold_seconds: Minimum gap duration to report

    Returns:
        List of detected gaps with details
    """
    gaps = []

    # Find timestamp column
    ts_col = None
    for col in ["ts_event", "ts_recv", "timestamp", "time"]:
        if col in df.columns:
            ts_col = col
            break

    if ts_col is None or len(df) < 2:
        return gaps

    try:
        # Convert to datetime if needed
        ts_series = df[ts_col].copy()
        if ts_series.dtype in ["int64", "int32"]:
            ts_series = pd.to_datetime(ts_series, unit="ns", utc=True)
        elif not pd.api.types.is_datetime64_any_dtype(ts_series):
            ts_series = pd.to_datetime(ts_series, utc=True)

        # Sort by timestamp
        ts_sorted = ts_series.sort_values()

        # Calculate time differences
        time_diffs = ts_sorted.diff()
        threshold = pd.Timedelta(seconds=threshold_seconds)

        # Find gaps exceeding threshold
        gap_indices = time_diffs[time_diffs > threshold].index

        for idx in gap_indices:
            loc = ts_sorted.index.get_loc(idx)
            if loc > 0:
                prev_idx = ts_sorted.index[loc - 1]
                gap_start = ts_sorted[prev_idx]
                gap_end = ts_sorted[idx]
                duration = gap_end - gap_start

                gaps.append(
                    {
                        "start": str(gap_start),
                        "end": str(gap_end),
                        "duration": str(duration),
                        "seconds": duration.total_seconds(),
                    }
                )

    except Exception:
        pass  # Skip gap detection if timestamp conversion fails

    return gaps


def detect_price_outliers(
    df: pd.DataFrame,
    std_threshold: float = 3.0,
) -> List[Dict[str, Any]]:
    """
    Detect price outliers using standard deviation method.

    Args:
        df: DataFrame containing market data
        std_threshold: Number of standard deviations for outlier detection

    Returns:
        List of detected outliers with details
    """
    outliers = []

    # Find price column
    price_col = None
    for col in ["price", "close", "last_price"]:
        if col in df.columns:
            price_col = col
            break

    if price_col is None:
        return outliers

    try:
        prices = df[price_col].copy()

        # Handle fixed-point prices
        if prices.dtype in ["int64", "int32"]:
            prices = prices / 1e9

        # Remove zeros and NaN
        prices = prices[prices > 0].dropna()

        if len(prices) < 10:  # Need enough data for meaningful statistics
            return outliers

        mean_price = prices.mean()
        std_price = prices.std()

        if std_price == 0:
            return outliers

        # Find outliers
        deviations = np.abs(prices - mean_price) / std_price
        outlier_mask = deviations > std_threshold

        for idx in prices[outlier_mask].index[:100]:  # Limit to first 100
            outliers.append(
                {
                    "index": int(idx) if hasattr(idx, "__int__") else str(idx),
                    "price": float(prices[idx]),
                    "deviation": float(deviations[idx]),
                    "mean": float(mean_price),
                    "std": float(std_price),
                }
            )

    except Exception:
        pass  # Skip outlier detection on error

    return outliers


def detect_null_values(df: pd.DataFrame) -> List[str]:
    """
    Detect columns with null values.

    Args:
        df: DataFrame containing market data

    Returns:
        List of column names with null values
    """
    null_columns = []

    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            null_columns.append(col)

    return null_columns


def detect_duplicates(df: pd.DataFrame) -> int:
    """
    Detect duplicate records.

    Args:
        df: DataFrame containing market data

    Returns:
        Number of duplicate records
    """
    try:
        # Use all columns for duplicate detection
        return int(df.duplicated().sum())
    except Exception:
        return 0


def get_quality_score_explanation(score: int) -> str:
    """
    Get explanation for quality score.

    Args:
        score: Quality score (0-100)

    Returns:
        Human-readable explanation
    """
    if score >= 90:
        return "Excellent data quality. No significant issues detected."
    elif score >= 70:
        return "Good data quality. Minor issues may be present but data is usable."
    elif score >= 50:
        return "Fair data quality. Some issues detected that may affect analysis."
    elif score >= 25:
        return "Poor data quality. Significant issues detected. Use with caution."
    else:
        return "Very poor data quality. Data may be corrupted or incomplete."
