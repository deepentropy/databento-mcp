"""Smart Data Summaries module for auto-generating statistics from market data.

Provides functions to generate human-readable statistics and insights from
returned data including price range, volume, trade count, and peak activity.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


def generate_data_summary(df: pd.DataFrame, schema: str = "trades") -> str:
    """
    Generate a human-readable summary of market data.

    Args:
        df: DataFrame containing market data
        schema: The data schema type (trades, ohlcv-*, mbp-*, etc.)

    Returns:
        Formatted string with statistics and insights
    """
    if df.empty:
        return "📊 No data to summarize."

    summary_parts = ["📊 **Data Summary**\n"]
    record_count = len(df)
    summary_parts.append(f"📈 Records: {record_count:,}")

    # Generate schema-specific summaries
    if schema == "trades" or "trades" in schema.lower():
        summary_parts.extend(_summarize_trades(df))
    elif schema.startswith("ohlcv"):
        summary_parts.extend(_summarize_ohlcv(df))
    elif schema.startswith("mbp") or schema == "tbbo":
        summary_parts.extend(_summarize_orderbook(df))
    else:
        summary_parts.extend(_summarize_generic(df))

    # Add time-based insights if timestamp available
    time_insights = _generate_time_insights(df)
    if time_insights:
        summary_parts.append("\n⏰ **Time Insights**")
        summary_parts.extend(time_insights)

    return "\n".join(summary_parts)


def _summarize_trades(df: pd.DataFrame) -> List[str]:
    """Generate summary for trade data."""
    summary = []

    # Price statistics
    if "price" in df.columns:
        price_col = df["price"]
        # Handle potential FIXED_PRICE format (integer prices)
        if price_col.dtype in ["int64", "int32"]:
            # Databento uses fixed-point prices (1e-9 scaling)
            price_col = price_col / 1e9
        summary.append(f"\n💰 **Price Range**")
        summary.append(f"  Min: ${price_col.min():,.4f}")
        summary.append(f"  Max: ${price_col.max():,.4f}")
        summary.append(f"  Mean: ${price_col.mean():,.4f}")
        price_range = price_col.max() - price_col.min()
        summary.append(f"  Range: ${price_range:,.4f}")

    # Volume statistics
    if "size" in df.columns:
        size_col = df["size"]
        total_volume = size_col.sum()
        summary.append(f"\n📦 **Volume**")
        summary.append(f"  Total: {total_volume:,}")
        summary.append(f"  Average: {size_col.mean():,.2f}")
        summary.append(f"  Max single trade: {size_col.max():,}")

    # Trade count and direction
    summary.append(f"\n🔢 **Trade Count**: {len(df):,}")

    if "side" in df.columns:
        buy_count = (df["side"] == "B").sum() if df["side"].dtype == object else (df["side"] == 1).sum()
        sell_count = len(df) - buy_count
        buy_pct = (buy_count / len(df) * 100) if len(df) > 0 else 0
        summary.append(f"  Buy: {buy_count:,} ({buy_pct:.1f}%)")
        summary.append(f"  Sell: {sell_count:,} ({100 - buy_pct:.1f}%)")

    return summary


def _summarize_ohlcv(df: pd.DataFrame) -> List[str]:
    """Generate summary for OHLCV bar data."""
    summary = []

    if all(col in df.columns for col in ["open", "high", "low", "close"]):
        summary.append(f"\n💰 **Price Statistics**")
        summary.append(f"  Open (first): ${df['open'].iloc[0]:,.4f}")
        summary.append(f"  Close (last): ${df['close'].iloc[-1]:,.4f}")
        summary.append(f"  High: ${df['high'].max():,.4f}")
        summary.append(f"  Low: ${df['low'].min():,.4f}")

        # Calculate overall change
        first_open = df["open"].iloc[0]
        last_close = df["close"].iloc[-1]
        change = last_close - first_open
        change_pct = (change / first_open * 100) if first_open != 0 else 0
        direction = "📈" if change >= 0 else "📉"
        summary.append(f"\n{direction} **Overall Change**")
        summary.append(f"  Absolute: ${change:+,.4f}")
        summary.append(f"  Percentage: {change_pct:+.2f}%")

    if "volume" in df.columns:
        total_vol = df["volume"].sum()
        avg_vol = df["volume"].mean()
        summary.append(f"\n📦 **Volume**")
        summary.append(f"  Total: {total_vol:,}")
        summary.append(f"  Average per bar: {avg_vol:,.0f}")

    return summary


def _summarize_orderbook(df: pd.DataFrame) -> List[str]:
    """Generate summary for order book data (MBP, TBBO)."""
    summary = []

    # Bid prices
    bid_cols = [c for c in df.columns if "bid" in c.lower() and "price" in c.lower()]
    if bid_cols:
        bid_col = bid_cols[0]
        summary.append(f"\n💚 **Best Bid**")
        summary.append(f"  Average: ${df[bid_col].mean():,.4f}")
        summary.append(f"  High: ${df[bid_col].max():,.4f}")
        summary.append(f"  Low: ${df[bid_col].min():,.4f}")

    # Ask prices
    ask_cols = [c for c in df.columns if "ask" in c.lower() and "price" in c.lower()]
    if ask_cols:
        ask_col = ask_cols[0]
        summary.append(f"\n🔴 **Best Ask**")
        summary.append(f"  Average: ${df[ask_col].mean():,.4f}")
        summary.append(f"  High: ${df[ask_col].max():,.4f}")
        summary.append(f"  Low: ${df[ask_col].min():,.4f}")

    # Spread analysis
    if bid_cols and ask_cols:
        spread = df[ask_cols[0]] - df[bid_cols[0]]
        summary.append(f"\n📐 **Spread**")
        summary.append(f"  Average: ${spread.mean():,.4f}")
        summary.append(f"  Min: ${spread.min():,.4f}")
        summary.append(f"  Max: ${spread.max():,.4f}")

    return summary


def _summarize_generic(df: pd.DataFrame) -> List[str]:
    """Generate generic summary for other data types."""
    summary = []
    summary.append(f"\n📋 **Column Statistics**")

    numeric_cols = df.select_dtypes(include=["number"]).columns[:5]
    for col in numeric_cols:
        summary.append(f"\n  {col}:")
        summary.append(f"    Min: {df[col].min():,.4f}")
        summary.append(f"    Max: {df[col].max():,.4f}")
        summary.append(f"    Mean: {df[col].mean():,.4f}")

    return summary


def _generate_time_insights(df: pd.DataFrame) -> List[str]:
    """Generate time-based insights."""
    insights = []

    # Find timestamp column
    ts_col = None
    for col in ["ts_event", "ts_recv", "timestamp", "time"]:
        if col in df.columns:
            ts_col = col
            break

    if ts_col is None:
        return insights

    try:
        # Convert to datetime if needed
        ts_series = df[ts_col]
        if ts_series.dtype in ["int64", "int32"]:
            # Nanosecond timestamps
            ts_series = pd.to_datetime(ts_series, unit="ns", utc=True)
        elif not pd.api.types.is_datetime64_any_dtype(ts_series):
            ts_series = pd.to_datetime(ts_series, utc=True)

        # Time range
        start_time = ts_series.min()
        end_time = ts_series.max()
        duration = end_time - start_time
        insights.append(f"  Time range: {start_time} to {end_time}")
        insights.append(f"  Duration: {duration}")

        # Find peak activity hour
        if len(df) > 100:  # Only if enough data
            hour_counts = ts_series.dt.hour.value_counts()
            peak_hour = hour_counts.idxmax()
            peak_pct = (hour_counts.max() / len(df) * 100)
            insights.append(f"  Peak activity: {peak_hour:02d}:00 UTC ({peak_pct:.1f}% of records)")

    except Exception:
        pass  # Skip time insights if conversion fails

    return insights


def generate_quick_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate quick statistics dictionary for programmatic use.

    Args:
        df: DataFrame containing market data

    Returns:
        Dictionary with statistics
    """
    stats = {
        "record_count": len(df),
        "columns": list(df.columns),
    }

    if "price" in df.columns:
        price_col = df["price"]
        if price_col.dtype in ["int64", "int32"]:
            price_col = price_col / 1e9
        stats["price"] = {
            "min": float(price_col.min()),
            "max": float(price_col.max()),
            "mean": float(price_col.mean()),
            "std": float(price_col.std()),
        }

    if "size" in df.columns:
        stats["volume"] = {
            "total": int(df["size"].sum()),
            "mean": float(df["size"].mean()),
            "max": int(df["size"].max()),
        }

    if "volume" in df.columns:
        stats["volume"] = {
            "total": int(df["volume"].sum()),
            "mean": float(df["volume"].mean()),
        }

    return stats
