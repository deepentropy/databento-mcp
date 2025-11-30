"""Query Size Warnings module for estimating and warning about large queries.

Provides functions to estimate query size before execution and warn users
about potentially expensive operations.
"""
from typing import Dict, List, Optional, Tuple

# Approximate record sizes in bytes for different schemas
SCHEMA_RECORD_SIZES = {
    "trades": 64,
    "tbbo": 48,
    "mbp-1": 56,
    "mbp-10": 560,
    "mbo": 80,
    "ohlcv-1s": 56,
    "ohlcv-1m": 56,
    "ohlcv-1h": 56,
    "ohlcv-1d": 56,
    "definition": 2048,
    "imbalance": 128,
    "statistics": 64,
    "status": 32,
}

# Thresholds for warnings
WARNING_THRESHOLDS = {
    "size_bytes": 100 * 1024 * 1024,  # 100 MB
    "record_count": 1_000_000,  # 1 million records
    "estimated_cost_usd": 10.0,  # $10 USD
}


def estimate_query_size(
    record_count: int,
    schema: str,
) -> Dict[str, float]:
    """
    Estimate the size of a query result.

    Args:
        record_count: Estimated number of records
        schema: Data schema type

    Returns:
        Dictionary with size estimates
    """
    record_size = SCHEMA_RECORD_SIZES.get(schema, 64)
    estimated_bytes = record_count * record_size

    return {
        "record_count": record_count,
        "estimated_bytes": estimated_bytes,
        "estimated_mb": estimated_bytes / (1024 * 1024),
        "estimated_gb": estimated_bytes / (1024 * 1024 * 1024),
        "record_size_bytes": record_size,
    }


def check_query_warnings(
    record_count: int,
    size_bytes: int,
    cost_usd: float = 0.0,
) -> List[str]:
    """
    Check if a query exceeds warning thresholds.

    Args:
        record_count: Estimated number of records
        size_bytes: Estimated size in bytes
        cost_usd: Estimated cost in USD

    Returns:
        List of warning messages
    """
    warnings = []

    if record_count > WARNING_THRESHOLDS["record_count"]:
        warnings.append(
            f"⚠️ Large query: {record_count:,} records "
            f"(threshold: {WARNING_THRESHOLDS['record_count']:,})"
        )

    if size_bytes > WARNING_THRESHOLDS["size_bytes"]:
        size_mb = size_bytes / (1024 * 1024)
        threshold_mb = WARNING_THRESHOLDS["size_bytes"] / (1024 * 1024)
        warnings.append(
            f"⚠️ Large data size: {size_mb:.1f} MB "
            f"(threshold: {threshold_mb:.0f} MB)"
        )

    if cost_usd > WARNING_THRESHOLDS["estimated_cost_usd"]:
        warnings.append(
            f"⚠️ High estimated cost: ${cost_usd:.2f} USD "
            f"(threshold: ${WARNING_THRESHOLDS['estimated_cost_usd']:.2f})"
        )

    return warnings


def generate_alternatives(
    record_count: int,
    schema: str,
    date_range_days: int,
) -> List[str]:
    """
    Generate alternative suggestions for large queries.

    Args:
        record_count: Estimated number of records
        schema: Data schema type
        date_range_days: Number of days in the query range

    Returns:
        List of alternative suggestions
    """
    suggestions = []

    # Suggest aggregated data for tick-level schemas
    if schema in ["trades", "mbo", "mbp-1", "mbp-10", "tbbo"]:
        if record_count > 100_000:
            suggestions.append(
                "💡 Consider using aggregated data (ohlcv-1m, ohlcv-1h, or ohlcv-1d) "
                "for reduced data volume"
            )

    # Suggest batch jobs for large downloads
    if record_count > 1_000_000:
        suggestions.append(
            "💡 For large historical downloads, consider using `submit_batch_job` "
            "which is more cost-effective and doesn't timeout"
        )

    # Suggest smaller date ranges
    if date_range_days > 30:
        suggestions.append(
            "💡 Consider splitting the query into smaller date ranges "
            "(e.g., weekly or monthly) for better performance"
        )

    # Suggest using limits
    if record_count > 10_000:
        suggestions.append(
            "💡 Use the `limit` parameter to retrieve a sample first "
            "before fetching full data"
        )

    return suggestions


def format_query_warning(
    record_count: int,
    size_bytes: int,
    cost_usd: float,
    schema: str,
    date_range_days: int,
) -> Optional[str]:
    """
    Format a complete query warning message.

    Args:
        record_count: Estimated number of records
        size_bytes: Estimated size in bytes
        cost_usd: Estimated cost in USD
        schema: Data schema type
        date_range_days: Number of days in the query range

    Returns:
        Formatted warning message or None if no warnings
    """
    warnings = check_query_warnings(record_count, size_bytes, cost_usd)

    if not warnings:
        return None

    message_parts = ["\n🚨 **Query Warnings**\n"]
    message_parts.extend(warnings)

    # Add estimates
    message_parts.append(f"\n📊 **Estimates**")
    message_parts.append(f"  Records: {record_count:,}")
    message_parts.append(f"  Size: {size_bytes / (1024 * 1024):.1f} MB")
    if cost_usd > 0:
        message_parts.append(f"  Cost: ${cost_usd:.4f} USD")

    # Add alternatives
    alternatives = generate_alternatives(record_count, schema, date_range_days)
    if alternatives:
        message_parts.append(f"\n💡 **Alternatives**")
        message_parts.extend(alternatives)

    return "\n".join(message_parts)


def estimate_date_range_days(start: str, end: str) -> int:
    """
    Calculate the number of days between two dates.

    Args:
        start: Start date in YYYY-MM-DD format
        end: End date in YYYY-MM-DD format

    Returns:
        Number of days
    """
    from datetime import datetime

    try:
        start_date = datetime.strptime(start[:10], "%Y-%m-%d")
        end_date = datetime.strptime(end[:10], "%Y-%m-%d")
        return (end_date - start_date).days + 1
    except (ValueError, TypeError):
        return 1


def generate_explain_output(
    dataset: str,
    symbols: List[str],
    schema: str,
    start: str,
    end: str,
    record_count: int,
    size_bytes: int,
    cost_usd: float,
    cache_status: str,
) -> str:
    """
    Generate explain mode output for a query.

    Args:
        dataset: Dataset name
        symbols: List of symbols
        schema: Data schema
        start: Start date
        end: End date
        record_count: Estimated records
        size_bytes: Estimated size
        cost_usd: Estimated cost
        cache_status: Cache status (hit/miss/expired)

    Returns:
        Formatted explain output
    """
    output_parts = ["🔍 **Query Explain Mode** (No API call made)\n"]

    # Query details
    output_parts.append("📋 **Query Details**")
    output_parts.append(f"  Dataset: {dataset}")
    output_parts.append(f"  Symbols: {', '.join(symbols)}")
    output_parts.append(f"  Schema: {schema}")
    output_parts.append(f"  Date Range: {start} to {end}")

    date_range_days = estimate_date_range_days(start, end)
    output_parts.append(f"  Days: {date_range_days}")

    # Estimates
    output_parts.append(f"\n📊 **Estimates**")
    output_parts.append(f"  Records: ~{record_count:,}")
    output_parts.append(f"  Size: ~{size_bytes / (1024 * 1024):.1f} MB")
    output_parts.append(f"  Cost: ~${cost_usd:.4f} USD")

    # Cache status
    output_parts.append(f"\n📦 **Cache Status**")
    output_parts.append(f"  Status: {cache_status}")

    # Warnings
    warnings = check_query_warnings(record_count, size_bytes, cost_usd)
    if warnings:
        output_parts.append(f"\n⚠️ **Warnings**")
        for warning in warnings:
            output_parts.append(f"  {warning}")

    # Alternatives
    alternatives = generate_alternatives(record_count, schema, date_range_days)
    if alternatives:
        output_parts.append(f"\n💡 **Suggestions**")
        for alt in alternatives:
            output_parts.append(f"  {alt}")

    return "\n".join(output_parts)
