"""Input validation module for the Databento MCP server."""
import re
from datetime import datetime
from typing import Optional


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


# Known valid schemas from Databento
VALID_SCHEMAS = frozenset([
    "mbo",
    "mbp-1",
    "mbp-10",
    "trades",
    "tbbo",
    "ohlcv-1s",
    "ohlcv-1m",
    "ohlcv-1h",
    "ohlcv-1d",
    "definition",
    "imbalance",
    "statistics",
    "status",
])

# Known valid encodings
VALID_ENCODINGS = frozenset(["dbn", "csv", "json"])

# Known valid compressions
VALID_COMPRESSIONS = frozenset(["none", "zstd"])

# Known valid symbol types (stype)
VALID_STYPES = frozenset([
    "raw_symbol",
    "instrument_id",
    "continuous",
    "parent",
    "smart",
])

# Regex for YYYY-MM-DD date format
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Regex for ISO 8601 datetime (simplified, covers common cases)
# Matches: 2024-01-15, 2024-01-15T10:30:00, 2024-01-15T10:30:00Z, 2024-01-15T10:30:00+00:00
ISO8601_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
)

# Regex for valid symbol characters (alphanumeric, dots, dashes, underscores, spaces)
SYMBOL_PATTERN = re.compile(r"^[\w.\-\s*]+$")

# Regex for dataset name pattern (VENUE.FORMAT like GLBX.MDP3)
DATASET_PATTERN = re.compile(r"^[A-Z0-9]+\.[A-Z0-9]+$")


def validate_date_format(date_str: str, field_name: str = "date") -> None:
    """
    Validate that a string is in YYYY-MM-DD or ISO 8601 format.
    
    Args:
        date_str: The date string to validate
        field_name: Name of the field for error messages
        
    Raises:
        ValidationError: If the date format is invalid
    """
    if not date_str:
        raise ValidationError(f"{field_name} cannot be empty")
    
    if not ISO8601_PATTERN.match(date_str):
        raise ValidationError(
            f"{field_name} must be in YYYY-MM-DD or ISO 8601 format, got: {date_str}"
        )
    
    # Try to parse the date portion to ensure it's valid
    try:
        date_part = date_str[:10]  # Extract YYYY-MM-DD portion
        datetime.strptime(date_part, "%Y-%m-%d")
    except ValueError as e:
        raise ValidationError(f"{field_name} contains invalid date: {str(e)}")


def validate_symbols(symbols: str, field_name: str = "symbols") -> list[str]:
    """
    Validate and parse a comma-separated list of symbols.
    
    Args:
        symbols: Comma-separated string of symbols
        field_name: Name of the field for error messages
        
    Returns:
        List of validated symbol strings
        
    Raises:
        ValidationError: If symbols are invalid
    """
    if not symbols or not symbols.strip():
        raise ValidationError(f"{field_name} cannot be empty")
    
    symbol_list = [s.strip() for s in symbols.split(",")]
    
    if not symbol_list or all(s == "" for s in symbol_list):
        raise ValidationError(f"{field_name} must contain at least one symbol")
    
    for symbol in symbol_list:
        if not symbol:
            raise ValidationError(f"{field_name} contains empty symbol")
        if not SYMBOL_PATTERN.match(symbol):
            raise ValidationError(
                f"{field_name} contains invalid characters in symbol: {symbol}"
            )
    
    return symbol_list


def validate_dataset(dataset: str) -> None:
    """
    Validate dataset name follows VENUE.FORMAT pattern (e.g., GLBX.MDP3).
    
    Args:
        dataset: The dataset name to validate
        
    Raises:
        ValidationError: If the dataset name is invalid
    """
    if not dataset:
        raise ValidationError("dataset cannot be empty")
    
    if not DATASET_PATTERN.match(dataset):
        raise ValidationError(
            f"dataset must follow VENUE.FORMAT pattern (e.g., GLBX.MDP3), got: {dataset}"
        )


def validate_schema(schema: str) -> None:
    """
    Validate schema against known valid schemas.
    
    Args:
        schema: The schema name to validate
        
    Raises:
        ValidationError: If the schema is unknown
    """
    if not schema:
        raise ValidationError("schema cannot be empty")
    
    if schema not in VALID_SCHEMAS:
        raise ValidationError(
            f"Unknown schema: {schema}. Valid schemas are: {', '.join(sorted(VALID_SCHEMAS))}"
        )


def validate_encoding(encoding: str) -> None:
    """
    Validate encoding against known valid encodings.
    
    Args:
        encoding: The encoding to validate
        
    Raises:
        ValidationError: If the encoding is unknown
    """
    if not encoding:
        raise ValidationError("encoding cannot be empty")
    
    if encoding not in VALID_ENCODINGS:
        raise ValidationError(
            f"Unknown encoding: {encoding}. Valid encodings are: {', '.join(sorted(VALID_ENCODINGS))}"
        )


def validate_compression(compression: str) -> None:
    """
    Validate compression against known valid compressions.
    
    Args:
        compression: The compression to validate
        
    Raises:
        ValidationError: If the compression is unknown
    """
    if not compression:
        raise ValidationError("compression cannot be empty")
    
    if compression not in VALID_COMPRESSIONS:
        raise ValidationError(
            f"Unknown compression: {compression}. Valid compressions are: {', '.join(sorted(VALID_COMPRESSIONS))}"
        )


def validate_stype(stype: str, field_name: str = "stype") -> None:
    """
    Validate symbol type (stype) against known valid types.
    
    Args:
        stype: The symbol type to validate
        field_name: Name of the field for error messages
        
    Raises:
        ValidationError: If the stype is unknown
    """
    if not stype:
        raise ValidationError(f"{field_name} cannot be empty")
    
    if stype not in VALID_STYPES:
        raise ValidationError(
            f"Unknown {field_name}: {stype}. Valid stypes are: {', '.join(sorted(VALID_STYPES))}"
        )


def validate_numeric_range(
    value: int | float,
    field_name: str,
    min_value: Optional[int | float] = None,
    max_value: Optional[int | float] = None,
) -> None:
    """
    Validate that a numeric value is within specified bounds.
    
    Args:
        value: The numeric value to validate
        field_name: Name of the field for error messages
        min_value: Minimum allowed value (inclusive), or None for no minimum
        max_value: Maximum allowed value (inclusive), or None for no maximum
        
    Raises:
        ValidationError: If the value is outside the allowed range
    """
    if min_value is not None and value < min_value:
        raise ValidationError(
            f"{field_name} must be at least {min_value}, got: {value}"
        )
    
    if max_value is not None and value > max_value:
        raise ValidationError(
            f"{field_name} must be at most {max_value}, got: {value}"
        )


def validate_date_range(start: str, end: str) -> None:
    """
    Validate that start date is before or equal to end date.
    
    Args:
        start: Start date string in YYYY-MM-DD or ISO 8601 format
        end: End date string in YYYY-MM-DD or ISO 8601 format
        
    Raises:
        ValidationError: If start is after end
    """
    # First validate both dates are in correct format
    validate_date_format(start, "start")
    validate_date_format(end, "end")
    
    # Extract date portions for comparison
    start_date = start[:10]
    end_date = end[:10]
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    if start_dt > end_dt:
        raise ValidationError(
            f"start date ({start}) must be before or equal to end date ({end})"
        )
