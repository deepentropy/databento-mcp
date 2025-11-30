"""Tests for the validation module."""
import pytest
from databento_mcp.validation import (
    ValidationError,
    validate_date_format,
    validate_symbols,
    validate_dataset,
    validate_schema,
    validate_encoding,
    validate_compression,
    validate_stype,
    validate_numeric_range,
    validate_date_range,
)


class TestValidateDateFormat:
    """Tests for validate_date_format function."""

    def test_valid_date_yyyy_mm_dd(self):
        """Test valid YYYY-MM-DD date format."""
        validate_date_format("2024-01-15")  # Should not raise

    def test_valid_date_iso8601_basic(self):
        """Test valid ISO 8601 date with time."""
        validate_date_format("2024-01-15T10:30:00")  # Should not raise

    def test_valid_date_iso8601_with_z(self):
        """Test valid ISO 8601 date with Z timezone."""
        validate_date_format("2024-01-15T10:30:00Z")  # Should not raise

    def test_valid_date_iso8601_with_offset(self):
        """Test valid ISO 8601 date with timezone offset."""
        validate_date_format("2024-01-15T10:30:00+00:00")  # Should not raise

    def test_valid_date_iso8601_with_milliseconds(self):
        """Test valid ISO 8601 date with milliseconds."""
        validate_date_format("2024-01-15T10:30:00.123Z")  # Should not raise

    def test_invalid_date_empty(self):
        """Test empty date raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_date_format("")

    def test_invalid_date_format(self):
        """Test invalid date format raises ValidationError."""
        with pytest.raises(ValidationError, match="must be in YYYY-MM-DD"):
            validate_date_format("15-01-2024")

    def test_invalid_date_value(self):
        """Test invalid date value raises ValidationError."""
        with pytest.raises(ValidationError, match="invalid date"):
            validate_date_format("2024-13-45")


class TestValidateSymbols:
    """Tests for validate_symbols function."""

    def test_valid_single_symbol(self):
        """Test valid single symbol."""
        result = validate_symbols("AAPL")
        assert result == ["AAPL"]

    def test_valid_multiple_symbols(self):
        """Test valid multiple symbols."""
        result = validate_symbols("AAPL, MSFT, GOOG")
        assert result == ["AAPL", "MSFT", "GOOG"]

    def test_valid_symbol_with_wildcard(self):
        """Test valid symbol with wildcard."""
        result = validate_symbols("ES*")
        assert result == ["ES*"]

    def test_valid_symbol_with_dots(self):
        """Test valid symbol with dots."""
        result = validate_symbols("BRK.A")
        assert result == ["BRK.A"]

    def test_invalid_symbols_empty(self):
        """Test empty symbols raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_symbols("")

    def test_invalid_symbols_whitespace_only(self):
        """Test whitespace-only symbols raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_symbols("   ")

    def test_invalid_symbol_characters(self):
        """Test invalid characters in symbol raises ValidationError."""
        with pytest.raises(ValidationError, match="invalid characters"):
            validate_symbols("AAPL$")


class TestValidateDataset:
    """Tests for validate_dataset function."""

    def test_valid_dataset(self):
        """Test valid dataset name."""
        validate_dataset("GLBX.MDP3")  # Should not raise

    def test_valid_dataset_with_numbers(self):
        """Test valid dataset name with numbers."""
        validate_dataset("XNAS.ITCH")  # Should not raise

    def test_invalid_dataset_empty(self):
        """Test empty dataset raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_dataset("")

    def test_invalid_dataset_no_dot(self):
        """Test dataset without dot raises ValidationError."""
        with pytest.raises(ValidationError, match="must follow VENUE.FORMAT"):
            validate_dataset("GLBXMDP3")

    def test_invalid_dataset_lowercase(self):
        """Test lowercase dataset raises ValidationError."""
        with pytest.raises(ValidationError, match="must follow VENUE.FORMAT"):
            validate_dataset("glbx.mdp3")


class TestValidateSchema:
    """Tests for validate_schema function."""

    def test_valid_schema_trades(self):
        """Test valid trades schema."""
        validate_schema("trades")  # Should not raise

    def test_valid_schema_ohlcv(self):
        """Test valid OHLCV schema."""
        validate_schema("ohlcv-1m")  # Should not raise

    def test_valid_schema_mbp(self):
        """Test valid MBP schema."""
        validate_schema("mbp-1")  # Should not raise

    def test_invalid_schema_empty(self):
        """Test empty schema raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_schema("")

    def test_invalid_schema_unknown(self):
        """Test unknown schema raises ValidationError."""
        with pytest.raises(ValidationError, match="Unknown schema"):
            validate_schema("invalid_schema")


class TestValidateEncoding:
    """Tests for validate_encoding function."""

    def test_valid_encoding_dbn(self):
        """Test valid DBN encoding."""
        validate_encoding("dbn")  # Should not raise

    def test_valid_encoding_csv(self):
        """Test valid CSV encoding."""
        validate_encoding("csv")  # Should not raise

    def test_valid_encoding_json(self):
        """Test valid JSON encoding."""
        validate_encoding("json")  # Should not raise

    def test_invalid_encoding_empty(self):
        """Test empty encoding raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_encoding("")

    def test_invalid_encoding_unknown(self):
        """Test unknown encoding raises ValidationError."""
        with pytest.raises(ValidationError, match="Unknown encoding"):
            validate_encoding("xml")


class TestValidateCompression:
    """Tests for validate_compression function."""

    def test_valid_compression_none(self):
        """Test valid none compression."""
        validate_compression("none")  # Should not raise

    def test_valid_compression_zstd(self):
        """Test valid zstd compression."""
        validate_compression("zstd")  # Should not raise

    def test_invalid_compression_empty(self):
        """Test empty compression raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_compression("")

    def test_invalid_compression_unknown(self):
        """Test unknown compression raises ValidationError."""
        with pytest.raises(ValidationError, match="Unknown compression"):
            validate_compression("gzip")


class TestValidateStype:
    """Tests for validate_stype function."""

    def test_valid_stype_raw_symbol(self):
        """Test valid raw_symbol stype."""
        validate_stype("raw_symbol")  # Should not raise

    def test_valid_stype_instrument_id(self):
        """Test valid instrument_id stype."""
        validate_stype("instrument_id")  # Should not raise

    def test_valid_stype_continuous(self):
        """Test valid continuous stype."""
        validate_stype("continuous")  # Should not raise

    def test_invalid_stype_empty(self):
        """Test empty stype raises ValidationError."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_stype("")

    def test_invalid_stype_unknown(self):
        """Test unknown stype raises ValidationError."""
        with pytest.raises(ValidationError, match="Unknown stype"):
            validate_stype("unknown_stype")


class TestValidateNumericRange:
    """Tests for validate_numeric_range function."""

    def test_valid_value_within_range(self):
        """Test valid value within range."""
        validate_numeric_range(50, "limit", min_value=1, max_value=100)  # Should not raise

    def test_valid_value_at_min(self):
        """Test valid value at minimum."""
        validate_numeric_range(1, "limit", min_value=1, max_value=100)  # Should not raise

    def test_valid_value_at_max(self):
        """Test valid value at maximum."""
        validate_numeric_range(100, "limit", min_value=1, max_value=100)  # Should not raise

    def test_valid_value_no_min(self):
        """Test valid value with no minimum."""
        validate_numeric_range(-100, "offset", max_value=100)  # Should not raise

    def test_valid_value_no_max(self):
        """Test valid value with no maximum."""
        validate_numeric_range(1000000, "limit", min_value=1)  # Should not raise

    def test_invalid_value_below_min(self):
        """Test value below minimum raises ValidationError."""
        with pytest.raises(ValidationError, match="must be at least"):
            validate_numeric_range(0, "limit", min_value=1)

    def test_invalid_value_above_max(self):
        """Test value above maximum raises ValidationError."""
        with pytest.raises(ValidationError, match="must be at most"):
            validate_numeric_range(101, "limit", max_value=100)


class TestValidateDateRange:
    """Tests for validate_date_range function."""

    def test_valid_range_different_dates(self):
        """Test valid date range with different dates."""
        validate_date_range("2024-01-01", "2024-01-15")  # Should not raise

    def test_valid_range_same_date(self):
        """Test valid date range with same date."""
        validate_date_range("2024-01-15", "2024-01-15")  # Should not raise

    def test_invalid_range_start_after_end(self):
        """Test start after end raises ValidationError."""
        with pytest.raises(ValidationError, match="must be before or equal to"):
            validate_date_range("2024-01-15", "2024-01-01")

    def test_invalid_range_invalid_start(self):
        """Test invalid start date raises ValidationError."""
        with pytest.raises(ValidationError, match="must be in YYYY-MM-DD"):
            validate_date_range("invalid", "2024-01-15")

    def test_invalid_range_invalid_end(self):
        """Test invalid end date raises ValidationError."""
        with pytest.raises(ValidationError, match="must be in YYYY-MM-DD"):
            validate_date_range("2024-01-01", "invalid")
