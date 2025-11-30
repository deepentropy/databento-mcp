"""Tests for the errors module."""
from databento_mcp.errors import (
    ErrorCode,
    MCPError,
    validation_error,
    invalid_date_error,
    invalid_symbols_error,
    invalid_dataset_error,
    invalid_schema_error,
    date_range_error,
    api_unavailable_error,
    rate_limit_error,
    auth_error,
    not_found_error,
    api_error,
    file_not_found_error,
    invalid_path_error,
    write_error,
    read_error,
    no_data_error,
    parse_error,
)


class TestErrorCode:
    """Tests for ErrorCode enum."""

    def test_validation_error_codes(self):
        """Test validation error codes exist and have correct values."""
        assert ErrorCode.E1001_INVALID_DATE.value == "E1001"
        assert ErrorCode.E1002_INVALID_SYMBOLS.value == "E1002"
        assert ErrorCode.E1003_INVALID_DATASET.value == "E1003"
        assert ErrorCode.E1004_INVALID_SCHEMA.value == "E1004"
        assert ErrorCode.E1005_INVALID_PARAMETER.value == "E1005"
        assert ErrorCode.E1006_DATE_RANGE_INVALID.value == "E1006"

    def test_api_error_codes(self):
        """Test API error codes exist and have correct values."""
        assert ErrorCode.E2001_API_UNAVAILABLE.value == "E2001"
        assert ErrorCode.E2002_RATE_LIMITED.value == "E2002"
        assert ErrorCode.E2003_AUTH_FAILED.value == "E2003"
        assert ErrorCode.E2004_NOT_FOUND.value == "E2004"
        assert ErrorCode.E2005_API_ERROR.value == "E2005"

    def test_file_error_codes(self):
        """Test file error codes exist and have correct values."""
        assert ErrorCode.E3001_FILE_NOT_FOUND.value == "E3001"
        assert ErrorCode.E3002_INVALID_PATH.value == "E3002"
        assert ErrorCode.E3003_WRITE_ERROR.value == "E3003"
        assert ErrorCode.E3004_READ_ERROR.value == "E3004"

    def test_data_error_codes(self):
        """Test data error codes exist and have correct values."""
        assert ErrorCode.E4001_NO_DATA.value == "E4001"
        assert ErrorCode.E4002_PARSE_ERROR.value == "E4002"


class TestMCPError:
    """Tests for MCPError class."""

    def test_basic_error_creation(self):
        """Test creating a basic MCPError."""
        error = MCPError(
            code=ErrorCode.E1001_INVALID_DATE,
            message="Invalid date format",
            suggestion="Use YYYY-MM-DD format"
        )
        assert error.code == ErrorCode.E1001_INVALID_DATE
        assert error.message == "Invalid date format"
        assert error.suggestion == "Use YYYY-MM-DD format"
        assert error.details is None
        assert error.recoverable is True

    def test_error_with_details(self):
        """Test creating an MCPError with details."""
        error = MCPError(
            code=ErrorCode.E1001_INVALID_DATE,
            message="Invalid date",
            suggestion="Fix the date",
            details={"field": "start", "value": "bad-date"}
        )
        assert error.details == {"field": "start", "value": "bad-date"}

    def test_non_recoverable_error(self):
        """Test creating a non-recoverable error."""
        error = MCPError(
            code=ErrorCode.E2003_AUTH_FAILED,
            message="Auth failed",
            suggestion="Check API key",
            recoverable=False
        )
        assert error.recoverable is False

    def test_to_response_basic(self):
        """Test to_response formats correctly for basic error."""
        error = MCPError(
            code=ErrorCode.E1001_INVALID_DATE,
            message="Invalid date format",
            suggestion="Use YYYY-MM-DD format"
        )
        response = error.to_response()
        assert "❌ Error [E1001]" in response
        assert "Invalid date format" in response
        assert "💡 Suggestion: Use YYYY-MM-DD format" in response
        assert "✅ This error is recoverable" in response

    def test_to_response_with_details(self):
        """Test to_response includes details when present."""
        error = MCPError(
            code=ErrorCode.E1001_INVALID_DATE,
            message="Invalid date",
            suggestion="Fix the date",
            details={"field": "start", "value": "bad-date"}
        )
        response = error.to_response()
        assert "📋 Details:" in response
        assert "field: start" in response
        assert "value: bad-date" in response

    def test_to_response_non_recoverable(self):
        """Test to_response shows non-recoverable message."""
        error = MCPError(
            code=ErrorCode.E2003_AUTH_FAILED,
            message="Auth failed",
            suggestion="Check API key",
            recoverable=False
        )
        response = error.to_response()
        assert "⚠️ This error may require configuration changes" in response
        assert "support@databento.com" in response


class TestErrorFactories:
    """Tests for error factory functions."""

    def test_validation_error(self):
        """Test validation_error factory."""
        error = validation_error("limit", "must be positive", "Use a positive number")
        assert error.code == ErrorCode.E1005_INVALID_PARAMETER
        assert "Invalid limit" in error.message
        assert error.details == {"parameter": "limit"}

    def test_invalid_date_error(self):
        """Test invalid_date_error factory."""
        error = invalid_date_error("bad-date", "start")
        assert error.code == ErrorCode.E1001_INVALID_DATE
        assert "Invalid start" in error.message
        assert error.details["field"] == "start"
        assert error.details["value"] == "bad-date"

    def test_invalid_symbols_error(self):
        """Test invalid_symbols_error factory."""
        error = invalid_symbols_error("AAPL$")
        assert error.code == ErrorCode.E1002_INVALID_SYMBOLS
        assert "Invalid symbols" in error.message
        assert error.details["symbols"] == "AAPL$"

    def test_invalid_dataset_error(self):
        """Test invalid_dataset_error factory."""
        error = invalid_dataset_error("invalid")
        assert error.code == ErrorCode.E1003_INVALID_DATASET
        assert "Invalid dataset" in error.message
        assert "list_datasets" in error.suggestion

    def test_invalid_schema_error(self):
        """Test invalid_schema_error factory."""
        error = invalid_schema_error("bad_schema", ["trades", "ohlcv-1m"])
        assert error.code == ErrorCode.E1004_INVALID_SCHEMA
        assert "Invalid schema" in error.message
        assert error.details["valid_schemas"] == ["trades", "ohlcv-1m"]

    def test_date_range_error(self):
        """Test date_range_error factory."""
        error = date_range_error("2024-01-15", "2024-01-01")
        assert error.code == ErrorCode.E1006_DATE_RANGE_INVALID
        assert "Invalid date range" in error.message
        assert error.details["start"] == "2024-01-15"
        assert error.details["end"] == "2024-01-01"

    def test_api_unavailable_error(self):
        """Test api_unavailable_error factory."""
        error = api_unavailable_error("Connection timeout")
        assert error.code == ErrorCode.E2001_API_UNAVAILABLE
        assert "unavailable" in error.message
        assert error.details["error"] == "Connection timeout"

    def test_api_unavailable_error_no_details(self):
        """Test api_unavailable_error without details."""
        error = api_unavailable_error()
        assert error.code == ErrorCode.E2001_API_UNAVAILABLE
        assert error.details is None

    def test_rate_limit_error(self):
        """Test rate_limit_error factory."""
        error = rate_limit_error(30)
        assert error.code == ErrorCode.E2002_RATE_LIMITED
        assert "rate limit" in error.message.lower()
        assert "30" in error.suggestion

    def test_rate_limit_error_no_retry_after(self):
        """Test rate_limit_error without retry_after."""
        error = rate_limit_error()
        assert error.code == ErrorCode.E2002_RATE_LIMITED
        assert "60" in error.suggestion  # Default

    def test_auth_error(self):
        """Test auth_error factory."""
        error = auth_error()
        assert error.code == ErrorCode.E2003_AUTH_FAILED
        assert "Authentication failed" in error.message
        assert error.recoverable is False

    def test_not_found_error(self):
        """Test not_found_error factory."""
        error = not_found_error("GLBX.MDP3/invalid_symbol")
        assert error.code == ErrorCode.E2004_NOT_FOUND
        assert "not found" in error.message.lower()

    def test_api_error(self):
        """Test api_error factory."""
        error = api_error("Server error", "500 Internal Server Error")
        assert error.code == ErrorCode.E2005_API_ERROR
        assert "Server error" in error.message

    def test_file_not_found_error(self):
        """Test file_not_found_error factory."""
        error = file_not_found_error("/path/to/file.dbn")
        assert error.code == ErrorCode.E3001_FILE_NOT_FOUND
        assert "not found" in error.message.lower()
        assert error.details["file_path"] == "/path/to/file.dbn"

    def test_invalid_path_error(self):
        """Test invalid_path_error factory."""
        error = invalid_path_error("../etc/passwd", "Path traversal not allowed")
        assert error.code == ErrorCode.E3002_INVALID_PATH
        assert "Invalid path" in error.message
        assert error.details["reason"] == "Path traversal not allowed"

    def test_write_error(self):
        """Test write_error factory."""
        error = write_error("/path/to/file.dbn", "Permission denied")
        assert error.code == ErrorCode.E3003_WRITE_ERROR
        assert "write" in error.message.lower()

    def test_read_error(self):
        """Test read_error factory."""
        error = read_error("/path/to/file.dbn", "Invalid format")
        assert error.code == ErrorCode.E3004_READ_ERROR
        assert "read" in error.message.lower()

    def test_no_data_error(self):
        """Test no_data_error factory."""
        error = no_data_error("AAPL from 2024-01-01 to 2024-01-02")
        assert error.code == ErrorCode.E4001_NO_DATA
        assert "No data" in error.message

    def test_parse_error(self):
        """Test parse_error factory."""
        error = parse_error("DBN file", "Invalid header")
        assert error.code == ErrorCode.E4002_PARSE_ERROR
        assert "Failed to parse" in error.message
