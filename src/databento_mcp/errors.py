"""Structured error handling module for the Databento MCP server."""
from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass, field


class ErrorCode(Enum):
    """Error codes for categorizing MCP server errors."""
    
    # Validation Errors (E1xxx)
    E1001_INVALID_DATE = "E1001"
    E1002_INVALID_SYMBOLS = "E1002"
    E1003_INVALID_DATASET = "E1003"
    E1004_INVALID_SCHEMA = "E1004"
    E1005_INVALID_PARAMETER = "E1005"
    E1006_DATE_RANGE_INVALID = "E1006"
    
    # API Errors (E2xxx)
    E2001_API_UNAVAILABLE = "E2001"
    E2002_RATE_LIMITED = "E2002"
    E2003_AUTH_FAILED = "E2003"
    E2004_NOT_FOUND = "E2004"
    E2005_API_ERROR = "E2005"
    
    # File Errors (E3xxx)
    E3001_FILE_NOT_FOUND = "E3001"
    E3002_INVALID_PATH = "E3002"
    E3003_WRITE_ERROR = "E3003"
    E3004_READ_ERROR = "E3004"
    
    # Data Errors (E4xxx)
    E4001_NO_DATA = "E4001"
    E4002_PARSE_ERROR = "E4002"


@dataclass
class MCPError:
    """Structured error with code, message, and recovery suggestions."""
    
    code: ErrorCode
    message: str
    suggestion: str
    details: Optional[Dict[str, Any]] = field(default=None)
    recoverable: bool = True
    
    def to_response(self) -> str:
        """Format error for MCP TextContent response."""
        response = f"❌ Error [{self.code.value}]: {self.message}\n\n"
        response += f"💡 Suggestion: {self.suggestion}\n"
        if self.details:
            response += "\n📋 Details:\n"
            for key, value in self.details.items():
                response += f"  - {key}: {value}\n"
        if self.recoverable:
            response += "\n✅ This error is recoverable. Please fix the issue and try again."
        else:
            response += "\n⚠️ This error may require configuration changes. Contact support@databento.com if the issue persists."
        return response


# Pre-built error factories
def validation_error(param: str, message: str, suggestion: str) -> MCPError:
    """Create a validation error for an invalid parameter."""
    return MCPError(
        code=ErrorCode.E1005_INVALID_PARAMETER,
        message=f"Invalid {param}: {message}",
        suggestion=suggestion,
        details={"parameter": param}
    )


def invalid_date_error(date_str: str, field_name: str = "date") -> MCPError:
    """Create an error for invalid date format."""
    return MCPError(
        code=ErrorCode.E1001_INVALID_DATE,
        message=f"Invalid {field_name}: {date_str}",
        suggestion="Use YYYY-MM-DD format (e.g., '2024-01-15') or ISO 8601 format (e.g., '2024-01-15T10:30:00Z').",
        details={"field": field_name, "value": date_str}
    )


def invalid_symbols_error(symbols: str) -> MCPError:
    """Create an error for invalid symbols."""
    return MCPError(
        code=ErrorCode.E1002_INVALID_SYMBOLS,
        message=f"Invalid symbols: {symbols}",
        suggestion="Provide a comma-separated list of valid symbols (e.g., 'AAPL, MSFT'). Symbols can contain alphanumeric characters, dots, dashes, and underscores.",
        details={"symbols": symbols}
    )


def invalid_dataset_error(dataset: str) -> MCPError:
    """Create an error for invalid dataset name."""
    return MCPError(
        code=ErrorCode.E1003_INVALID_DATASET,
        message=f"Invalid dataset: {dataset}",
        suggestion="Dataset must follow VENUE.FORMAT pattern (e.g., 'GLBX.MDP3', 'XNAS.ITCH'). Use `list_datasets` to see available datasets.",
        details={"dataset": dataset}
    )


def invalid_schema_error(schema: str, valid_schemas: list) -> MCPError:
    """Create an error for invalid schema."""
    return MCPError(
        code=ErrorCode.E1004_INVALID_SCHEMA,
        message=f"Invalid schema: {schema}",
        suggestion=f"Valid schemas are: {', '.join(valid_schemas)}",
        details={"schema": schema, "valid_schemas": valid_schemas}
    )


def date_range_error(start: str, end: str) -> MCPError:
    """Create an error for invalid date range."""
    return MCPError(
        code=ErrorCode.E1006_DATE_RANGE_INVALID,
        message=f"Invalid date range: start ({start}) is after end ({end})",
        suggestion="Ensure the start date is before or equal to the end date.",
        details={"start": start, "end": end}
    )


def api_unavailable_error(details: str = None) -> MCPError:
    """Create an error for API unavailability."""
    return MCPError(
        code=ErrorCode.E2001_API_UNAVAILABLE,
        message="Databento API is currently unavailable",
        suggestion="Wait a few minutes and try again. Check https://status.databento.com for service status.",
        details={"error": details} if details else None,
        recoverable=True
    )


def rate_limit_error(retry_after: int = None) -> MCPError:
    """Create an error for rate limiting."""
    return MCPError(
        code=ErrorCode.E2002_RATE_LIMITED,
        message="API rate limit exceeded",
        suggestion=f"Wait {retry_after or 60} seconds before retrying. Consider using smaller date ranges or fewer symbols.",
        details={"retry_after_seconds": retry_after} if retry_after else None
    )


def auth_error() -> MCPError:
    """Create an error for authentication failure."""
    return MCPError(
        code=ErrorCode.E2003_AUTH_FAILED,
        message="Authentication failed",
        suggestion="Check that DATABENTO_API_KEY is set correctly and the key is valid.",
        recoverable=False
    )


def not_found_error(resource: str) -> MCPError:
    """Create an error for resource not found."""
    return MCPError(
        code=ErrorCode.E2004_NOT_FOUND,
        message=f"Resource not found: {resource}",
        suggestion="Verify the resource exists. Use `list_datasets` to see available datasets.",
        details={"resource": resource}
    )


def api_error(message: str, details: str = None) -> MCPError:
    """Create a general API error."""
    return MCPError(
        code=ErrorCode.E2005_API_ERROR,
        message=message,
        suggestion="Check the error details and try again. If the problem persists, check API documentation.",
        details={"error": details} if details else None
    )


def file_not_found_error(file_path: str) -> MCPError:
    """Create an error for file not found."""
    return MCPError(
        code=ErrorCode.E3001_FILE_NOT_FOUND,
        message=f"File not found: {file_path}",
        suggestion="Check that the file path is correct and the file exists.",
        details={"file_path": file_path}
    )


def invalid_path_error(file_path: str, reason: str) -> MCPError:
    """Create an error for invalid file path."""
    return MCPError(
        code=ErrorCode.E3002_INVALID_PATH,
        message=f"Invalid path: {file_path}",
        suggestion="Ensure the path is valid and within the allowed directory (DATABENTO_DATA_DIR if set).",
        details={"file_path": file_path, "reason": reason}
    )


def write_error(file_path: str, reason: str) -> MCPError:
    """Create an error for file write failure."""
    return MCPError(
        code=ErrorCode.E3003_WRITE_ERROR,
        message=f"Failed to write file: {file_path}",
        suggestion="Check that you have write permissions and sufficient disk space.",
        details={"file_path": file_path, "reason": reason}
    )


def read_error(file_path: str, reason: str) -> MCPError:
    """Create an error for file read failure."""
    return MCPError(
        code=ErrorCode.E3004_READ_ERROR,
        message=f"Failed to read file: {file_path}",
        suggestion="Check that the file is a valid DBN or Parquet file and is not corrupted.",
        details={"file_path": file_path, "reason": reason}
    )


def no_data_error(query_info: str) -> MCPError:
    """Create an error for no data returned."""
    return MCPError(
        code=ErrorCode.E4001_NO_DATA,
        message=f"No data available: {query_info}",
        suggestion="Check that the symbol exists for the specified date range. Use `get_dataset_range` to verify data availability.",
        details={"query": query_info}
    )


def parse_error(data_type: str, reason: str) -> MCPError:
    """Create an error for data parsing failure."""
    return MCPError(
        code=ErrorCode.E4002_PARSE_ERROR,
        message=f"Failed to parse {data_type}",
        suggestion="Check that the data format is correct. This may indicate corrupted data.",
        details={"data_type": data_type, "reason": reason}
    )
