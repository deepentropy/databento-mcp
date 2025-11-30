"""Databento MCP Server - Provides access to Databento market data API."""
import logging
import os
import sys
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import databento as db
from mcp.server import Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    Prompt,
    PromptArgument,
    PromptMessage,
    GetPromptResult,
    TextResourceContents,
)
from mcp.server.stdio import stdio_server
from dotenv import load_dotenv

from cache import Cache
from connection_pool import get_pool
from metrics import get_metrics, TimedOperation
from async_io import read_dbn_file_async, write_parquet_async, read_parquet_async
from validation import (
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
from retry import with_retry, is_transient_error, RetryError  # noqa: F401 - exported for future use

# Load environment variables
load_dotenv()

# Configure logging
def _configure_logging() -> logging.Logger:
    """Configure logging based on DATABENTO_LOG_LEVEL environment variable."""
    log_level_str = os.getenv("DATABENTO_LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )
    
    # Create logger for this module
    logger = logging.getLogger("databento_mcp")
    logger.setLevel(log_level)
    
    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    logger.info(f"Databento MCP server logging initialized at level: {log_level_str}")
    return logger


logger = _configure_logging()

# Initialize cache (1 hour default TTL)
cache = Cache(cache_dir="cache", default_ttl=3600)

# Initialize Databento client using connection pool
api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("Error: DATABENTO_API_KEY environment variable not set", file=sys.stderr)
    sys.exit(1)

# Use connection pool for historical client
client = get_pool().get_historical_client()

# Create MCP server
app = Server("databento-mcp")

# Get allowed data directory from environment variable
ALLOWED_DATA_DIR = os.getenv("DATABENTO_DATA_DIR")

# Display limits for output
MAX_SYMBOLS_DISPLAY = 50
MAX_MAPPINGS_DISPLAY = 20


def ensure_dbn_extension(path: str, compression: str) -> str:
    """
    Ensure the path has the correct DBN file extension based on compression.
    
    Args:
        path: The file path
        compression: Compression type ("zstd" or "none")
        
    Returns:
        Path with correct extension
    """
    if compression == "zstd":
        if path.endswith(".dbn"):
            return path + ".zst"
        elif not path.endswith(".dbn.zst"):
            return path + ".dbn.zst"
    elif compression == "none":
        if not path.endswith(".dbn"):
            return path + ".dbn"
    return path


def ensure_parquet_extension(path: str) -> str:
    """
    Ensure the path has the .parquet extension.
    
    Args:
        path: The file path
        
    Returns:
        Path with .parquet extension
    """
    if not path.endswith(".parquet"):
        return path + ".parquet"
    return path


def validate_file_path(file_path: str, must_exist: bool = False) -> Path:
    """
    Validate and normalize a file path for security.
    
    Args:
        file_path: The path to validate
        must_exist: Whether the file must already exist
        
    Returns:
        Resolved Path object
        
    Raises:
        ValueError: If the path is invalid or outside allowed directory
        FileNotFoundError: If must_exist is True and file doesn't exist
    """
    # Convert to Path object
    path = Path(file_path)
    
    # Resolve to absolute path (follows symlinks)
    try:
        resolved_path = path.resolve()
    except (OSError, ValueError) as e:
        raise ValueError(f"Invalid file path: {e}")
    
    # If DATABENTO_DATA_DIR is set, enforce it
    if ALLOWED_DATA_DIR:
        allowed_dir = Path(ALLOWED_DATA_DIR).resolve()
        try:
            resolved_path.relative_to(allowed_dir)
        except ValueError:
            raise ValueError(
                f"File path must be within DATABENTO_DATA_DIR: {allowed_dir}"
            )
    else:
        # Without DATABENTO_DATA_DIR, ensure path doesn't escape current working directory
        # by checking that resolved path is within cwd or an absolute path was given
        cwd = Path.cwd().resolve()
        try:
            # Check if resolved path is within current directory
            resolved_path.relative_to(cwd)
        except ValueError:
            # Allow absolute paths that don't try to traverse
            if ".." in str(file_path):
                raise ValueError("Directory traversal (..) not allowed in file paths")
    
    # Check existence if required
    if must_exist and not resolved_path.exists():
        raise FileNotFoundError(f"File not found: {resolved_path}")
    
    # Check parent directory exists for write operations
    if not must_exist and not resolved_path.parent.exists():
        raise ValueError(f"Parent directory does not exist: {resolved_path.parent}")
    
    return resolved_path


def serialize_data(data: Any) -> str:
    """Convert data to JSON string, handling special types."""
    if hasattr(data, 'to_json'):
        return data.to_json()
    elif hasattr(data, 'to_dict'):
        return json.dumps(data.to_dict(), indent=2)
    elif hasattr(data, '__dict__'):
        return json.dumps(data.__dict__, default=str, indent=2)
    else:
        return json.dumps(data, default=str, indent=2)


@app.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources."""
    return [
        Resource(
            uri="databento://schemas",
            name="Databento Schema Reference",
            description="Documentation of available data schemas",
            mimeType="text/markdown"
        ),
        Resource(
            uri="databento://datasets",
            name="Databento Dataset Reference",
            description="Common datasets and their descriptions",
            mimeType="text/markdown"
        ),
        Resource(
            uri="databento://error-codes",
            name="Error Code Reference",
            description="Complete list of error codes and their meanings",
            mimeType="text/markdown"
        )
    ]


@app.read_resource()
async def read_resource(uri: str) -> list[TextResourceContents]:
    """Read a resource by URI."""
    if uri == "databento://schemas":
        content = """# Databento Schemas

## Trade Data
- `trades` - Individual trades with price, size, timestamp
- `tbbo` - Top of book best bid/offer

## Order Book
- `mbp-1` - Market by price (top level)
- `mbp-10` - Market by price (10 levels)
- `mbo` - Market by order (full book)

## OHLCV Bars
- `ohlcv-1s` - 1-second bars
- `ohlcv-1m` - 1-minute bars
- `ohlcv-1h` - 1-hour bars
- `ohlcv-1d` - Daily bars

## Reference
- `definition` - Instrument definitions
- `statistics` - Market statistics
- `status` - Trading status
- `imbalance` - Auction imbalance"""
        return [TextResourceContents(uri=uri, mimeType="text/markdown", text=content)]

    elif uri == "databento://datasets":
        content = """# Common Databento Datasets

## Futures & Options
- `GLBX.MDP3` - CME Globex (ES, NQ, CL, etc.)
- `IFEU.IMPACT` - ICE Futures Europe

## US Equities
- `XNAS.ITCH` - Nasdaq TotalView
- `XNYS.PILLAR` - NYSE
- `DBEQ.BASIC` - Consolidated equities

## Options
- `OPRA.PILLAR` - US Options"""
        return [TextResourceContents(uri=uri, mimeType="text/markdown", text=content)]

    elif uri == "databento://error-codes":
        content = """# Error Code Reference

## E1xxx - Validation Errors
- E1001: Invalid date format
- E1002: Invalid symbols
- E1003: Invalid dataset
- E1004: Invalid schema
- E1005: Invalid parameter
- E1006: Invalid date range

## E2xxx - API Errors
- E2001: API unavailable
- E2002: Rate limited
- E2003: Authentication failed
- E2004: Resource not found
- E2005: General API error

## E3xxx - File Errors
- E3001: File not found
- E3002: Invalid path
- E3003: Write error
- E3004: Read error

## E4xxx - Data Errors
- E4001: No data available
- E4002: Parse error"""
        return [TextResourceContents(uri=uri, mimeType="text/markdown", text=content)]

    else:
        raise ValueError(f"Unknown resource: {uri}")


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    """List available prompts for guiding Claude."""
    return [
        Prompt(
            name="market-data-workflow",
            description="Step-by-step guide for retrieving market data from Databento",
            arguments=[]
        ),
        Prompt(
            name="cost-aware-query",
            description="How to estimate costs before running expensive queries",
            arguments=[
                PromptArgument(
                    name="dataset",
                    description="The dataset you want to query",
                    required=False
                )
            ]
        ),
        Prompt(
            name="troubleshooting",
            description="Diagnose and resolve common issues with the Databento MCP server",
            arguments=[]
        )
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: dict = None) -> GetPromptResult:
    """Get a prompt by name."""
    if name == "market-data-workflow":
        return GetPromptResult(
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text="""# Market Data Retrieval Workflow

## Step 1: Discover Available Data
Use `list_datasets` to see all available Databento datasets.

## Step 2: Check Data Availability
Use `get_dataset_range` to see the date range for your chosen dataset.

## Step 3: Estimate Costs (Important!)
ALWAYS use `get_cost` before retrieving large amounts of data:
- Estimates cost in USD
- Shows record count
- Helps avoid unexpected charges

## Step 4: Retrieve Data
Use `get_historical_data` for historical data or `get_live_data` for real-time streaming.

## Step 5: Export if Needed
Use `export_to_parquet` or `write_dbn_file` to save data locally.

## Tips:
- Start with small date ranges
- Use the cache (data is cached for 1 hour)
- Check `get_session_info` for trading hours context"""
                    )
                )
            ]
        )
    elif name == "cost-aware-query":
        dataset = arguments.get("dataset", "GLBX.MDP3") if arguments else "GLBX.MDP3"
        return GetPromptResult(
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text=f"""# Cost-Aware Querying Guide

Before retrieving data from {dataset}, follow these steps:

## 1. Always Check Cost First
```
get_cost(
    dataset="{dataset}",
    symbols="YOUR_SYMBOL",
    schema="trades",
    start="2024-01-01",
    end="2024-01-02"
)
```

## 2. Cost Factors
- **Schema**: `trades` and `mbo` are most expensive, `ohlcv-1d` is cheapest
- **Date Range**: Longer ranges = higher cost
- **Symbols**: More symbols = higher cost

## 3. Cost-Saving Tips
- Use `ohlcv-*` schemas instead of `trades` when possible
- Limit date ranges
- Use batch jobs for large downloads (cheaper than real-time)

## 4. If Cost is Too High
- Reduce date range
- Use a less granular schema
- Submit a batch job instead"""
                    )
                )
            ]
        )
    elif name == "troubleshooting":
        return GetPromptResult(
            messages=[
                PromptMessage(
                    role="user",
                    content=TextContent(
                        type="text",
                        text="""# Troubleshooting Guide

## Check Server Health First
Run `health_check` with `verbose=true` to diagnose issues.

## Common Issues

### E1xxx - Validation Errors
- **E1001**: Invalid date format. Use YYYY-MM-DD or ISO 8601.
- **E1002**: Invalid symbols. Check symbol format for your dataset.
- **E1003**: Invalid dataset. Use `list_datasets` to see valid options.

### E2xxx - API Errors
- **E2001**: API unavailable. Check status.databento.com
- **E2002**: Rate limited. Wait 60 seconds and retry.
- **E2003**: Auth failed. Check DATABENTO_API_KEY.

### E3xxx - File Errors
- **E3001**: File not found. Check the file path.
- **E3002**: Invalid path. Paths must be within DATABENTO_DATA_DIR.

## Still Having Issues?
1. Check logs (set DATABENTO_LOG_LEVEL=DEBUG)
2. Clear cache with `clear_cache`
3. Verify API key at databento.com/portal"""
                    )
                )
            ]
        )
    else:
        raise ValueError(f"Unknown prompt: {name}")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="health_check",
            description="Check the health and connectivity of the Databento API. Use this to diagnose connection issues or verify the server is working properly.",
            inputSchema={
                "type": "object",
                "properties": {
                    "verbose": {
                        "type": "boolean",
                        "description": "Include detailed diagnostic information",
                        "default": False
                    }
                }
            }
        ),
        Tool(
            name="get_historical_data",
            description="Retrieve historical market data for symbols from Databento",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3', 'XNAS.ITCH')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols to retrieve"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Data schema (e.g., 'trades', 'ohlcv-1m', 'mbp-1', 'tbbo')",
                        "default": "trades"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of records to return (default: 1000)",
                        "default": 1000
                    }
                },
                "required": ["dataset", "symbols", "start", "end"]
            }
        ),
        Tool(
            name="get_symbol_metadata",
            description="Get metadata for symbols including symbology mappings and instrument definitions",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional)"
                    }
                },
                "required": ["dataset", "symbols", "start"]
            }
        ),
        Tool(
            name="search_instruments",
            description="Search for instruments in a dataset",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name to search in"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Symbol pattern to search for (supports wildcards)"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format"
                    }
                },
                "required": ["dataset", "start"]
            }
        ),
        Tool(
            name="list_datasets",
            description="List all available datasets from Databento",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="clear_cache",
            description="Clear the API response cache",
            inputSchema={
                "type": "object",
                "properties": {
                    "expired_only": {
                        "type": "boolean",
                        "description": "Only clear expired entries (default: false)",
                        "default": False
                    }
                }
            }
        ),
        Tool(
            name="get_cost",
            description="Estimate the cost of a historical data query before executing it",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3', 'XNAS.ITCH')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Data schema (e.g., 'trades', 'ohlcv-1m', 'mbp-1')",
                        "default": "trades"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD or ISO 8601 datetime)"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD or ISO 8601 datetime)"
                    }
                },
                "required": ["dataset", "symbols", "schema", "start", "end"]
            }
        ),
        Tool(
            name="get_live_data",
            description="Subscribe to real-time market data for a limited duration",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Data schema (e.g., 'trades', 'mbp-1', 'ohlcv-1s')",
                        "default": "trades"
                    },
                    "duration": {
                        "type": "integer",
                        "description": "How long to stream data in seconds (default: 10, max: 60)",
                        "default": 10
                    }
                },
                "required": ["dataset", "symbols"]
            }
        ),
        Tool(
            name="resolve_symbols",
            description="Resolve symbols between different symbology types",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols to resolve"
                    },
                    "stype_in": {
                        "type": "string",
                        "description": "Input symbol type (e.g., 'raw_symbol', 'instrument_id', 'continuous', 'parent')",
                        "default": "raw_symbol"
                    },
                    "stype_out": {
                        "type": "string",
                        "description": "Output symbol type (e.g., 'instrument_id', 'raw_symbol')",
                        "default": "instrument_id"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date for resolution (YYYY-MM-DD)"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date for resolution (YYYY-MM-DD, optional)"
                    }
                },
                "required": ["dataset", "symbols", "stype_in", "stype_out", "start"]
            }
        ),
        # Batch Job Management Tools
        Tool(
            name="submit_batch_job",
            description="Submit a batch data download job for large historical datasets",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Data schema (e.g., 'trades', 'ohlcv-1m')"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD or ISO 8601)"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD or ISO 8601)"
                    },
                    "encoding": {
                        "type": "string",
                        "description": "Output encoding (default: 'dbn')",
                        "enum": ["dbn", "csv", "json"],
                        "default": "dbn"
                    },
                    "compression": {
                        "type": "string",
                        "description": "Compression type (default: 'zstd')",
                        "enum": ["none", "zstd"],
                        "default": "zstd"
                    },
                    "split_duration": {
                        "type": "string",
                        "description": "Split files by duration (default: 'day')",
                        "enum": ["day", "week", "month", "none"],
                        "default": "day"
                    }
                },
                "required": ["dataset", "symbols", "schema", "start", "end"]
            }
        ),
        Tool(
            name="list_batch_jobs",
            description="List all batch jobs with their current status",
            inputSchema={
                "type": "object",
                "properties": {
                    "states": {
                        "type": "string",
                        "description": "Filter by states (comma-separated: 'received', 'queued', 'processing', 'done', 'expired')",
                        "default": "queued,processing,done"
                    },
                    "since": {
                        "type": "string",
                        "description": "Only show jobs since this date (ISO 8601)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of jobs to return (default: 20)",
                        "default": 20
                    }
                }
            }
        ),
        Tool(
            name="get_batch_job_files",
            description="Get download information for a completed batch job",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The batch job ID"
                    }
                },
                "required": ["job_id"]
            }
        ),
        # Session Detection Tool
        Tool(
            name="get_session_info",
            description="Identify the current trading session based on time",
            inputSchema={
                "type": "object",
                "properties": {
                    "timestamp": {
                        "type": "string",
                        "description": "ISO 8601 timestamp (optional, defaults to current time)"
                    }
                }
            }
        ),
        # Enhanced Metadata Tools
        Tool(
            name="list_publishers",
            description="List data publishers with their details",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Filter by dataset (optional)"
                    }
                }
            }
        ),
        Tool(
            name="list_fields",
            description="List fields available for a specific schema",
            inputSchema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Schema name (e.g., 'trades', 'mbp-1')"
                    },
                    "encoding": {
                        "type": "string",
                        "description": "Encoding format (default: 'json')",
                        "enum": ["dbn", "csv", "json"],
                        "default": "json"
                    }
                },
                "required": ["schema"]
            }
        ),
        Tool(
            name="get_dataset_range",
            description="Get the available date range for a dataset",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name"
                    }
                },
                "required": ["dataset"]
            }
        ),
        # DBN File Processing Tools
        Tool(
            name="read_dbn_file",
            description="Read and parse a DBN file, returning the records as structured data",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the DBN file (can be .dbn or .dbn.zst for zstd-compressed)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of records to return (default: 1000)",
                        "default": 1000
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number of records to skip (default: 0)",
                        "default": 0
                    }
                },
                "required": ["file_path"]
            }
        ),
        Tool(
            name="get_dbn_metadata",
            description="Get only the metadata from a DBN file without reading all records",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the DBN file"
                    }
                },
                "required": ["file_path"]
            }
        ),
        Tool(
            name="write_dbn_file",
            description="Write historical data query results directly to a DBN file",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Data schema (e.g., 'trades', 'ohlcv-1m')"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD or ISO 8601)"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD or ISO 8601)"
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Path for output file"
                    },
                    "compression": {
                        "type": "string",
                        "description": "Compression type (default: 'zstd')",
                        "enum": ["none", "zstd"],
                        "default": "zstd"
                    }
                },
                "required": ["dataset", "symbols", "schema", "start", "end", "output_path"]
            }
        ),
        # Parquet Export Tools
        Tool(
            name="convert_dbn_to_parquet",
            description="Convert a DBN file to Parquet format",
            inputSchema={
                "type": "object",
                "properties": {
                    "input_path": {
                        "type": "string",
                        "description": "Path to the input DBN file"
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Path for output Parquet file (optional, defaults to input_path with .parquet extension)"
                    },
                    "compression": {
                        "type": "string",
                        "description": "Parquet compression (default: 'snappy')",
                        "enum": ["snappy", "gzip", "zstd", "none"],
                        "default": "snappy"
                    }
                },
                "required": ["input_path"]
            }
        ),
        Tool(
            name="export_to_parquet",
            description="Query historical data and export directly to Parquet format",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Dataset name (e.g., 'GLBX.MDP3')"
                    },
                    "symbols": {
                        "type": "string",
                        "description": "Comma-separated list of symbols"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Data schema (e.g., 'trades', 'ohlcv-1m')"
                    },
                    "start": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD or ISO 8601)"
                    },
                    "end": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD or ISO 8601)"
                    },
                    "output_path": {
                        "type": "string",
                        "description": "Path for output Parquet file"
                    },
                    "compression": {
                        "type": "string",
                        "description": "Parquet compression (default: 'snappy')",
                        "enum": ["snappy", "gzip", "zstd", "none"],
                        "default": "snappy"
                    }
                },
                "required": ["dataset", "symbols", "schema", "start", "end", "output_path"]
            }
        ),
        Tool(
            name="read_parquet_file",
            description="Read a Parquet file and return the data",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to the Parquet file"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of records to return (default: 1000)",
                        "default": 1000
                    },
                    "columns": {
                        "type": "string",
                        "description": "Comma-separated list of columns to read (optional, reads all if not specified)"
                    }
                },
                "required": ["file_path"]
            }
        ),
        Tool(
            name="get_metrics",
            description="Get server performance metrics and usage statistics",
            inputSchema={
                "type": "object",
                "properties": {
                    "reset": {
                        "type": "boolean",
                        "description": "Reset metrics after retrieval",
                        "default": False
                    }
                }
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls."""
    logger.info(f"Tool call: {name} with arguments: {arguments}")

    if name == "health_check":
        verbose = arguments.get("verbose", False) if arguments else False
        
        try:
            # Test API connectivity
            start_time = time.perf_counter()
            datasets = client.metadata.list_datasets()
            response_time = time.perf_counter() - start_time
            
            # Build health status
            result = "🟢 Health Check: HEALTHY\n\n"
            result += "✅ API Connectivity: OK\n"
            result += "✅ Authentication: Valid\n"
            result += f"✅ Response Time: {response_time*1000:.0f}ms\n"
            result += f"✅ Datasets Available: {len(datasets)}\n"
            
            if verbose:
                result += "\n📋 Diagnostic Details:\n"
                # Safely display API key suffix (only if key is long enough)
                if api_key and len(api_key) >= 8:
                    key_display = f"{'*' * 8}...{api_key[-4:]}"
                else:
                    key_display = "Set (hidden)" if api_key else "Not set"
                result += f"  - API Key: {key_display}\n"
                result += f"  - Log Level: {os.getenv('DATABENTO_LOG_LEVEL', 'INFO')}\n"
                result += f"  - Data Directory: {ALLOWED_DATA_DIR or 'Not restricted'}\n"
                result += f"  - Cache Directory: {cache.cache_dir}\n"
                result += "\n📊 Sample Datasets:\n"
                for dataset in datasets[:5]:
                    result += f"  - {dataset}\n"
                if len(datasets) > 5:
                    result += f"  ... and {len(datasets) - 5} more\n"
            
            logger.info("Health check passed")
            return [TextContent(type="text", text=result)]
            
        except Exception as e:
            error_str = str(e).lower()
            result = "🔴 Health Check: UNHEALTHY\n\n"
            
            if "401" in error_str or "auth" in error_str or "unauthorized" in error_str:
                result += "❌ Authentication: FAILED\n"
                result += "💡 Check that DATABENTO_API_KEY is set correctly.\n"
            elif "429" in error_str or "rate" in error_str:
                result += "⚠️ Rate Limited\n"
                result += "💡 Wait 60 seconds before retrying.\n"
            elif "timeout" in error_str or "connection" in error_str:
                result += "❌ API Connectivity: FAILED\n"
                result += "💡 Check your internet connection or visit status.databento.com\n"
            else:
                result += f"❌ Error: {str(e)}\n"
                result += "💡 Check logs for more details (set DATABENTO_LOG_LEVEL=DEBUG).\n"
            
            if verbose:
                result += "\n📋 Error Details:\n"
                result += f"  - Exception Type: {type(e).__name__}\n"
                result += f"  - Message: {str(e)}\n"
            
            logger.error(f"Health check failed: {e}", exc_info=True)
            return [TextContent(type="text", text=result)]

    elif name == "get_historical_data":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            validate_date_format(arguments["start"], "start")
            validate_date_format(arguments["end"], "end")
            validate_date_range(arguments["start"], arguments["end"])
            schema = arguments.get("schema", "trades")
            validate_schema(schema)
            limit = arguments.get("limit", 1000)
            validate_numeric_range(limit, "limit", min_value=1, max_value=100000)
        except ValidationError as e:
            logger.warning(f"Validation error in get_historical_data: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        start = arguments["start"]
        end = arguments["end"]

        # Create cache key
        cache_key = f"historical:{dataset}:{','.join(sorted(symbols))}:{start}:{end}:{schema}:{limit}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for get_historical_data: {cache_key}")
            get_metrics().record_cache_hit()
            return [TextContent(
                type="text",
                text=f"[Cached] Historical data for {', '.join(symbols)}:\n\n{cached_data}"
            )]

        get_metrics().record_cache_miss()

        try:
            with TimedOperation("get_historical_data"):
                # Fetch data from Databento using pooled client
                logger.debug("Fetching historical data from Databento API")
                get_metrics().record_api_call()
                pool_client = get_pool().get_historical_client()
                data = pool_client.timeseries.get_range(
                    dataset=dataset,
                    symbols=symbols,
                    start=start,
                    end=end,
                    schema=schema,
                    limit=limit
                )

                # Convert to DataFrame for easier viewing
                df = data.to_df()

            # Format response
            result = f"Historical {schema} data for {', '.join(symbols)}:\n"
            result += f"Dataset: {dataset}\n"
            result += f"Period: {start} to {end}\n"
            result += f"Records returned: {len(df)}\n\n"
            result += f"Sample data (first 10 rows):\n{df.head(10).to_string()}\n\n"
            result += f"Summary statistics:\n{df.describe().to_string()}"

            # Cache the result
            cache.set(cache_key, result, ttl=3600)
            
            logger.info(f"Successfully retrieved {len(df)} records for get_historical_data")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_historical_data: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error retrieving historical data: {str(e)}")]

    elif name == "get_symbol_metadata":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            validate_date_format(arguments["start"], "start")
            if arguments.get("end"):
                validate_date_format(arguments["end"], "end")
        except ValidationError as e:
            logger.warning(f"Validation error in get_symbol_metadata: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        start = arguments["start"]
        end = arguments.get("end")

        # Create cache key
        cache_key = f"metadata:{dataset}:{','.join(sorted(symbols))}:{start}:{end}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for get_symbol_metadata: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Metadata:\n\n{cached_data}"
            )]

        try:
            # Fetch metadata
            logger.debug("Fetching symbol metadata from Databento API")
            metadata = client.metadata.get_dataset_range(
                dataset=dataset,
                symbols=symbols,
                start=start,
                end=end
            )

            # Format response
            result = f"Metadata for {', '.join(symbols)} in {dataset}:\n\n"

            for record in metadata:
                result += f"Symbol: {record.symbol}\n"
                result += f"  Instrument ID: {record.instrument_id}\n"
                result += f"  Start: {record.start_date}\n"
                result += f"  End: {record.end_date}\n"
                if hasattr(record, 'name'):
                    result += f"  Name: {record.name}\n"
                if hasattr(record, 'asset'):
                    result += f"  Asset: {record.asset}\n"
                if hasattr(record, 'currency'):
                    result += f"  Currency: {record.currency}\n"
                result += "\n"

            # Cache the result
            cache.set(cache_key, result, ttl=7200)  # 2 hour TTL for metadata
            
            logger.info("Successfully retrieved metadata for get_symbol_metadata")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_symbol_metadata: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error retrieving metadata: {str(e)}")]

    elif name == "search_instruments":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            validate_date_format(arguments["start"], "start")
        except ValidationError as e:
            logger.warning(f"Validation error in search_instruments: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        symbols = arguments.get("symbols", "*")
        start = arguments["start"]

        # Create cache key
        cache_key = f"search:{dataset}:{symbols}:{start}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for search_instruments: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Search results:\n\n{cached_data}"
            )]

        try:
            # Search for instruments
            logger.debug("Searching instruments from Databento API")
            instruments = client.metadata.get_dataset_range(
                dataset=dataset,
                symbols=[symbols] if symbols else ["*"],
                start=start
            )

            # Format response
            result = f"Instruments matching '{symbols}' in {dataset}:\n\n"

            count = 0
            for record in instruments[:50]:  # Limit to first 50 results
                result += f"{record.symbol}"
                if hasattr(record, 'name'):
                    result += f" - {record.name}"
                result += "\n"
                count += 1

            if count == 0:
                result = "No instruments found matching the criteria."
            elif count == 50:
                result += "\n(Showing first 50 results)"

            # Cache the result
            cache.set(cache_key, result, ttl=7200)  # 2 hour TTL
            
            logger.info(f"Successfully found {count} instruments for search_instruments")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in search_instruments: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error searching instruments: {str(e)}")]

    elif name == "list_datasets":
        # Create cache key
        cache_key = "datasets:list"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug("Cache hit for list_datasets")
            return [TextContent(
                type="text",
                text=f"[Cached] Available datasets:\n\n{cached_data}"
            )]

        try:
            # List datasets
            logger.debug("Listing datasets from Databento API")
            datasets = client.metadata.list_datasets()

            # Format response
            result = "Available Databento datasets:\n\n"
            for dataset in datasets:
                result += f"- {dataset}\n"

            # Cache the result
            cache.set(cache_key, result, ttl=86400)  # 24 hour TTL
            
            logger.info(f"Successfully listed {len(datasets)} datasets")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in list_datasets: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error listing datasets: {str(e)}")]

    elif name == "clear_cache":
        expired_only = arguments.get("expired_only", False)

        try:
            if expired_only:
                cache.clear_expired()
                logger.info("Cleared expired cache entries")
                return [TextContent(type="text", text="Expired cache entries cleared successfully.")]
            else:
                cache.clear()
                logger.info("Cleared all cache entries")
                return [TextContent(type="text", text="All cache entries cleared successfully.")]
        except Exception as e:
            logger.error(f"Error in clear_cache: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error clearing cache: {str(e)}")]

    elif name == "get_cost":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            schema = arguments.get("schema", "trades")
            validate_schema(schema)
            validate_date_format(arguments["start"], "start")
            validate_date_format(arguments["end"], "end")
            validate_date_range(arguments["start"], arguments["end"])
        except ValidationError as e:
            logger.warning(f"Validation error in get_cost: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        start = arguments["start"]
        end = arguments["end"]

        # Create cache key
        cache_key = f"cost:{dataset}:{','.join(sorted(symbols))}:{schema}:{start}:{end}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for get_cost: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Cost estimate:\n\n{cached_data}"
            )]

        try:
            # Get cost estimate
            logger.debug("Fetching cost estimate from Databento API")
            cost = client.metadata.get_cost(
                dataset=dataset,
                symbols=symbols,
                schema=schema,
                start=start,
                end=end
            )

            # Get record count
            record_count = client.metadata.get_record_count(
                dataset=dataset,
                symbols=symbols,
                schema=schema,
                start=start,
                end=end
            )

            # Get billable size
            billable_size = client.metadata.get_billable_size(
                dataset=dataset,
                symbols=symbols,
                schema=schema,
                start=start,
                end=end
            )

            # Format response
            result = f"Cost Estimate for {', '.join(symbols)}:\n"
            result += f"Dataset: {dataset}\n"
            result += f"Schema: {schema}\n"
            result += f"Period: {start} to {end}\n\n"
            result += f"Estimated Cost: ${cost:.4f} USD\n"
            result += f"Estimated Records: {record_count:,}\n"
            result += f"Estimated Size: {billable_size:,} bytes ({billable_size / (1024*1024):.2f} MB)\n"

            # Cache the result (shorter TTL as prices may change)
            cache.set(cache_key, result, ttl=1800)  # 30 minute TTL
            
            logger.info(f"Successfully estimated cost: ${cost:.4f}")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_cost: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error estimating cost: {str(e)}")]

    elif name == "get_live_data":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            schema = arguments.get("schema", "trades")
            validate_schema(schema)
            duration = arguments.get("duration", 10)
            validate_numeric_range(duration, "duration", min_value=1, max_value=60)
        except ValidationError as e:
            logger.warning(f"Validation error in get_live_data: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]

        try:
            with TimedOperation("get_live_data"):
                # Create Live client from pool (new client each time as Live clients aren't reusable)
                logger.debug(f"Starting live data stream for {duration} seconds")
                get_metrics().record_api_call()
                live_client = get_pool().get_live_client()

                # Subscribe to data
                live_client.subscribe(
                    dataset=dataset,
                    schema=schema,
                    symbols=symbols,
                )

                # Collect records for the specified duration
                records = []
                start_time = time.time()

                # Use iteration with timeout
                for record in live_client:
                    records.append(record)
                    elapsed = time.time() - start_time
                    if elapsed >= duration:
                        break

                # Stop and cleanup
                live_client.stop()

            # Format response
            result = f"Live Data for {', '.join(symbols)}:\n"
            result += f"Dataset: {dataset}\n"
            result += f"Schema: {schema}\n"
            result += f"Duration: {duration} seconds\n"
            result += f"Records received: {len(records)}\n\n"

            if records:
                result += "Sample records (first 10):\n"
                for i, record in enumerate(records[:10]):
                    result += f"  {i + 1}. {type(record).__name__}: "
                    if hasattr(record, "ts_event"):
                        result += f"ts_event={record.ts_event} "
                    if hasattr(record, "price"):
                        result += f"price={record.price} "
                    if hasattr(record, "size"):
                        result += f"size={record.size} "
                    if hasattr(record, "symbol"):
                        result += f"symbol={record.symbol} "
                    result += "\n"
            else:
                result += "No records received during the streaming period.\n"

            logger.info(f"Successfully streamed {len(records)} records for get_live_data")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_live_data: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error streaming live data: {str(e)}")]

    elif name == "resolve_symbols":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            stype_in = arguments.get("stype_in", "raw_symbol")
            stype_out = arguments.get("stype_out", "instrument_id")
            validate_stype(stype_in, "stype_in")
            validate_stype(stype_out, "stype_out")
            validate_date_format(arguments["start"], "start")
            if arguments.get("end"):
                validate_date_format(arguments["end"], "end")
        except ValidationError as e:
            logger.warning(f"Validation error in resolve_symbols: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        start = arguments["start"]
        end = arguments.get("end")

        # Create cache key
        cache_key = f"resolve:{dataset}:{','.join(sorted(symbols))}:{stype_in}:{stype_out}:{start}:{end}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for resolve_symbols: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Symbol resolution:\n\n{cached_data}"
            )]

        try:
            # Resolve symbols
            logger.debug("Resolving symbols via Databento API")
            resolution = client.symbology.resolve(
                dataset=dataset,
                symbols=symbols,
                stype_in=stype_in,
                stype_out=stype_out,
                start_date=start,
                end_date=end
            )

            # Format response
            result = "Symbol Resolution:\n"
            result += f"Dataset: {dataset}\n"
            result += f"Input type: {stype_in}\n"
            result += f"Output type: {stype_out}\n"
            result += f"Period: {start}"
            if end:
                result += f" to {end}"
            result += "\n\n"

            # Extract mappings from response
            mappings = resolution.get("result", {})
            resolved_count = 0
            total_count = len(symbols)

            result += "Mappings:\n"
            for input_symbol, mapping_data in mappings.items():
                result += f"  {input_symbol}:\n"
                if isinstance(mapping_data, dict):
                    for date_range, output_symbol in mapping_data.items():
                        result += f"    {date_range}: {output_symbol}\n"
                        resolved_count += 1
                elif isinstance(mapping_data, list):
                    for item in mapping_data:
                        if isinstance(item, dict):
                            date_range = item.get("d", "N/A")
                            output_symbol = item.get("s", "N/A")
                            result += f"    {date_range}: {output_symbol}\n"
                        else:
                            result += f"    {item}\n"
                        resolved_count += 1
                else:
                    result += f"    {mapping_data}\n"
                    resolved_count += 1

            # Determine resolution status
            if resolved_count >= total_count:
                status = "full"
            elif resolved_count > 0:
                status = "partial"
            else:
                status = "none"

            result += f"\nResolution status: {status}\n"
            result += f"Symbols resolved: {resolved_count}/{total_count}\n"

            # Cache the result
            cache.set(cache_key, result, ttl=3600)  # 1 hour TTL
            
            logger.info(f"Successfully resolved {resolved_count}/{total_count} symbols")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in resolve_symbols: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error resolving symbols: {str(e)}")]

    elif name == "submit_batch_job":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            validate_schema(arguments["schema"])
            validate_date_format(arguments["start"], "start")
            validate_date_format(arguments["end"], "end")
            validate_date_range(arguments["start"], arguments["end"])
            encoding = arguments.get("encoding", "dbn")
            validate_encoding(encoding)
            compression = arguments.get("compression", "zstd")
            validate_compression(compression)
        except ValidationError as e:
            logger.warning(f"Validation error in submit_batch_job: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        schema = arguments["schema"]
        start = arguments["start"]
        end = arguments["end"]
        split_duration = arguments.get("split_duration", "day")

        try:
            # Submit batch job
            logger.debug("Submitting batch job to Databento API")
            job_info = client.batch.submit_job(
                dataset=dataset,
                symbols=symbols,
                schema=schema,
                start=start,
                end=end,
                encoding=encoding,
                compression=compression,
                split_duration=split_duration
            )

            # Format response
            result = "Batch Job Submitted:\n"
            result += f"Job ID: {job_info.get('job_id', 'N/A')}\n"
            result += f"State: {job_info.get('state', 'N/A')}\n"
            result += f"Dataset: {dataset}\n"
            result += f"Schema: {schema}\n"
            result += f"Symbols: {', '.join(symbols)}\n"
            result += f"Period: {start} to {end}\n"
            result += f"Encoding: {encoding}\n"
            result += f"Compression: {compression}\n"
            result += f"Split Duration: {split_duration}\n"

            # Include cost if available
            if "cost_usd" in job_info:
                result += f"\nEstimated Cost: ${job_info['cost_usd']:.4f} USD\n"
            if "ts_received" in job_info:
                result += f"Submitted: {job_info['ts_received']}\n"

            logger.info(f"Successfully submitted batch job: {job_info.get('job_id', 'N/A')}")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in submit_batch_job: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error submitting batch job: {str(e)}")]

    elif name == "list_batch_jobs":
        try:
            # Validate inputs
            limit = arguments.get("limit", 20)
            validate_numeric_range(limit, "limit", min_value=1, max_value=100)
            if arguments.get("since"):
                validate_date_format(arguments["since"], "since")
        except ValidationError as e:
            logger.warning(f"Validation error in list_batch_jobs: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        states = arguments.get("states", "queued,processing,done")
        since = arguments.get("since")

        # Create cache key
        cache_key = f"batch_jobs:{states}:{since}:{limit}"

        # Check cache with short TTL (batch job status changes frequently)
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for list_batch_jobs: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Batch jobs:\n\n{cached_data}"
            )]

        try:
            # Parse states
            state_list = [s.strip() for s in states.split(",")]

            # List batch jobs
            logger.debug("Listing batch jobs from Databento API")
            jobs = client.batch.list_jobs(states=state_list, since=since)

            # Limit results
            jobs = jobs[:limit]

            # Group jobs by state
            jobs_by_state = {}
            for job in jobs:
                state = job.get("state", "unknown")
                if state not in jobs_by_state:
                    jobs_by_state[state] = []
                jobs_by_state[state].append(job)

            # Format response
            result = f"Batch Jobs ({len(jobs)} total):\n\n"

            for state, state_jobs in jobs_by_state.items():
                result += f"=== {state.upper()} ({len(state_jobs)}) ===\n"
                for job in state_jobs:
                    result += f"  Job ID: {job.get('job_id', 'N/A')}\n"
                    result += f"    Dataset: {job.get('dataset', 'N/A')}\n"
                    result += f"    Schema: {job.get('schema', 'N/A')}\n"
                    if "cost_usd" in job:
                        result += f"    Cost: ${job['cost_usd']:.4f} USD\n"
                    if "ts_received" in job:
                        result += f"    Submitted: {job['ts_received']}\n"
                    if "ts_process_start" in job:
                        result += f"    Processing Started: {job['ts_process_start']}\n"
                    if "ts_process_done" in job:
                        result += f"    Completed: {job['ts_process_done']}\n"
                    result += "\n"

            if len(jobs) == 0:
                result = "No batch jobs found matching the criteria."

            # Cache with short TTL (5 minutes)
            cache.set(cache_key, result, ttl=300)
            
            logger.info(f"Successfully listed {len(jobs)} batch jobs")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in list_batch_jobs: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error listing batch jobs: {str(e)}")]

    elif name == "get_batch_job_files":
        job_id = arguments["job_id"]
        if not job_id:
            logger.warning("Validation error in get_batch_job_files: job_id cannot be empty")
            return [TextContent(type="text", text="Validation error: job_id cannot be empty")]

        # Create cache key
        cache_key = f"batch_files:{job_id}"

        # Check cache with short TTL
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for get_batch_job_files: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Batch job files:\n\n{cached_data}"
            )]

        try:
            # Get file list for job
            logger.debug(f"Getting files for batch job: {job_id}")
            files = client.batch.list_files(job_id=job_id)

            # Format response
            result = f"Batch Job Files for {job_id}:\n\n"

            if not files:
                result += "No files available yet. The job may still be processing.\n"
            else:
                total_size = 0
                for i, file_info in enumerate(files, 1):
                    result += f"File {i}:\n"
                    result += f"  Filename: {file_info.get('filename', 'N/A')}\n"
                    size = file_info.get('size', 0)
                    total_size += size
                    result += f"  Size: {size:,} bytes ({size / (1024*1024):.2f} MB)\n"
                    if "hash" in file_info:
                        result += f"  Hash: {file_info['hash']}\n"
                    if "urls" in file_info:
                        urls = file_info["urls"]
                        if isinstance(urls, dict) and "https" in urls:
                            result += f"  Download URL: {urls['https']}\n"
                        elif isinstance(urls, str):
                            result += f"  Download URL: {urls}\n"
                    if "ts_expiration" in file_info:
                        result += f"  Expires: {file_info['ts_expiration']}\n"
                    result += "\n"

                result += f"Total Files: {len(files)}\n"
                result += f"Total Size: {total_size:,} bytes ({total_size / (1024*1024):.2f} MB)\n"

            # Cache with short TTL (5 minutes)
            cache.set(cache_key, result, ttl=300)
            
            logger.info(f"Successfully retrieved {len(files)} files for batch job {job_id}")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_batch_job_files: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error getting batch job files: {str(e)}")]

    elif name == "get_session_info":
        timestamp_str = arguments.get("timestamp")

        try:
            # Parse timestamp or use current time
            if timestamp_str:
                try:
                    validate_date_format(timestamp_str, "timestamp")
                except ValidationError as e:
                    logger.warning(f"Validation error in get_session_info: {e}")
                    return [TextContent(type="text", text=f"Validation error: {str(e)}")]
                ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = datetime.now(timezone.utc)

            # Get UTC hour
            utc_hour = ts.hour

            # Determine trading session
            if 0 <= utc_hour < 7:
                session_name = "Asian"
                session_start = "00:00 UTC"
                session_end = "07:00 UTC"
            elif 7 <= utc_hour < 14:
                session_name = "London"
                session_start = "07:00 UTC"
                session_end = "14:00 UTC"
            elif 14 <= utc_hour < 22:
                session_name = "NY"
                session_start = "14:00 UTC"
                session_end = "22:00 UTC"
            else:
                session_name = "Off-hours"
                session_start = "22:00 UTC"
                session_end = "00:00 UTC"

            # Format response
            result = "Trading Session Info:\n\n"
            result += f"Current Session: {session_name}\n"
            result += f"Session Start: {session_start}\n"
            result += f"Session End: {session_end}\n"
            result += f"Current Timestamp: {ts.isoformat()}\n"
            result += f"UTC Hour: {utc_hour}\n"

            logger.info(f"Successfully determined session info: {session_name}")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_session_info: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error determining session info: {str(e)}")]

    elif name == "list_publishers":
        dataset_filter = arguments.get("dataset")
        if dataset_filter:
            try:
                validate_dataset(dataset_filter)
            except ValidationError as e:
                logger.warning(f"Validation error in list_publishers: {e}")
                return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        # Create cache key
        cache_key = f"publishers:{dataset_filter}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for list_publishers: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Publishers:\n\n{cached_data}"
            )]

        try:
            # Get publishers
            logger.debug("Listing publishers from Databento API")
            publishers = client.metadata.list_publishers()

            # Filter by dataset if specified
            if dataset_filter:
                publishers = [p for p in publishers if p.get("dataset") == dataset_filter]

            # Format response
            result = f"Data Publishers ({len(publishers)} total):\n\n"

            for pub in publishers:
                result += f"Publisher ID: {pub.get('publisher_id', 'N/A')}\n"
                result += f"  Dataset: {pub.get('dataset', 'N/A')}\n"
                result += f"  Venue: {pub.get('venue', 'N/A')}\n"
                if "description" in pub:
                    result += f"  Description: {pub['description']}\n"
                result += "\n"

            if len(publishers) == 0:
                result = "No publishers found matching the criteria."

            # Cache for 24 hours (publishers rarely change)
            cache.set(cache_key, result, ttl=86400)
            
            logger.info(f"Successfully listed {len(publishers)} publishers")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in list_publishers: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error listing publishers: {str(e)}")]

    elif name == "list_fields":
        try:
            # Validate inputs
            validate_schema(arguments["schema"])
            encoding = arguments.get("encoding", "json")
            validate_encoding(encoding)
        except ValidationError as e:
            logger.warning(f"Validation error in list_fields: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        schema = arguments["schema"]

        # Create cache key
        cache_key = f"fields:{schema}:{encoding}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for list_fields: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Fields:\n\n{cached_data}"
            )]

        try:
            # Get fields
            logger.debug(f"Listing fields for schema {schema}")
            fields = client.metadata.list_fields(schema=schema, encoding=encoding)

            # Format response
            result = f"Fields for schema '{schema}' (encoding: {encoding}):\n\n"

            for field in fields:
                result += f"{field.get('name', 'N/A')}\n"
                result += f"  Type: {field.get('type', 'N/A')}\n"
                if "description" in field:
                    result += f"  Description: {field['description']}\n"
                result += "\n"

            if len(fields) == 0:
                result = "No fields found for the specified schema."

            # Cache for 24 hours (field definitions rarely change)
            cache.set(cache_key, result, ttl=86400)
            
            logger.info(f"Successfully listed {len(fields)} fields for schema {schema}")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in list_fields: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error listing fields: {str(e)}")]

    elif name == "get_dataset_range":
        try:
            validate_dataset(arguments["dataset"])
        except ValidationError as e:
            logger.warning(f"Validation error in get_dataset_range: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]

        # Create cache key
        cache_key = f"dataset_range:{dataset}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for get_dataset_range: {cache_key}")
            return [TextContent(
                type="text",
                text=f"[Cached] Dataset range:\n\n{cached_data}"
            )]

        try:
            # Get dataset range
            logger.debug(f"Getting dataset range for {dataset}")
            range_info = client.metadata.get_dataset_range(dataset=dataset)

            # Format response
            result = f"Dataset Range for {dataset}:\n\n"
            result += f"Dataset: {dataset}\n"
            result += f"Start Date: {range_info.get('start_date', 'N/A')}\n"
            result += f"End Date: {range_info.get('end_date', 'ongoing')}\n"

            # Cache for 1 hour (dataset ranges can update)
            cache.set(cache_key, result, ttl=3600)
            
            logger.info(f"Successfully retrieved dataset range for {dataset}")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_dataset_range: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error getting dataset range: {str(e)}")]

    elif name == "read_dbn_file":
        try:
            # Validate inputs
            limit = arguments.get("limit", 1000)
            validate_numeric_range(limit, "limit", min_value=1, max_value=100000)
            offset = arguments.get("offset", 0)
            validate_numeric_range(offset, "offset", min_value=0)
        except ValidationError as e:
            logger.warning(f"Validation error in read_dbn_file: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        file_path = arguments["file_path"]

        try:
            # Validate file path
            resolved_path = validate_file_path(file_path, must_exist=True)

            # Read DBN file asynchronously
            logger.debug(f"Reading DBN file: {resolved_path}")
            metadata, df = await read_dbn_file_async(resolved_path, limit=0, offset=0)

            # Build metadata info
            result = "DBN File Contents:\n\n"
            result += "=== Metadata ===\n"
            result += f"Version: {metadata.version}\n"
            result += f"Dataset: {metadata.dataset}\n"
            result += f"Schema: {metadata.schema}\n"
            result += f"Start: {metadata.start}\n"
            result += f"End: {metadata.end}\n"
            result += f"Symbol Count: {metadata.symbol_cstr_len}\n"

            total_records = len(df)
            result += f"Total Records: {total_records}\n\n"

            # Apply offset and limit
            if offset > 0:
                df = df.iloc[offset:]
            if limit > 0:
                df = df.head(limit)

            result += f"=== Records (offset={offset}, limit={limit}) ===\n"
            result += f"Records returned: {len(df)}\n\n"

            if len(df) > 0:
                result += "Sample data (first 10 rows):\n"
                result += df.head(10).to_string()
                result += "\n"

            logger.info(f"Successfully read {len(df)} records from DBN file")
            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            logger.warning(f"File not found in read_dbn_file: {e}")
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            logger.warning(f"Invalid path in read_dbn_file: {e}")
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            logger.error(f"Error in read_dbn_file: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error reading DBN file: {str(e)}")]

    elif name == "get_dbn_metadata":
        file_path = arguments["file_path"]

        try:
            # Validate file path
            resolved_path = validate_file_path(file_path, must_exist=True)

            # Read DBN file using DBNStore
            logger.debug(f"Reading DBN metadata from: {resolved_path}")
            store = db.DBNStore.from_file(str(resolved_path))

            # Get metadata
            metadata = store.metadata

            # Build response
            result = "DBN File Metadata:\n\n"
            result += f"File: {resolved_path}\n"
            result += f"Version: {metadata.version}\n"
            result += f"Dataset: {metadata.dataset}\n"
            result += f"Schema: {metadata.schema}\n"
            result += f"Start: {metadata.start}\n"
            result += f"End: {metadata.end}\n"
            result += f"Symbol Count: {metadata.symbol_cstr_len}\n"

            # Get symbols if available
            if hasattr(metadata, 'symbols') and metadata.symbols:
                result += "\nSymbols:\n"
                for symbol in metadata.symbols[:MAX_SYMBOLS_DISPLAY]:
                    result += f"  - {symbol}\n"
                if len(metadata.symbols) > MAX_SYMBOLS_DISPLAY:
                    result += f"  ... and {len(metadata.symbols) - MAX_SYMBOLS_DISPLAY} more\n"

            # Get mappings if available
            if hasattr(store, 'symbology') and store.symbology:
                result += "\nSymbology Mappings:\n"
                mappings = store.symbology
                count = 0
                for key, value in mappings.items():
                    if count >= MAX_MAPPINGS_DISPLAY:
                        result += "  ... and more mappings\n"
                        break
                    result += f"  {key}: {value}\n"
                    count += 1

            logger.info(f"Successfully read DBN metadata from {file_path}")
            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            logger.warning(f"File not found in get_dbn_metadata: {e}")
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            logger.warning(f"Invalid path in get_dbn_metadata: {e}")
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            logger.error(f"Error in get_dbn_metadata: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error reading DBN metadata: {str(e)}")]

    elif name == "write_dbn_file":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            validate_schema(arguments["schema"])
            validate_date_format(arguments["start"], "start")
            validate_date_format(arguments["end"], "end")
            validate_date_range(arguments["start"], arguments["end"])
            compression = arguments.get("compression", "zstd")
            validate_compression(compression)
        except ValidationError as e:
            logger.warning(f"Validation error in write_dbn_file: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        schema = arguments["schema"]
        start = arguments["start"]
        end = arguments["end"]
        output_path = arguments["output_path"]

        try:
            # Determine final path with correct extension using helper function
            final_path = ensure_dbn_extension(output_path, compression)
            
            # Validate the final output path
            resolved_path = validate_file_path(final_path, must_exist=False)

            # Query data and write to file
            logger.debug(f"Writing DBN file to: {resolved_path}")
            data = client.timeseries.get_range(
                dataset=dataset,
                symbols=symbols,
                start=start,
                end=end,
                schema=schema,
            )

            # Write to DBN file
            data.to_file(str(resolved_path))

            # Get file stats
            file_size = resolved_path.stat().st_size
            df = data.to_df()
            record_count = len(df)

            # Format response
            result = "DBN File Written:\n\n"
            result += f"File Path: {resolved_path}\n"
            result += f"File Size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)\n"
            result += f"Record Count: {record_count:,}\n"
            result += f"Schema: {schema}\n"
            result += f"Compression: {compression}\n"
            result += f"Dataset: {dataset}\n"
            result += f"Symbols: {', '.join(symbols)}\n"
            result += f"Period: {start} to {end}\n"

            logger.info(f"Successfully wrote {record_count} records to DBN file")
            return [TextContent(type="text", text=result)]

        except ValueError as e:
            logger.warning(f"Invalid path in write_dbn_file: {e}")
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            logger.error(f"Error in write_dbn_file: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error writing DBN file: {str(e)}")]

    elif name == "convert_dbn_to_parquet":
        input_path = arguments["input_path"]
        output_path = arguments.get("output_path")
        compression = arguments.get("compression", "snappy")

        try:
            # Validate input path
            resolved_input = validate_file_path(input_path, must_exist=True)

            # Generate output path if not specified
            if output_path:
                resolved_output = validate_file_path(output_path, must_exist=False)
            else:
                # Replace .dbn.zst or .dbn with .parquet
                input_str = str(resolved_input)
                if input_str.endswith(".dbn.zst"):
                    output_str = input_str[:-8] + ".parquet"
                elif input_str.endswith(".dbn"):
                    output_str = input_str[:-4] + ".parquet"
                else:
                    output_str = input_str + ".parquet"
                # Validate the auto-generated output path
                resolved_output = validate_file_path(output_str, must_exist=False)

            # Get input file size
            input_size = resolved_input.stat().st_size

            # Read DBN file asynchronously
            logger.debug(f"Converting DBN to Parquet: {resolved_input} -> {resolved_output}")
            metadata, df = await read_dbn_file_async(resolved_input, limit=0, offset=0)
            record_count = len(df)

            # Write to Parquet asynchronously
            output_size = await write_parquet_async(df, resolved_output, compression=compression)

            # Get columns
            columns = list(df.columns)

            # Format response
            result = "DBN to Parquet Conversion:\n\n"
            result += f"Input File: {resolved_input}\n"
            result += f"Output File: {resolved_output}\n"
            result += f"Input Size: {input_size:,} bytes ({input_size / (1024*1024):.2f} MB)\n"
            result += f"Output Size: {output_size:,} bytes ({output_size / (1024*1024):.2f} MB)\n"
            result += f"Record Count: {record_count:,}\n"
            result += f"Schema: {metadata.schema}\n"
            result += f"Compression: {compression}\n"
            result += f"\nColumns Written ({len(columns)}):\n"
            for col in columns:
                result += f"  - {col}\n"

            logger.info(f"Successfully converted DBN to Parquet: {record_count} records")
            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            logger.warning(f"File not found in convert_dbn_to_parquet: {e}")
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            logger.warning(f"Invalid path in convert_dbn_to_parquet: {e}")
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            logger.error(f"Error in convert_dbn_to_parquet: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error converting DBN to Parquet: {str(e)}")]

    elif name == "export_to_parquet":
        try:
            # Validate inputs
            validate_dataset(arguments["dataset"])
            symbols = validate_symbols(arguments["symbols"])
            validate_schema(arguments["schema"])
            validate_date_format(arguments["start"], "start")
            validate_date_format(arguments["end"], "end")
            validate_date_range(arguments["start"], arguments["end"])
        except ValidationError as e:
            logger.warning(f"Validation error in export_to_parquet: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        dataset = arguments["dataset"]
        schema = arguments["schema"]
        start = arguments["start"]
        end = arguments["end"]
        output_path = arguments["output_path"]
        compression = arguments.get("compression", "snappy")

        try:
            # Determine final path with correct extension using helper function
            final_path = ensure_parquet_extension(output_path)
            
            # Validate the final output path
            resolved_path = validate_file_path(final_path, must_exist=False)

            # Query data using pooled client
            logger.debug(f"Exporting to Parquet: {resolved_path}")
            get_metrics().record_api_call()
            pool_client = get_pool().get_historical_client()
            data = pool_client.timeseries.get_range(
                dataset=dataset,
                symbols=symbols,
                start=start,
                end=end,
                schema=schema,
            )

            # Convert to DataFrame
            df = data.to_df()
            record_count = len(df)

            # Write to Parquet asynchronously
            file_size = await write_parquet_async(df, resolved_path, compression=compression)

            # Get columns
            columns = list(df.columns)

            # Format response
            result = "Export to Parquet:\n\n"
            result += f"Output File: {resolved_path}\n"
            result += f"File Size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)\n"
            result += f"Record Count: {record_count:,}\n"
            result += f"Schema: {schema}\n"
            result += f"Compression: {compression}\n"
            result += f"Dataset: {dataset}\n"
            result += f"Symbols: {', '.join(symbols)}\n"
            result += f"Period: {start} to {end}\n"
            result += f"\nColumns Written ({len(columns)}):\n"
            for col in columns:
                result += f"  - {col}\n"

            logger.info(f"Successfully exported {record_count} records to Parquet")
            return [TextContent(type="text", text=result)]

        except ValueError as e:
            logger.warning(f"Invalid path in export_to_parquet: {e}")
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            logger.error(f"Error in export_to_parquet: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error exporting to Parquet: {str(e)}")]

    elif name == "read_parquet_file":
        try:
            # Validate inputs
            limit = arguments.get("limit", 1000)
            validate_numeric_range(limit, "limit", min_value=1, max_value=100000)
        except ValidationError as e:
            logger.warning(f"Validation error in read_parquet_file: {e}")
            return [TextContent(type="text", text=f"Validation error: {str(e)}")]

        file_path = arguments["file_path"]
        columns_str = arguments.get("columns")

        try:
            # Validate file path
            resolved_path = validate_file_path(file_path, must_exist=True)

            # Parse columns if specified
            columns = None
            if columns_str:
                columns = [c.strip() for c in columns_str.split(",")]

            # Read Parquet file asynchronously
            logger.debug(f"Reading Parquet file: {resolved_path}")
            df, schema_info, parquet_metadata = await read_parquet_async(
                resolved_path, limit=limit, columns=columns
            )

            # Get total record count
            total_records = len(df)
            df_limited = df

            # Build response
            result = "Parquet File Contents:\n\n"
            result += f"File: {resolved_path}\n"
            result += f"Total Records: {total_records:,}\n"
            result += f"Row Groups: {parquet_metadata.num_row_groups}\n"
            result += f"Created By: {parquet_metadata.created_by}\n\n"

            result += f"=== Schema ({len(schema_info)}) ===\n"
            for field in schema_info:
                result += f"  {field.name}: {field.type}\n"

            result += f"\n=== Records (limit={limit}) ===\n"
            result += f"Records returned: {len(df_limited)}\n\n"

            if len(df_limited) > 0:
                result += "Sample data (first 10 rows):\n"
                result += df_limited.head(10).to_string()
                result += "\n"

            logger.info(f"Successfully read {len(df_limited)} records from Parquet file")
            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            logger.warning(f"File not found in read_parquet_file: {e}")
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            logger.warning(f"Invalid path in read_parquet_file: {e}")
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            logger.error(f"Error in read_parquet_file: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error reading Parquet file: {str(e)}")]

    elif name == "get_metrics":
        reset = arguments.get("reset", False) if arguments else False

        try:
            metrics_collector = get_metrics()
            summary = metrics_collector.get_summary()

            # Format response
            result = "📊 Server Metrics:\n\n"
            result += f"⏱️ Uptime: {summary['uptime_seconds']:.2f} seconds\n"
            result += f"🌐 Total API Calls: {summary['total_api_calls']}\n\n"

            # Cache metrics
            cache_info = summary["cache"]
            result += "📦 Cache Statistics:\n"
            result += f"  Hits: {cache_info['hits']}\n"
            result += f"  Misses: {cache_info['misses']}\n"
            result += f"  Hit Rate: {cache_info['hit_rate']:.2%}\n\n"

            # Tool metrics
            tools = summary["tools"]
            if tools:
                result += "🔧 Tool Performance:\n"
                for tool_name, tool_metrics in tools.items():
                    result += f"\n  {tool_name}:\n"
                    result += f"    Calls: {tool_metrics['calls']}\n"
                    result += f"    Successes: {tool_metrics['successes']}\n"
                    result += f"    Errors: {tool_metrics['errors']}\n"
                    result += f"    Success Rate: {tool_metrics['success_rate']:.2%}\n"
                    latency = tool_metrics["latency_ms"]
                    result += "    Latency (ms):\n"
                    result += f"      Avg: {latency['avg']}\n"
                    result += f"      Min: {latency['min']}\n"
                    result += f"      Max: {latency['max']}\n"
                    result += f"      P95: {latency['p95']}\n"
                    result += f"      P99: {latency['p99']}\n"
            else:
                result += "🔧 No tool calls recorded yet.\n"

            if reset:
                metrics_collector.reset()
                result += "\n✅ Metrics have been reset.\n"

            logger.info("Successfully retrieved metrics")
            return [TextContent(type="text", text=result)]

        except Exception as e:
            logger.error(f"Error in get_metrics: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error getting metrics: {str(e)}")]

    else:
        logger.warning(f"Unknown tool called: {name}")
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    logger.info("Starting Databento MCP server")
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())

