"""Databento MCP Server - Provides access to Databento market data API."""
import os
import sys
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import databento as db
import pyarrow.parquet as pq
import pandas as pd
from mcp.server import Server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
)
from mcp.server.stdio import stdio_server
from dotenv import load_dotenv

from cache import Cache

# Load environment variables
load_dotenv()

# Initialize cache (1 hour default TTL)
cache = Cache(cache_dir="cache", default_ttl=3600)

# Initialize Databento client
api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("Error: DATABENTO_API_KEY environment variable not set", file=sys.stderr)
    sys.exit(1)

client = db.Historical(api_key)

# Create MCP server
app = Server("databento-mcp")

# Get allowed data directory from environment variable
ALLOWED_DATA_DIR = os.getenv("DATABENTO_DATA_DIR")


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
    
    # Resolve to absolute path
    try:
        resolved_path = path.resolve()
    except (OSError, ValueError) as e:
        raise ValueError(f"Invalid file path: {e}")
    
    # Check for directory traversal attempts
    if ".." in str(file_path):
        raise ValueError("Directory traversal (..) not allowed in file paths")
    
    # If DATABENTO_DATA_DIR is set, enforce it
    if ALLOWED_DATA_DIR:
        allowed_dir = Path(ALLOWED_DATA_DIR).resolve()
        try:
            resolved_path.relative_to(allowed_dir)
        except ValueError:
            raise ValueError(
                f"File path must be within DATABENTO_DATA_DIR: {allowed_dir}"
            )
    
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
    return []


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
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
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls."""

    if name == "get_historical_data":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        start = arguments["start"]
        end = arguments["end"]
        schema = arguments.get("schema", "trades")
        limit = arguments.get("limit", 1000)

        # Create cache key
        cache_key = f"historical:{dataset}:{','.join(sorted(symbols))}:{start}:{end}:{schema}:{limit}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Historical data for {', '.join(symbols)}:\n\n{cached_data}"
            )]

        try:
            # Fetch data from Databento
            data = client.timeseries.get_range(
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error retrieving historical data: {str(e)}")]

    elif name == "get_symbol_metadata":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        start = arguments["start"]
        end = arguments.get("end")

        # Create cache key
        cache_key = f"metadata:{dataset}:{','.join(sorted(symbols))}:{start}:{end}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Metadata:\n\n{cached_data}"
            )]

        try:
            # Fetch metadata
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error retrieving metadata: {str(e)}")]

    elif name == "search_instruments":
        dataset = arguments["dataset"]
        symbols = arguments.get("symbols", "*")
        start = arguments["start"]

        # Create cache key
        cache_key = f"search:{dataset}:{symbols}:{start}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Search results:\n\n{cached_data}"
            )]

        try:
            # Search for instruments
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error searching instruments: {str(e)}")]

    elif name == "list_datasets":
        # Create cache key
        cache_key = "datasets:list"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Available datasets:\n\n{cached_data}"
            )]

        try:
            # List datasets
            datasets = client.metadata.list_datasets()

            # Format response
            result = "Available Databento datasets:\n\n"
            for dataset in datasets:
                result += f"- {dataset}\n"

            # Cache the result
            cache.set(cache_key, result, ttl=86400)  # 24 hour TTL

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error listing datasets: {str(e)}")]

    elif name == "clear_cache":
        expired_only = arguments.get("expired_only", False)

        try:
            if expired_only:
                cache.clear_expired()
                return [TextContent(type="text", text="Expired cache entries cleared successfully.")]
            else:
                cache.clear()
                return [TextContent(type="text", text="All cache entries cleared successfully.")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error clearing cache: {str(e)}")]

    elif name == "get_cost":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        schema = arguments.get("schema", "trades")
        start = arguments["start"]
        end = arguments["end"]

        # Create cache key
        cache_key = f"cost:{dataset}:{','.join(sorted(symbols))}:{schema}:{start}:{end}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Cost estimate:\n\n{cached_data}"
            )]

        try:
            # Get cost estimate
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error estimating cost: {str(e)}")]

    elif name == "get_live_data":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        schema = arguments.get("schema", "trades")
        duration = arguments.get("duration", 10)

        # Validate duration (max 60 seconds to prevent long-running calls)
        if duration < 1:
            return [TextContent(type="text", text="Error: duration must be at least 1 second")]
        if duration > 60:
            return [TextContent(type="text", text="Error: duration cannot exceed 60 seconds")]

        try:
            # Create Live client
            live_client = db.Live(key=api_key)

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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error streaming live data: {str(e)}")]

    elif name == "resolve_symbols":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        stype_in = arguments.get("stype_in", "raw_symbol")
        stype_out = arguments.get("stype_out", "instrument_id")
        start = arguments["start"]
        end = arguments.get("end")

        # Create cache key
        cache_key = f"resolve:{dataset}:{','.join(sorted(symbols))}:{stype_in}:{stype_out}:{start}:{end}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Symbol resolution:\n\n{cached_data}"
            )]

        try:
            # Resolve symbols
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error resolving symbols: {str(e)}")]

    elif name == "submit_batch_job":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        schema = arguments["schema"]
        start = arguments["start"]
        end = arguments["end"]
        encoding = arguments.get("encoding", "dbn")
        compression = arguments.get("compression", "zstd")
        split_duration = arguments.get("split_duration", "day")

        try:
            # Submit batch job
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error submitting batch job: {str(e)}")]

    elif name == "list_batch_jobs":
        states = arguments.get("states", "queued,processing,done")
        since = arguments.get("since")
        limit = arguments.get("limit", 20)

        # Create cache key
        cache_key = f"batch_jobs:{states}:{since}:{limit}"

        # Check cache with short TTL (batch job status changes frequently)
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Batch jobs:\n\n{cached_data}"
            )]

        try:
            # Parse states
            state_list = [s.strip() for s in states.split(",")]

            # List batch jobs
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error listing batch jobs: {str(e)}")]

    elif name == "get_batch_job_files":
        job_id = arguments["job_id"]

        # Create cache key
        cache_key = f"batch_files:{job_id}"

        # Check cache with short TTL
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Batch job files:\n\n{cached_data}"
            )]

        try:
            # Get file list for job
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error getting batch job files: {str(e)}")]

    elif name == "get_session_info":
        timestamp_str = arguments.get("timestamp")

        try:
            # Parse timestamp or use current time
            if timestamp_str:
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error determining session info: {str(e)}")]

    elif name == "list_publishers":
        dataset_filter = arguments.get("dataset")

        # Create cache key
        cache_key = f"publishers:{dataset_filter}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Publishers:\n\n{cached_data}"
            )]

        try:
            # Get publishers
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error listing publishers: {str(e)}")]

    elif name == "list_fields":
        schema = arguments["schema"]
        encoding = arguments.get("encoding", "json")

        # Create cache key
        cache_key = f"fields:{schema}:{encoding}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Fields:\n\n{cached_data}"
            )]

        try:
            # Get fields
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

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error listing fields: {str(e)}")]

    elif name == "get_dataset_range":
        dataset = arguments["dataset"]

        # Create cache key
        cache_key = f"dataset_range:{dataset}"

        # Check cache
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            return [TextContent(
                type="text",
                text=f"[Cached] Dataset range:\n\n{cached_data}"
            )]

        try:
            # Get dataset range
            range_info = client.metadata.get_dataset_range(dataset=dataset)

            # Format response
            result = f"Dataset Range for {dataset}:\n\n"
            result += f"Dataset: {dataset}\n"
            result += f"Start Date: {range_info.get('start_date', 'N/A')}\n"
            result += f"End Date: {range_info.get('end_date', 'ongoing')}\n"

            # Cache for 1 hour (dataset ranges can update)
            cache.set(cache_key, result, ttl=3600)

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=f"Error getting dataset range: {str(e)}")]

    elif name == "read_dbn_file":
        file_path = arguments["file_path"]
        limit = arguments.get("limit", 1000)
        offset = arguments.get("offset", 0)

        try:
            # Validate file path
            resolved_path = validate_file_path(file_path, must_exist=True)

            # Read DBN file using DBNStore
            store = db.DBNStore.from_file(str(resolved_path))

            # Get metadata
            metadata = store.metadata

            # Build metadata info
            result = "DBN File Contents:\n\n"
            result += "=== Metadata ===\n"
            result += f"Version: {metadata.version}\n"
            result += f"Dataset: {metadata.dataset}\n"
            result += f"Schema: {metadata.schema}\n"
            result += f"Start: {metadata.start}\n"
            result += f"End: {metadata.end}\n"
            result += f"Symbol Count: {metadata.symbol_cstr_len}\n"

            # Convert to DataFrame for record handling
            df = store.to_df()
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

            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error reading DBN file: {str(e)}")]

    elif name == "get_dbn_metadata":
        file_path = arguments["file_path"]

        try:
            # Validate file path
            resolved_path = validate_file_path(file_path, must_exist=True)

            # Read DBN file using DBNStore
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
                result += f"\nSymbols:\n"
                for symbol in metadata.symbols[:50]:  # Limit to first 50
                    result += f"  - {symbol}\n"
                if len(metadata.symbols) > 50:
                    result += f"  ... and {len(metadata.symbols) - 50} more\n"

            # Get mappings if available
            if hasattr(store, 'symbology') and store.symbology:
                result += f"\nSymbology Mappings:\n"
                mappings = store.symbology
                count = 0
                for key, value in mappings.items():
                    if count >= 20:  # Limit output
                        result += f"  ... and more mappings\n"
                        break
                    result += f"  {key}: {value}\n"
                    count += 1

            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error reading DBN metadata: {str(e)}")]

    elif name == "write_dbn_file":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        schema = arguments["schema"]
        start = arguments["start"]
        end = arguments["end"]
        output_path = arguments["output_path"]
        compression = arguments.get("compression", "zstd")

        try:
            # Validate output path
            resolved_path = validate_file_path(output_path, must_exist=False)

            # Ensure correct file extension
            if compression == "zstd" and not str(resolved_path).endswith(".dbn.zst"):
                if str(resolved_path).endswith(".dbn"):
                    resolved_path = Path(str(resolved_path) + ".zst")
                elif not str(resolved_path).endswith(".dbn.zst"):
                    resolved_path = Path(str(resolved_path) + ".dbn.zst")
            elif compression == "none" and not str(resolved_path).endswith(".dbn"):
                resolved_path = Path(str(resolved_path) + ".dbn")

            # Query data and write to file
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

            return [TextContent(type="text", text=result)]

        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
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
                resolved_output = Path(output_str)

            # Get input file size
            input_size = resolved_input.stat().st_size

            # Read DBN file
            store = db.DBNStore.from_file(str(resolved_input))
            df = store.to_df()
            record_count = len(df)

            # Convert compression for parquet
            parquet_compression = compression if compression != "none" else None

            # Write to Parquet
            df.to_parquet(str(resolved_output), compression=parquet_compression)

            # Get output file size
            output_size = resolved_output.stat().st_size

            # Get columns
            columns = list(df.columns)

            # Format response
            result = "DBN to Parquet Conversion:\n\n"
            result += f"Input File: {resolved_input}\n"
            result += f"Output File: {resolved_output}\n"
            result += f"Input Size: {input_size:,} bytes ({input_size / (1024*1024):.2f} MB)\n"
            result += f"Output Size: {output_size:,} bytes ({output_size / (1024*1024):.2f} MB)\n"
            result += f"Record Count: {record_count:,}\n"
            result += f"Schema: {store.metadata.schema}\n"
            result += f"Compression: {compression}\n"
            result += f"\nColumns Written ({len(columns)}):\n"
            for col in columns:
                result += f"  - {col}\n"

            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error converting DBN to Parquet: {str(e)}")]

    elif name == "export_to_parquet":
        dataset = arguments["dataset"]
        symbols = arguments["symbols"].split(",")
        symbols = [s.strip() for s in symbols]
        schema = arguments["schema"]
        start = arguments["start"]
        end = arguments["end"]
        output_path = arguments["output_path"]
        compression = arguments.get("compression", "snappy")

        try:
            # Validate output path
            resolved_path = validate_file_path(output_path, must_exist=False)

            # Ensure .parquet extension
            if not str(resolved_path).endswith(".parquet"):
                resolved_path = Path(str(resolved_path) + ".parquet")

            # Query data
            data = client.timeseries.get_range(
                dataset=dataset,
                symbols=symbols,
                start=start,
                end=end,
                schema=schema,
            )

            # Convert to DataFrame
            df = data.to_df()
            record_count = len(df)

            # Convert compression for parquet
            parquet_compression = compression if compression != "none" else None

            # Write to Parquet
            df.to_parquet(str(resolved_path), compression=parquet_compression)

            # Get file stats
            file_size = resolved_path.stat().st_size

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

            return [TextContent(type="text", text=result)]

        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error exporting to Parquet: {str(e)}")]

    elif name == "read_parquet_file":
        file_path = arguments["file_path"]
        limit = arguments.get("limit", 1000)
        columns_str = arguments.get("columns")

        try:
            # Validate file path
            resolved_path = validate_file_path(file_path, must_exist=True)

            # Parse columns if specified
            columns = None
            if columns_str:
                columns = [c.strip() for c in columns_str.split(",")]

            # Read Parquet file
            parquet_file = pq.read_table(str(resolved_path), columns=columns)
            df = parquet_file.to_pandas()

            # Get total record count and schema
            total_records = len(df)
            schema_info = parquet_file.schema

            # Apply limit
            df_limited = df.head(limit)

            # Get file metadata
            parquet_metadata = pq.read_metadata(str(resolved_path))

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

            return [TextContent(type="text", text=result)]

        except FileNotFoundError as e:
            return [TextContent(type="text", text=f"File not found: {str(e)}")]
        except ValueError as e:
            return [TextContent(type="text", text=f"Invalid path: {str(e)}")]
        except Exception as e:
            return [TextContent(type="text", text=f"Error reading Parquet file: {str(e)}")]

    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())

