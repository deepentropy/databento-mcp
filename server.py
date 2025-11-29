"""Databento MCP Server - Provides access to Databento market data API."""
import os
import sys
import asyncio
import json
import time
from typing import Any

import databento as db
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
                result += f"\n(Showing first 50 results)"

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

