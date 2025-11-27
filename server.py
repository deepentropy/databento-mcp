"""Databento MCP Server - Provides access to Databento market data API."""
import os
import sys
import asyncio
import json
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

