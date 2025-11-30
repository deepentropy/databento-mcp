# Databento MCP Server

A Model Context Protocol (MCP) server for accessing Databento's financial market data API.

## Features

✅ **Retrieve Historical Data** - Get trades, OHLCV bars, market depth, and more  
✅ **Symbol Metadata** - Access instrument definitions and symbology mappings  
✅ **Instrument Search** - Find available symbols with wildcard support  
✅ **Dataset Discovery** - List all available Databento datasets  
✅ **Cost Estimation** - Estimate query costs before executing  
✅ **Live Data Streaming** - Subscribe to real-time market data  
✅ **Symbology Resolution** - Resolve symbols between different symbology types  
✅ **Batch Job Management** - Submit and manage large-scale batch data downloads  
✅ **Session Detection** - Identify current trading session (Asian, London, NY)  
✅ **Enhanced Metadata** - List publishers, fields, and dataset ranges  
✅ **DBN File Processing** - Read, write, and parse DBN format files  
✅ **Parquet Export** - Convert data to Apache Parquet format  
✅ **Smart Caching** - File-based cache with automatic expiration to reduce API calls  
✅ **Input Validation** - Comprehensive validation of all inputs with clear error messages  
✅ **Retry Logic** - Automatic retries with exponential backoff for transient errors  
✅ **Structured Logging** - Configurable logging with DATABENTO_LOG_LEVEL  
✅ **MCP Compatible** - Works with Claude Desktop and other MCP clients  

## Quick Start

```powershell
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure your API key
Copy-Item .env.example .env
# Edit .env and add your Databento API key

# 3. Test the setup
python test_setup.py

# 4. Run the server
python server.py
```

See [QUICKSTART.md](QUICKSTART.md) for detailed setup instructions.

## Available Tools

### 🔹 get_historical_data
Retrieve historical market data (trades, OHLCV, market depth, etc.)

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3", "XNAS.ITCH")
- `symbols` - Comma-separated list of symbols
- `start` - Start date (YYYY-MM-DD)
- `end` - End date (YYYY-MM-DD)
- `schema` - Data schema (e.g., "trades", "ohlcv-1m", "mbp-1")
- `limit` - Max records to return (default: 1000)

### 🔹 get_symbol_metadata
Get metadata including symbology mappings and instrument definitions

**Parameters:**
- `dataset` - Dataset name
- `symbols` - Comma-separated list of symbols
- `start` - Start date (YYYY-MM-DD)
- `end` - End date (YYYY-MM-DD, optional)

### 🔹 search_instruments
Search for instruments with wildcard support

**Parameters:**
- `dataset` - Dataset name
- `symbols` - Symbol pattern (supports wildcards like "ES*")
- `start` - Start date (YYYY-MM-DD)

### 🔹 list_datasets
List all available Databento datasets

### 🔹 clear_cache
Clear the API response cache

**Parameters:**
- `expired_only` - Only clear expired entries (default: false)

### 🔹 get_cost
Estimate the cost of a historical data query before executing it

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3", "XNAS.ITCH")
- `symbols` - Comma-separated list of symbols
- `schema` - Data schema (e.g., "trades", "ohlcv-1m", "mbp-1")
- `start` - Start date (YYYY-MM-DD or ISO 8601 datetime)
- `end` - End date (YYYY-MM-DD or ISO 8601 datetime)

**Returns:**
- Estimated cost in USD
- Estimated record count
- Estimated data size in bytes

### 🔹 get_live_data
Subscribe to real-time market data for a limited duration

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3")
- `symbols` - Comma-separated list of symbols
- `schema` - Data schema (e.g., "trades", "mbp-1", "ohlcv-1s")
- `duration` - How long to stream data in seconds (default: 10, max: 60)

**Returns:**
- Array of received market data records
- Count of records received
- Stream duration

### 🔹 resolve_symbols
Resolve symbols between different symbology types

**Parameters:**
- `dataset` - Dataset name
- `symbols` - Comma-separated list of symbols to resolve
- `stype_in` - Input symbol type (e.g., "raw_symbol", "instrument_id", "continuous", "parent")
- `stype_out` - Output symbol type (e.g., "instrument_id", "raw_symbol")
- `start` - Start date for resolution (YYYY-MM-DD)
- `end` - End date for resolution (YYYY-MM-DD, optional)

**Returns:**
- Resolved symbol mappings
- Resolution status (full/partial)
- Count of resolved symbols

### 🔹 submit_batch_job
Submit a batch data download job for large historical datasets

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3")
- `symbols` - Comma-separated list of symbols
- `schema` - Data schema (e.g., "trades", "ohlcv-1m")
- `start` - Start date (YYYY-MM-DD or ISO 8601)
- `end` - End date (YYYY-MM-DD or ISO 8601)
- `encoding` - Output encoding (optional: "dbn", "csv", "json", default: "dbn")
- `compression` - Compression type (optional: "none", "zstd", default: "zstd")
- `split_duration` - Split files by duration (optional: "day", "week", "month", "none")

**Returns:**
- Job ID
- Job state (received, queued, processing, done)
- Estimated cost in USD
- Submission timestamp

### 🔹 list_batch_jobs
List all batch jobs with their current status

**Parameters:**
- `states` - Filter by states (optional, comma-separated: "received", "queued", "processing", "done", "expired")
- `since` - Only show jobs since this date (optional, ISO 8601)
- `limit` - Maximum number of jobs to return (optional, default: 20)

**Returns:**
- List of jobs with ID, state, dataset, schema, cost, timestamps
- Total job count
- Jobs grouped by state

### 🔹 get_batch_job_files
Get download information for a completed batch job

**Parameters:**
- `job_id` - The batch job ID

**Returns:**
- Job state
- List of files with filename, size, hash, download URL
- Total size
- Expiration date

### 🔹 get_session_info
Identify the current trading session based on time

**Parameters:**
- `timestamp` - ISO 8601 timestamp (optional, defaults to current time)

**Returns:**
- Current session name (Asian, London, NY, Off-hours)
- Session start time (UTC)
- Session end time (UTC)
- Current timestamp
- UTC hour

**Session Definitions:**
- **Asian**: 00:00 - 07:00 UTC
- **London**: 07:00 - 14:00 UTC
- **NY**: 14:00 - 22:00 UTC
- **Off-hours**: 22:00 - 00:00 UTC

### 🔹 list_publishers
List data publishers with their details

**Parameters:**
- `dataset` - Filter by dataset (optional)

**Returns:**
- List of publishers with publisher_id, dataset, venue, description

### 🔹 list_fields
List fields available for a specific schema

**Parameters:**
- `schema` - Schema name (e.g., "trades", "mbp-1")
- `encoding` - Encoding format (optional: "dbn", "csv", "json", default: "json")

**Returns:**
- List of fields with name, type, description

### 🔹 get_dataset_range
Get the available date range for a dataset

**Parameters:**
- `dataset` - Dataset name

**Returns:**
- Dataset name
- Start date (earliest available data)
- End date (latest available data, or "ongoing" if active)

### 🔹 read_dbn_file
Read and parse a DBN file, returning the records as structured data

**Parameters:**
- `file_path` - Path to the DBN file (can be .dbn or .dbn.zst for zstd-compressed)
- `limit` - Maximum number of records to return (default: 1000)
- `offset` - Number of records to skip (default: 0)

**Returns:**
- File metadata (version, dataset, schema, timestamps, symbol count)
- Record count
- Parsed records up to limit

### 🔹 get_dbn_metadata
Get only the metadata from a DBN file without reading all records

**Parameters:**
- `file_path` - Path to the DBN file

**Returns:**
- Version, dataset, schema
- Start and end timestamps
- Symbols list
- Symbology mappings (if present)

### 🔹 write_dbn_file
Write historical data query results directly to a DBN file

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3")
- `symbols` - Comma-separated list of symbols
- `schema` - Data schema (e.g., "trades", "ohlcv-1m")
- `start` - Start date (YYYY-MM-DD or ISO 8601)
- `end` - End date (YYYY-MM-DD or ISO 8601)
- `output_path` - Path for output file
- `compression` - Compression type (optional: "none", "zstd", default: "zstd")

**Returns:**
- File path written
- File size in bytes
- Record count
- Schema and compression used

### 🔹 convert_dbn_to_parquet
Convert a DBN file to Parquet format

**Parameters:**
- `input_path` - Path to the input DBN file
- `output_path` - Path for output Parquet file (optional, defaults to input_path with .parquet extension)
- `compression` - Parquet compression (optional: "snappy", "gzip", "zstd", "none", default: "snappy")

**Returns:**
- Input and output file paths
- Input and output file sizes
- Record count
- Columns written

### 🔹 export_to_parquet
Query historical data and export directly to Parquet format

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3")
- `symbols` - Comma-separated list of symbols
- `schema` - Data schema (e.g., "trades", "ohlcv-1m")
- `start` - Start date (YYYY-MM-DD or ISO 8601)
- `end` - End date (YYYY-MM-DD or ISO 8601)
- `output_path` - Path for output Parquet file
- `compression` - Parquet compression (optional: "snappy", "gzip", "zstd", "none", default: "snappy")

**Returns:**
- Output file path
- File size in bytes
- Record count
- Columns written

### 🔹 read_parquet_file
Read a Parquet file and return the data

**Parameters:**
- `file_path` - Path to the Parquet file
- `limit` - Maximum number of records to return (default: 1000)
- `columns` - Comma-separated list of columns to read (optional, reads all if not specified)

**Returns:**
- Schema/columns information
- Record count (total in file)
- Records (up to limit)
- File metadata

## File Path Security

The server implements path validation to prevent directory traversal attacks:
- Paths containing `..` are rejected
- Set `DATABENTO_DATA_DIR` environment variable to restrict file operations to a specific directory

## Input Validation

All tool inputs are validated before processing:

- **Date formats**: Must be YYYY-MM-DD or ISO 8601 (e.g., "2024-01-15T10:30:00Z")
- **Date ranges**: Start date must be before or equal to end date
- **Symbols**: Non-empty, valid characters (alphanumeric, dots, dashes, underscores)
- **Dataset names**: Must follow VENUE.FORMAT pattern (e.g., "GLBX.MDP3")
- **Schema**: Must be a known schema (trades, ohlcv-1m, mbp-1, etc.)
- **Encoding**: Must be "dbn", "csv", or "json"
- **Compression**: Must be "none" or "zstd"
- **Symbol types (stype)**: Must be "raw_symbol", "instrument_id", "continuous", "parent", or "smart"
- **Numeric parameters**: Validated against min/max bounds (e.g., limit: 1-100000, duration: 1-60)

Invalid inputs return clear error messages explaining the validation failure.

## Retry Behavior

The server includes a retry module for handling transient API errors:

- **Exponential backoff**: Delays increase exponentially between retries (1s, 2s, 4s, etc.)
- **Jitter**: Random variation in delay times to prevent thundering herd
- **Rate limit detection**: HTTP 429 errors trigger retries with appropriate backoff
- **Transient error detection**: Connection errors, timeouts, and 502/503/504 errors are retried
- **Configurable retries**: Default is 3 retry attempts
- **Logging**: All retry attempts are logged with error details

## Logging

The server provides structured logging for debugging and monitoring.

### DATABENTO_LOG_LEVEL

Control the logging verbosity by setting the `DATABENTO_LOG_LEVEL` environment variable:

```bash
# Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
export DATABENTO_LOG_LEVEL=DEBUG

# Or in .env file
DATABENTO_LOG_LEVEL=INFO
```

**Log Levels:**
- `DEBUG`: Detailed information for debugging (API calls, cache hits, etc.)
- `INFO`: General operational information (default)
- `WARNING`: Warnings and validation errors
- `ERROR`: Errors with stack traces
- `CRITICAL`: Critical failures

**What's logged:**
- Tool calls with parameters
- Success/failure of operations
- API errors with full stack traces
- Cache hits and misses
- Retry attempts

**Suppressed loggers:**
- `httpx`, `httpcore`, `asyncio` are set to WARNING level to reduce noise

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Step-by-step setup guide
- **[USAGE.md](USAGE.md)** - Detailed usage examples and API reference
- **[Databento Docs](https://databento.com/docs)** - Official Databento documentation

## Project Structure

```
databento-mcp/
├── server.py          # Main MCP server implementation
├── cache.py           # File-based caching system
├── validation.py      # Input validation module
├── retry.py           # Retry logic with exponential backoff
├── requirements.txt   # Python dependencies
├── pyproject.toml     # Project configuration
├── test_setup.py      # Configuration verification script
├── .env.example       # Example environment variables
├── mcp-config.json    # Example MCP client configuration
├── README.md          # This file
├── QUICKSTART.md      # Quick start guide
├── USAGE.md           # Detailed usage documentation
└── LICENSE            # License file
```

## Configuration for MCP Clients

### Claude Desktop (Windows)

Edit: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databento": {
      "command": "python",
      "args": ["C:\\Users\\otrem\\PycharmProjects\\databento-mcp\\server.py"],
      "env": {
        "DATABENTO_API_KEY": "your_api_key_here"
      }
    }
  }
}
```

## Technologies Used

- **[Databento Python SDK](https://github.com/databento/databento-python)** - Official Python client
- **[MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)** - Model Context Protocol implementation
- **Python 3.10+** - Modern Python with type hints

## License

See LICENSE file.


