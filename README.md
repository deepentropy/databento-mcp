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
✅ **Health Check** - Diagnose API connectivity and configuration issues  
✅ **MCP Prompts** - Built-in guides for market data workflows and troubleshooting  
✅ **MCP Resources** - Reference documentation for schemas, datasets, and error codes  
✅ **Structured Errors** - Error codes with actionable suggestions for Claude  
✅ **MCP Compatible** - Works with Claude Desktop and other MCP clients  
✅ **Connection Pooling** - Singleton connection pool for better performance  
✅ **Metrics & Telemetry** - Server performance metrics and usage statistics  
✅ **Async File I/O** - Optimized async operations for file processing

### ✨ New UX Features

✅ **Smart Data Summaries** - Auto-generated statistics (price range, volume, trade count) with insights  
✅ **Enhanced Cache Feedback** - Show cache age, expiration time, and `force_refresh` option  
✅ **Query Size Warnings** - Estimates and warnings for large queries with alternative suggestions  
✅ **Explain Mode** - Dry-run queries to see estimates before execution  
✅ **Data Quality Alerts** - Detect time gaps, price outliers, null values, and duplicates  
✅ **Account Status** - Server uptime, cache stats, and tool usage metrics  
✅ **Quick Analysis** - One-call comprehensive symbol report  

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

### 🔹 health_check
Check the health and connectivity of the Databento API

**Parameters:**
- `verbose` - Include detailed diagnostic information (default: false)

**Returns:**
- API connectivity status
- Authentication status
- Response time
- Available datasets count
- Diagnostic details (when verbose=true)

### 🔹 get_historical_data
Retrieve historical market data (trades, OHLCV, market depth, etc.)

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3", "XNAS.ITCH")
- `symbols` - Comma-separated list of symbols
- `start` - Start date (YYYY-MM-DD)
- `end` - End date (YYYY-MM-DD)
- `schema` - Data schema (e.g., "trades", "ohlcv-1m", "mbp-1")
- `limit` - Max records to return (default: 1000)
- `explain` - Preview query estimates without executing (default: false) ✨
- `force_refresh` - Bypass cache and fetch fresh data (default: false) ✨

**Example:**
```python
# Get ES futures trades with explain mode
get_historical_data(
    dataset="GLBX.MDP3",
    symbols="ES.FUT",
    start="2024-01-15",
    end="2024-01-15",
    schema="trades",
    explain=True  # Preview query before execution
)
```

### 🔹 get_account_status ✨
Get comprehensive server status and account information

**Returns:**
- Server uptime and health
- Cache statistics (hit rate, entries)
- API usage metrics
- Tool usage breakdown

### 🔹 quick_analysis ✨
One-call comprehensive analysis of a symbol

**Parameters:**
- `dataset` - Dataset name (e.g., "GLBX.MDP3")
- `symbol` - Symbol to analyze (e.g., "ES.FUT")
- `date` - Date to analyze (YYYY-MM-DD)
- `schema` - Data schema (default: "trades")

**Returns:**
- Symbol metadata
- Cost estimate for full-day data
- Sample of recent trades/bars
- Data quality assessment
- Trading session info

**Example:**
```python
quick_analysis(
    dataset="GLBX.MDP3",
    symbol="ES.FUT",
    date="2024-01-15"
)
```

### 🔹 analyze_data_quality ✨
Analyze data quality and detect issues in market data

**Parameters:**
- `dataset` - Dataset name
- `symbols` - Comma-separated list of symbols
- `start` - Start date (YYYY-MM-DD)
- `end` - End date (YYYY-MM-DD)
- `schema` - Data schema (default: "trades")
- `limit` - Max records to analyze (default: 10000)

**Returns:**
- Quality score (0-100)
- Time gaps detected
- Price outliers (>3 standard deviations)
- Null values and duplicates
- Issues and warnings list

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

### 🔹 list_schemas
List all available data schemas from Databento

**Parameters:** None

**Returns:**
- List of all available schemas organized by category:
  - Trade Data: trades, tbbo
  - Order Book: mbp-1, mbp-10, mbo
  - OHLCV Bars: ohlcv-1s, ohlcv-1m, ohlcv-1h, ohlcv-1d
  - Reference Data: definition, statistics, status, imbalance
- Description for each schema

**Note:** Results are cached for 24 hours (schemas rarely change)

### 🔹 list_unit_prices
Get current pricing information per dataset/schema combination

**Parameters:**
- `dataset` - Filter by dataset name (optional, e.g., "GLBX.MDP3")

**Returns:**
- List of unit prices showing cost per GB or per record
- Prices grouped by dataset
- Helps understand pricing before querying

**Note:** Results are cached for 1 hour (prices may change)

### 🔹 cancel_batch_job
Cancel a pending or processing batch job

**Parameters:**
- `job_id` - The batch job ID to cancel (required)

**Returns:**
- Success/failure status
- Final job state
- Message explaining the result

**Error Handling:**
- Returns error if job not found
- Returns error if job already completed or expired
- Clears cached data for the job

### 🔹 download_batch_files
Download completed batch job files to a local directory

**Parameters:**
- `job_id` - The batch job ID (required)
- `output_dir` - Directory to save downloaded files (required)
- `overwrite` - Whether to overwrite existing files (optional, default: false)

**Returns:**
- List of downloaded files with paths and sizes
- Total download size
- Success/failure status for each file
- Hash verification warnings if applicable

**Security:** Output directory is validated using the same path security as other file operations

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

### 🔹 get_metrics
Get server performance metrics and usage statistics

**Parameters:**
- `reset` - Reset metrics after retrieval (default: false)

**Returns:**
- Server uptime
- Total API calls
- Cache hit/miss statistics and hit rate
- Per-tool metrics:
  - Call count
  - Success/error counts
  - Success rate
  - Latency statistics (avg, min, max, p95, p99)

## File Path Security

The server implements path validation to prevent directory traversal attacks:
- Paths containing `..` are rejected
- Set `DATABENTO_DATA_DIR` environment variable to restrict file operations to a specific directory

## Connection Pooling

The server uses a singleton connection pool for Databento clients to improve performance:
- Historical client is reused across tool calls
- Live client is created fresh for each streaming request (not reusable after stop)
- Pool can be reset for error recovery

## Metrics & Telemetry

The server collects comprehensive metrics for monitoring and debugging:

### Metrics Collected
- **API calls**: Total number of Databento API calls
- **Cache performance**: Hit/miss counts and hit rate
- **Tool performance**: Per-tool call counts, success rates, and latency statistics
- **Latency percentiles**: p95 and p99 latency for each tool

### Usage
```python
# Retrieve metrics via the get_metrics tool
# Returns JSON with uptime, API calls, cache stats, and per-tool metrics
```

### Configuration
- Set `DATABENTO_METRICS_ENABLED=false` to disable metrics collection

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

## Environment Variables

The server supports the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABENTO_API_KEY` | **Required.** Your Databento API key | - |
| `DATABENTO_DATA_DIR` | Restrict file operations to this directory | Not set (current directory) |
| `DATABENTO_LOG_LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL) | INFO |
| `DATABENTO_METRICS_ENABLED` | Enable/disable metrics collection | true |

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

## MCP Prompts

The server provides built-in prompts to guide Claude through common workflows:

### market-data-workflow
Step-by-step guide for retrieving market data. Use when you need to:
- Discover available datasets
- Check data availability for a date range
- Estimate costs before querying
- Retrieve and export data

### cost-aware-query
Guide for estimating costs before running expensive queries.

**Arguments:**
- `dataset` - The dataset you want to query (optional, defaults to GLBX.MDP3)

### troubleshooting
Diagnose and resolve common issues with the Databento MCP server. Covers:
- Validation errors (E1xxx)
- API errors (E2xxx)
- File errors (E3xxx)
- Data errors (E4xxx)

## MCP Resources

Reference documentation available as MCP resources:

| Resource URI | Description |
|--------------|-------------|
| `databento://schemas` | Documentation of available data schemas |
| `databento://datasets` | Common datasets and their descriptions |
| `databento://error-codes` | Complete list of error codes and meanings |

## Error Codes

The server uses structured error codes to help diagnose issues:

### Validation Errors (E1xxx)
| Code | Description |
|------|-------------|
| E1001 | Invalid date format |
| E1002 | Invalid symbols |
| E1003 | Invalid dataset |
| E1004 | Invalid schema |
| E1005 | Invalid parameter |
| E1006 | Invalid date range |

### API Errors (E2xxx)
| Code | Description |
|------|-------------|
| E2001 | API unavailable |
| E2002 | Rate limited |
| E2003 | Authentication failed |
| E2004 | Resource not found |
| E2005 | General API error |

### File Errors (E3xxx)
| Code | Description |
|------|-------------|
| E3001 | File not found |
| E3002 | Invalid path |
| E3003 | Write error |
| E3004 | Read error |

### Data Errors (E4xxx)
| Code | Description |
|------|-------------|
| E4001 | No data available |
| E4002 | Parse error |

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Step-by-step setup guide
- **[USAGE.md](USAGE.md)** - Detailed usage examples and API reference
- **[Databento Docs](https://databento.com/docs)** - Official Databento documentation

## Project Structure

```
databento-mcp/
├── server.py          # Main MCP server implementation
├── cache.py           # File-based caching system with enhanced feedback
├── connection_pool.py # Databento client connection pooling
├── metrics.py         # Metrics collection and reporting
├── async_io.py        # Async file I/O operations
├── validation.py      # Input validation module
├── retry.py           # Retry logic with exponential backoff
├── errors.py          # Structured error codes and messages
├── summaries.py       # Smart data summaries and insights ✨
├── query_warnings.py  # Query size warnings and explain mode ✨
├── data_quality.py    # Data quality alerts and scoring ✨
├── requirements.txt   # Python dependencies
├── pyproject.toml     # Project configuration
├── test_setup.py      # Configuration verification script
├── test_errors.py     # Tests for error handling
├── test_validation.py # Tests for input validation
├── test_retry.py      # Tests for retry logic
├── test_connection_pool.py  # Tests for connection pooling
├── test_metrics.py    # Tests for metrics collection
├── test_async_io.py   # Tests for async file I/O
├── test_summaries.py  # Tests for data summaries ✨
├── test_query_warnings.py  # Tests for query warnings ✨
├── test_data_quality.py    # Tests for data quality ✨
├── test_cache.py      # Tests for enhanced cache ✨
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


