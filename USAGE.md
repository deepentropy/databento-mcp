# Databento MCP Server - Usage Examples

This document provides examples of how to use the Databento MCP Server.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure API key:**
   ```bash
   cp .env.example .env
   # Edit .env and add your Databento API key
   ```

3. **Test configuration:**
   ```bash
   python test_setup.py
   ```

4. **Run the server:**
   ```bash
   python server.py
   ```

## Available Tools

### 1. get_historical_data

Retrieve historical market data for symbols.

**Parameters:**
- `dataset`: Dataset name (e.g., "GLBX.MDP3", "XNAS.ITCH")
- `symbols`: Comma-separated list of symbols
- `start`: Start date (YYYY-MM-DD)
- `end`: End date (YYYY-MM-DD)
- `schema`: Data schema (default: "trades")
  - Options: "trades", "ohlcv-1m", "ohlcv-1h", "ohlcv-1d", "mbp-1", "mbp-10", "tbbo", etc.
- `limit`: Maximum records to return (default: 1000)

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES.FUT",
  "start": "2024-01-01",
  "end": "2024-01-02",
  "schema": "ohlcv-1m",
  "limit": 1000
}
```

### 2. get_symbol_metadata

Get metadata for symbols including symbology mappings and instrument definitions.

**Parameters:**
- `dataset`: Dataset name
- `symbols`: Comma-separated list of symbols
- `start`: Start date (YYYY-MM-DD)
- `end`: End date (YYYY-MM-DD, optional)

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES.FUT,NQ.FUT",
  "start": "2024-01-01"
}
```

### 3. search_instruments

Search for instruments in a dataset.

**Parameters:**
- `dataset`: Dataset name
- `symbols`: Symbol pattern (supports wildcards like "*")
- `start`: Start date (YYYY-MM-DD)

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES*",
  "start": "2024-01-01"
}
```

### 4. list_datasets

List all available datasets from Databento.

**Parameters:** None

**Example:**
```json
{}
```

### 5. clear_cache

Clear the API response cache.

**Parameters:**
- `expired_only`: Only clear expired entries (default: false)

**Example:**
```json
{
  "expired_only": true
}
```

### 6. get_cost

Estimate the cost of a historical data query before executing it.

**Parameters:**
- `dataset`: Dataset name
- `symbols`: Comma-separated list of symbols
- `schema`: Data schema
- `start`: Start date (YYYY-MM-DD or ISO 8601)
- `end`: End date (YYYY-MM-DD or ISO 8601)

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES.FUT",
  "schema": "trades",
  "start": "2024-01-01",
  "end": "2024-01-02"
}
```

### 7. get_live_data

Subscribe to real-time market data for a limited duration.

**Parameters:**
- `dataset`: Dataset name
- `symbols`: Comma-separated list of symbols
- `schema`: Data schema (default: "trades")
- `duration`: How long to stream data in seconds (default: 10, max: 60)

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES.FUT",
  "schema": "trades",
  "duration": 10
}
```

### 8. resolve_symbols

Resolve symbols between different symbology types.

**Parameters:**
- `dataset`: Dataset name
- `symbols`: Comma-separated list of symbols to resolve
- `stype_in`: Input symbol type (e.g., "raw_symbol", "instrument_id")
- `stype_out`: Output symbol type (e.g., "instrument_id", "raw_symbol")
- `start`: Start date (YYYY-MM-DD)
- `end`: End date (YYYY-MM-DD, optional)

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES.FUT",
  "stype_in": "raw_symbol",
  "stype_out": "instrument_id",
  "start": "2024-01-01"
}
```

### 9. submit_batch_job

Submit a batch data download job for large historical datasets.

**Parameters:**
- `dataset`: Dataset name
- `symbols`: Comma-separated list of symbols
- `schema`: Data schema
- `start`: Start date (YYYY-MM-DD or ISO 8601)
- `end`: End date (YYYY-MM-DD or ISO 8601)
- `encoding`: Output encoding (default: "dbn", options: "dbn", "csv", "json")
- `compression`: Compression type (default: "zstd", options: "none", "zstd")
- `split_duration`: Split files by duration (default: "day", options: "day", "week", "month", "none")

**Example:**
```json
{
  "dataset": "GLBX.MDP3",
  "symbols": "ES.FUT,NQ.FUT",
  "schema": "trades",
  "start": "2024-01-01",
  "end": "2024-01-31",
  "encoding": "csv",
  "compression": "zstd",
  "split_duration": "day"
}
```

### 10. list_batch_jobs

List all batch jobs with their current status.

**Parameters:**
- `states`: Filter by states (default: "queued,processing,done", options: "received", "queued", "processing", "done", "expired")
- `since`: Only show jobs since this date (ISO 8601)
- `limit`: Maximum number of jobs to return (default: 20)

**Example:**
```json
{
  "states": "queued,processing,done",
  "limit": 10
}
```

### 11. get_batch_job_files

Get download information for a completed batch job.

**Parameters:**
- `job_id`: The batch job ID

**Example:**
```json
{
  "job_id": "ABCD-1234-EFGH-5678"
}
```

### 12. get_session_info

Identify the current trading session based on time.

**Parameters:**
- `timestamp`: ISO 8601 timestamp (optional, defaults to current time)

**Example - Current session:**
```json
{}
```

**Example - Specific timestamp:**
```json
{
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Session Definitions:**
- **Asian**: 00:00 - 07:00 UTC
- **London**: 07:00 - 14:00 UTC
- **NY**: 14:00 - 22:00 UTC
- **Off-hours**: 22:00 - 00:00 UTC

### 13. list_publishers

List data publishers with their details.

**Parameters:**
- `dataset`: Filter by dataset (optional)

**Example - All publishers:**
```json
{}
```

**Example - Filter by dataset:**
```json
{
  "dataset": "GLBX.MDP3"
}
```

### 14. list_fields

List fields available for a specific schema.

**Parameters:**
- `schema`: Schema name (e.g., "trades", "mbp-1")
- `encoding`: Encoding format (default: "json", options: "dbn", "csv", "json")

**Example:**
```json
{
  "schema": "trades",
  "encoding": "json"
}
```

### 15. get_dataset_range

Get the available date range for a dataset.

**Parameters:**
- `dataset`: Dataset name

**Example:**
```json
{
  "dataset": "GLBX.MDP3"
}
```

## Common Datasets

- **GLBX.MDP3**: CME Globex MDP 3.0 (futures and options)
- **XNAS.ITCH**: Nasdaq TotalView-ITCH (equities)
- **XNYS.TRADES**: NYSE Trades (equities)
- **OPRA.PILLAR**: OPRA (US options)
- **DBEQ.BASIC**: Databento Equities Basic (consolidated)

## Common Schemas

- **trades**: Individual trades
- **ohlcv-1m**: 1-minute OHLCV bars
- **ohlcv-1h**: 1-hour OHLCV bars
- **ohlcv-1d**: Daily OHLCV bars
- **mbp-1**: Market by price (top of book)
- **mbp-10**: Market by price (10 levels)
- **tbbo**: Top of book best bid/offer
- **definition**: Instrument definitions

## Cache Behavior

The server implements a file-based cache to reduce API calls:

- Historical data: 1 hour TTL
- Metadata: 2 hours TTL
- Dataset list: 24 hours TTL
- Publisher list: 24 hours TTL
- Field list: 24 hours TTL
- Dataset range: 1 hour TTL
- Batch job status: 5 minutes TTL
- Cost estimates: 30 minutes TTL
- Cache is automatically checked on each request
- Expired entries are deleted automatically
- Use `clear_cache` tool to manually clear cache

## Integration with MCP Clients

### Claude Desktop

Add to your Claude Desktop config file:

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databento": {
      "command": "python",
      "args": ["C:\\path\\to\\databento-mcp\\server.py"],
      "env": {
        "DATABENTO_API_KEY": "your_api_key_here"
      }
    }
  }
}
```

### Other MCP Clients

Use the standard MCP stdio protocol to connect to the server. The server communicates over stdin/stdout.

## Tips

1. **Start with small date ranges** to avoid hitting rate limits
2. **Use the cache** by querying the same data multiple times
3. **Check available schemas** for your dataset in the Databento documentation
4. **Symbol formats vary** by dataset - check the dataset documentation
5. **Use wildcards** in search_instruments to discover available symbols
6. **Use get_cost** before executing large queries to estimate costs
7. **Use batch jobs** for large historical data downloads
8. **Check session info** to understand market hours context

## Troubleshooting

### "DATABENTO_API_KEY environment variable not set"
- Make sure you've created a `.env` file with your API key
- Or set the environment variable: `$env:DATABENTO_API_KEY="your_key"`

### "Error retrieving historical data"
- Check that the dataset name is correct
- Verify the symbol format for your dataset
- Ensure the date range is valid
- Check that you have access to the dataset

### "No instruments found"
- Try using wildcards: "ES*" instead of "ES"
- Check the start date is within the dataset range
- Verify the dataset name is correct

### "Error submitting batch job"
- Verify all required parameters are provided
- Check that symbols exist in the dataset
- Ensure start and end dates are valid

## Additional Resources

- [Databento Documentation](https://databento.com/docs)
- [Databento Python SDK](https://github.com/databento/databento-python)
- [Databento Batch API Reference](https://databento.com/docs/api-reference-historical/batch)
- [MCP Specification](https://spec.modelcontextprotocol.io/)

