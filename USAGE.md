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

## Additional Resources

- [Databento Documentation](https://databento.com/docs)
- [Databento Python SDK](https://github.com/databento/databento-python)
- [MCP Specification](https://spec.modelcontextprotocol.io/)

