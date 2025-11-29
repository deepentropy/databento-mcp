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
✅ **Smart Caching** - File-based cache with automatic expiration to reduce API calls  
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

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Step-by-step setup guide
- **[USAGE.md](USAGE.md)** - Detailed usage examples and API reference
- **[Databento Docs](https://databento.com/docs)** - Official Databento documentation

## Project Structure

```
databento-mcp/
├── server.py          # Main MCP server implementation
├── cache.py           # File-based caching system
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


