# Databento MCP Server

A Model Context Protocol (MCP) server for accessing Databento's financial market data API.

## Features

✅ **Retrieve Historical Data** - Get trades, OHLCV bars, market depth, and more  
✅ **Symbol Metadata** - Access instrument definitions and symbology mappings  
✅ **Instrument Search** - Find available symbols with wildcard support  
✅ **Dataset Discovery** - List all available Databento datasets  
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


