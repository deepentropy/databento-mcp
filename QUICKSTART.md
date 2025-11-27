# Quick Start Guide

## 1. Initial Setup

```powershell
# Navigate to the project directory
cd C:\Users\otrem\PycharmProjects\databento-mcp

# Install dependencies (already done)
pip install -r requirements.txt
```

## 2. Configure API Key

Create a `.env` file:

```powershell
# Copy the example file
Copy-Item .env.example .env

# Edit .env and add your Databento API key
notepad .env
```

Or set it as an environment variable:

```powershell
$env:DATABENTO_API_KEY="your_api_key_here"
```

## 3. Test Configuration

```powershell
python test_setup.py
```

Expected output:
```
Databento MCP Server Configuration Check
==================================================

✓ DATABENTO_API_KEY is set
  Key: db-xxxxx...xxxx

Checking imports...
✓ databento 0.66.0
✓ mcp installed
✓ cache module available

Testing Databento connection...
✓ Databento client initialized
✓ Successfully connected to Databento API
  Available datasets: XX
  Sample datasets: GLBX.MDP3, XNAS.ITCH, ...

==================================================
Configuration check complete!

To run the server:
  python server.py
```

## 4. Run the Server

```powershell
python server.py
```

The server will now be running and listening for MCP protocol messages on stdin/stdout.

## 5. Configure MCP Client (e.g., Claude Desktop)

Edit your Claude Desktop configuration file:

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

Add the following configuration:

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

## 6. Example Usage

Once connected through an MCP client, you can use these tools:

### Get Historical Data
```
Please get me 1-minute OHLCV data for ES futures from 2024-01-01 to 2024-01-02 
using the GLBX.MDP3 dataset.
```

### Get Symbol Metadata
```
Show me metadata for ES.FUT and NQ.FUT symbols in the GLBX.MDP3 dataset 
starting from 2024-01-01.
```

### Search Instruments
```
Search for all instruments starting with "ES" in the GLBX.MDP3 dataset.
```

### List Datasets
```
What datasets are available from Databento?
```

## Troubleshooting

### Common Issues

1. **"DATABENTO_API_KEY not set"**
   - Make sure you created a `.env` file with your API key
   - Or set the environment variable in your shell/MCP config

2. **"No module named 'databento'"**
   - Run: `pip install -r requirements.txt`

3. **Import errors in IDE**
   - These are just IDE warnings because it doesn't recognize the installed packages
   - The code will run fine when executed

4. **Connection timeout**
   - Check your internet connection
   - Verify your API key is valid
   - Check Databento service status

## Next Steps

- Read [USAGE.md](USAGE.md) for detailed documentation on all available tools
- Check the [Databento Documentation](https://databento.com/docs) for dataset information
- Review the cache behavior to optimize your queries

## Cache Location

The server creates a `cache/` directory to store API responses. You can:
- Clear all cache: Use the `clear_cache` tool
- Clear expired cache: Use `clear_cache` with `expired_only: true`
- Manually delete: Remove files from `cache/` directory

## Support

For issues with:
- **Databento API**: Check [Databento Support](https://databento.com/support)
- **MCP Protocol**: See [MCP Specification](https://spec.modelcontextprotocol.io/)
- **This Server**: Review the code in `server.py` and `cache.py`

