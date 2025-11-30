![Databento MCP](https://raw.githubusercontent.com/deepentropy/databento-mcp/main/banner.svg)

A Model Context Protocol server for Databento market data.

[![PyPI version](https://badge.fury.io/py/databento-mcp.svg)](https://badge.fury.io/py/databento-mcp)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Installation

```bash
pip install databento-mcp
```

## Quick Start

1. Get your API key from [Databento](https://databento.com)
2. Configure your MCP client (see setup guides below)
3. Start querying market data through your AI assistant

## Setup Guides

### Claude Desktop

Add to your configuration file:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databento": {
      "command": "databento-mcp",
      "env": {
        "DATABENTO_API_KEY": "your-api-key"
      }
    }
  }
}
```

### GitHub Copilot CLI

Add the server to your Copilot CLI configuration:

```bash
gh copilot config set mcp-servers '{
  "databento": {
    "command": "databento-mcp",
    "env": {
      "DATABENTO_API_KEY": "your-api-key"
    }
  }
}'
```

Or add to your `~/.config/gh-copilot/config.yml`:

```yaml
mcp-servers:
  databento:
    command: databento-mcp
    env:
      DATABENTO_API_KEY: your-api-key
```

See [GitHub Copilot CLI MCP documentation](https://docs.github.com/en/copilot/how-tos/use-copilot-agents/use-copilot-cli#add-an-mcp-server) for more details.

### ChatGPT (via Developer Mode)

ChatGPT supports MCP servers through Developer Mode. 

1. Enable Developer Mode in ChatGPT settings
2. Add an MCP server with the following configuration:

```json
{
  "name": "databento",
  "command": "databento-mcp",
  "env": {
    "DATABENTO_API_KEY": "your-api-key"
  }
}
```

See [OpenAI Developer Mode documentation](https://platform.openai.com/docs/guides/developer-mode) for detailed setup instructions.

## Features

### Historical Data
- Retrieve trades, OHLCV bars, market depth, and more
- Support for all Databento schemas (trades, mbp-1, mbp-10, ohlcv-*, etc.)
- Cost estimation before query execution
- Smart data summaries with statistics

### Live Data
- Real-time market data streaming
- Configurable stream duration
- Multiple schema support

### File Operations
- Read/write DBN format files
- Export to Apache Parquet
- Convert between formats

### Batch Processing
- Submit large-scale batch jobs
- Monitor job status
- Download completed files

### Reference Data
- Symbol metadata and definitions
- Symbology resolution
- Dataset discovery
- Publisher information

### Quality & Performance
- Smart caching with configurable TTL
- Data quality analysis
- Connection pooling
- Comprehensive metrics

## Available Tools

| Tool | Description |
|------|-------------|
| `health_check` | Check API connectivity and server status |
| `get_historical_data` | Retrieve historical market data |
| `get_live_data` | Stream real-time market data |
| `get_cost` | Estimate query cost before execution |
| `get_symbol_metadata` | Get instrument definitions and mappings |
| `search_instruments` | Search for symbols with wildcards |
| `list_datasets` | List available Databento datasets |
| `list_schemas` | List available data schemas |
| `resolve_symbols` | Convert between symbology types |
| `submit_batch_job` | Submit batch data download |
| `list_batch_jobs` | List batch job status |
| `get_batch_job_files` | Get batch job download info |
| `cancel_batch_job` | Cancel pending batch job |
| `download_batch_files` | Download completed batch files |
| `read_dbn_file` | Parse and read DBN files |
| `get_dbn_metadata` | Get DBN file metadata |
| `write_dbn_file` | Write data to DBN format |
| `convert_dbn_to_parquet` | Convert DBN to Parquet |
| `export_to_parquet` | Query and export to Parquet |
| `read_parquet_file` | Read Parquet files |
| `get_session_info` | Get trading session info |
| `list_publishers` | List data publishers |
| `list_fields` | List schema fields |
| `get_dataset_range` | Get dataset date range |
| `list_unit_prices` | Get pricing information |
| `analyze_data_quality` | Analyze data quality issues |
| `quick_analysis` | Comprehensive symbol analysis |
| `get_account_status` | Server status and metrics |
| `get_metrics` | Performance metrics |
| `clear_cache` | Clear API response cache |

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `DATABENTO_API_KEY` | Databento API key (required) | - |
| `DATABENTO_DATA_DIR` | Restrict file operations to directory | Current directory |
| `DATABENTO_LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| `DATABENTO_METRICS_ENABLED` | Enable metrics collection | true |

## Common Datasets

| Dataset | Description |
|---------|-------------|
| `GLBX.MDP3` | CME Globex (ES, NQ, CL futures) |
| `XNAS.ITCH` | Nasdaq TotalView |
| `XNYS.PILLAR` | NYSE |
| `DBEQ.BASIC` | Consolidated US equities |
| `OPRA.PILLAR` | US options |
| `IFEU.IMPACT` | ICE Futures Europe |

## Common Schemas

| Schema | Description |
|--------|-------------|
| `trades` | Individual trades |
| `ohlcv-1m` | 1-minute OHLCV bars |
| `ohlcv-1h` | 1-hour OHLCV bars |
| `ohlcv-1d` | Daily OHLCV bars |
| `mbp-1` | Top of book |
| `mbp-10` | 10-level order book |
| `tbbo` | Top bid/offer |
| `definition` | Instrument definitions |

## Development

```bash
# Clone repository
git clone https://github.com/deepentropy/databento-mcp.git
cd databento-mcp

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black src/
ruff check src/
```

## License

[MIT License](LICENSE)

## Links

- [Databento Documentation](https://databento.com/docs)
- [Databento Python SDK](https://github.com/databento/databento-python)
- [MCP Specification](https://spec.modelcontextprotocol.io/)
