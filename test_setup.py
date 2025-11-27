"""Test script to verify the Databento MCP server setup."""
import os
import sys
from dotenv import load_dotenv


def main():
    """Check if the environment is properly configured."""
    load_dotenv()

    print("Databento MCP Server Configuration Check")
    print("=" * 50)
    print()

    # Check API key
    api_key = os.getenv("DATABENTO_API_KEY")
    if api_key:
        print("✓ DATABENTO_API_KEY is set")
        print(f"  Key: {api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "  Key: ***")
    else:
        print("✗ DATABENTO_API_KEY is not set")
        print("  Please set your API key in a .env file or as an environment variable")
        print()
        print("  Example:")
        print("    1. Copy .env.example to .env")
        print("    2. Edit .env and add your API key")
        print("    3. Or set: $env:DATABENTO_API_KEY=\"your_key\"")
        return 1

    print()
    print("Checking imports...")

    try:
        import databento
        print(f"✓ databento {databento.__version__}")
    except ImportError as e:
        print(f"✗ databento not installed: {e}")
        return 1

    try:
        import mcp
        print("✓ mcp installed")
    except ImportError as e:
        print(f"✗ mcp not installed: {e}")
        return 1

    try:
        from cache import Cache
        print("✓ cache module available")
    except ImportError as e:
        print(f"✗ cache module not found: {e}")
        return 1

    print()
    print("Testing Databento connection...")

    try:
        import databento as db
        client = db.Historical(api_key)
        print("✓ Databento client initialized")

        # Try to list datasets
        datasets = client.metadata.list_datasets()
        print(f"✓ Successfully connected to Databento API")
        print(f"  Available datasets: {len(datasets)}")
        print(f"  Sample datasets: {', '.join(list(datasets)[:5])}")

    except Exception as e:
        print(f"✗ Failed to connect to Databento: {e}")
        return 1

    print()
    print("=" * 50)
    print("Configuration check complete!")
    print()
    print("To run the server:")
    print("  python server.py")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())

