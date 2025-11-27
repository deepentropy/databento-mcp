"""
Configure Claude Desktop to use the Databento MCP Server.
This script creates/updates the claude_desktop_config.json file.
"""
import json
import os
import sys
from pathlib import Path

def main():
    """Configure Claude Desktop for Databento MCP Server."""
    
    # Determine Claude config path
    if sys.platform == "win32":
        config_path = Path(os.getenv("APPDATA")) / "Claude" / "claude_desktop_config.json"
    elif sys.platform == "darwin":
        config_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    else:
        print("❌ Unsupported platform. Please configure manually.")
        return 1
    
    print("Databento MCP Server - Claude Desktop Configuration")
    print("=" * 60)
    print()
    print(f"Config file: {config_path}")
    print()
    
    # Check if config exists
    if config_path.exists():
        print(f"✓ Config file exists ({config_path.stat().st_size} bytes)")
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                existing_config = json.load(f)
            print("✓ Existing config is valid JSON")
        except json.JSONDecodeError as e:
            print(f"⚠ Existing config has invalid JSON: {e}")
            existing_config = {}
    else:
        print("ℹ Config file doesn't exist yet")
        existing_config = {}
    
    # Get server path
    server_path = Path(__file__).parent / "server.py"
    server_path_str = str(server_path.absolute()).replace("\\", "\\\\")
    
    # Get API key
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        print()
        print("⚠ DATABENTO_API_KEY not found in environment")
        print("  The server will need your API key to function.")
        print()
        api_key = input("Enter your Databento API key (or press Enter to skip): ").strip()
        if not api_key:
            api_key = "your_api_key_here"
            print("  Using placeholder - you'll need to update the config manually")
    else:
        print(f"✓ Found API key: {api_key[:8]}...")
    
    # Create new server configuration
    server_config = {
        "command": "python",
        "args": [str(server_path.absolute())],
        "env": {
            "DATABENTO_API_KEY": api_key
        }
    }
    
    # Update configuration
    if "mcpServers" not in existing_config:
        existing_config["mcpServers"] = {}
    
    existing_config["mcpServers"]["databento"] = server_config
    
    # Backup existing config
    if config_path.exists():
        backup_path = config_path.with_suffix('.json.backup')
        import shutil
        shutil.copy2(config_path, backup_path)
        print(f"✓ Backed up existing config to: {backup_path}")
    
    # Write new config
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(existing_config, f, indent=2)
    
    print(f"✓ Configuration saved to: {config_path}")
    print()
    
    # Display the configuration
    print("Server configuration:")
    print("-" * 60)
    print(json.dumps(server_config, indent=2))
    print("-" * 60)
    print()
    
    # Show all configured servers
    if len(existing_config.get("mcpServers", {})) > 1:
        print(f"Total MCP servers configured: {len(existing_config['mcpServers'])}")
        for server_name in existing_config["mcpServers"].keys():
            marker = "✓" if server_name == "databento" else " "
            print(f"  {marker} {server_name}")
        print()
    
    print("✅ Configuration complete!")
    print()
    print("Next steps:")
    print("1. ⚠  RESTART Claude Desktop app (completely quit and reopen)")
    print("2. Look for the hammer icon (🔨) in Claude Desktop")
    print("3. Click it to see available MCP servers")
    print("4. 'databento' should appear in the list")
    print()
    print("If the server doesn't appear:")
    print("- Make sure Claude Desktop is completely closed")
    print("- Check the config file for JSON errors")
    print("- Verify the server path is correct")
    print("- Check Claude Desktop logs for errors")
    print()
    
    # Check if Claude is running
    if sys.platform == "win32":
        import subprocess
        try:
            result = subprocess.run(
                ['tasklist', '/FI', 'IMAGENAME eq claude.exe'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if "claude.exe" in result.stdout.lower():
                print("⚠ WARNING: Claude Desktop appears to be running!")
                print("   You MUST restart it for changes to take effect.")
                print()
        except:
            pass
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

