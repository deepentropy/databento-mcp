"""Quick verification that the Claude Desktop config is set up correctly."""
import json
from pathlib import Path
import os
import sys

def main():
    """Verify Claude Desktop configuration."""

    print("=" * 70)
    print("CLAUDE DESKTOP CONFIGURATION VERIFICATION")
    print("=" * 70)
    print()

    # Check config file
    if sys.platform == "win32":
        config_path = Path(os.getenv("APPDATA")) / "Claude" / "claude_desktop_config.json"
    else:
        config_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"

    print(f"Config file: {config_path}")

    if not config_path.exists():
        print("❌ Config file does NOT exist!")
        print(f"   Expected at: {config_path}")
        return 1

    print(f"✓ Config file exists ({config_path.stat().st_size} bytes)")
    print()

    # Read and validate JSON
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print("✓ Valid JSON syntax")
    except json.JSONDecodeError as e:
        print(f"❌ INVALID JSON: {e}")
        print("   Fix the JSON syntax errors in the config file")
        return 1

    # Check for mcpServers
    if "mcpServers" not in config:
        print("❌ No 'mcpServers' section found")
        return 1

    print("✓ 'mcpServers' section exists")

    # Check for databento server
    if "databento" not in config["mcpServers"]:
        print("❌ 'databento' server not configured")
        print()
        print("Available servers:", list(config["mcpServers"].keys()))
        return 1

    print("✓ 'databento' server is configured")
    print()

    # Display configuration
    db_config = config["mcpServers"]["databento"]
    print("Databento Server Configuration:")
    print("-" * 70)
    print(json.dumps(db_config, indent=2))
    print("-" * 70)
    print()

    # Validate configuration
    issues = []

    # Check command
    if "command" not in db_config:
        issues.append("Missing 'command' field")
    else:
        print(f"✓ Command: {db_config['command']}")

    # Check args
    if "args" not in db_config or not db_config["args"]:
        issues.append("Missing or empty 'args' field")
    else:
        server_path = Path(db_config["args"][0])
        print(f"✓ Server path: {server_path}")

        if not server_path.exists():
            issues.append(f"Server file does not exist: {server_path}")
            print(f"  ⚠ WARNING: File not found!")
        else:
            print(f"  ✓ Server file exists")

    # Check environment
    if "env" not in db_config:
        issues.append("Missing 'env' section")
    else:
        env = db_config["env"]
        if "DATABENTO_API_KEY" not in env:
            issues.append("Missing DATABENTO_API_KEY in env")
        else:
            api_key = env["DATABENTO_API_KEY"]
            if api_key == "your_api_key_here" or api_key == "db-changeme":
                issues.append("API key is still placeholder - needs to be replaced")
                print(f"  ⚠ API Key: PLACEHOLDER (needs update)")
            elif api_key.startswith("db-"):
                print(f"  ✓ API Key: {api_key[:8]}...{api_key[-4:]}")
            else:
                print(f"  ⚠ API Key: Set but format unusual")

    print()

    # Report issues
    if issues:
        print("⚠ ISSUES FOUND:")
        for issue in issues:
            print(f"  • {issue}")
        print()
    else:
        print("✅ ALL CHECKS PASSED!")
        print()

    # Final instructions
    print("=" * 70)
    print("NEXT STEPS:")
    print("=" * 70)
    print()

    if issues:
        print("1. Fix the issues listed above")
        print("2. Run: python configure_claude.py")
        print("3. Restart Claude Desktop")
    else:
        print("1. ⚠  RESTART CLAUDE DESKTOP (quit completely and reopen)")
        print("2. Look for the hammer icon 🔨 in Claude Desktop")
        print("3. Click it to see MCP servers")
        print("4. 'databento' should appear in the list")

    print()
    print("If the server still doesn't appear after restart:")
    print("  • Read TROUBLESHOOTING.md for detailed help")
    print("  • Check Claude Desktop logs in:")
    print(f"    {config_path.parent / 'logs'}")
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
                print("⚠ IMPORTANT: Claude Desktop is currently running!")
                print("   Changes will NOT take effect until you restart it.")
                print()
        except:
            pass

    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())

