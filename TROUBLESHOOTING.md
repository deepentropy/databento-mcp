# Troubleshooting Guide - Claude Desktop MCP Server

## ✅ Configuration Complete!

Your `claude_desktop_config.json` has been properly configured with the Databento MCP server.

**Config Location:** `C:\Users\otrem\AppData\Roaming\Claude\claude_desktop_config.json`

## 🔧 Configuration Applied

```json
{
  "mcpServers": {
    "databento": {
      "command": "python",
      "args": [
        "C:\\Users\\otrem\\PycharmProjects\\databento-mcp\\server.py"
      ],
      "env": {
        "DATABENTO_API_KEY": "db-vBvX...76"
      }
    }
  }
}
```

## ⚠️ CRITICAL: Restart Required!

**Claude Desktop MUST be completely restarted for the server to appear:**

### Windows Restart Steps:
1. **Close Claude Desktop** - Click X or right-click taskbar icon → Close
2. **Verify it's closed** - Check Task Manager (Ctrl+Shift+Esc) - no `claude.exe` should be running
3. **Wait 5 seconds**
4. **Reopen Claude Desktop** - Launch from Start Menu or desktop shortcut

## 🔍 How to Verify the Server is Working

After restarting Claude Desktop:

1. **Look for the hammer icon (🔨)** in the bottom-right corner of Claude Desktop
2. **Click the hammer icon** - This opens the MCP servers panel
3. **Look for "databento"** in the list of servers
4. **Check the status indicator** - Should show as connected/active

## ❌ Common Issues & Solutions

### Issue 1: Server Doesn't Appear in List

**Possible Causes:**
- Claude Desktop not fully restarted
- JSON syntax error in config file
- Wrong Python path
- Server crashes on startup

**Solutions:**
```powershell
# 1. Verify the config file is valid JSON
python -c "import json; json.load(open('C:/Users/otrem/AppData/Roaming/Claude/claude_desktop_config.json')); print('Valid JSON')"

# 2. Test the server manually
cd C:\Users\otrem\PycharmProjects\databento-mcp
python server.py
# Press Ctrl+C to stop after a few seconds

# 3. Check Python is accessible
where python
python --version

# 4. Reconfigure
python configure_claude.py
```

### Issue 2: Server Appears but Shows Error

**Possible Causes:**
- Invalid API key
- Missing dependencies
- Network issues

**Solutions:**
```powershell
# 1. Test API key and dependencies
python test_setup.py

# 2. Check API key in config
notepad "C:\Users\otrem\AppData\Roaming\Claude\claude_desktop_config.json"
# Make sure DATABENTO_API_KEY is your actual key, not "your_api_key_here"

# 3. Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### Issue 3: Server Connects but No Tools Available

**Possible Causes:**
- Server started but tool registration failed
- MCP protocol mismatch

**Solutions:**
```powershell
# 1. Update MCP library
pip install --upgrade mcp

# 2. Test server manually
python server.py
# Look for any error messages

# 3. Check Claude Desktop logs
# Windows: %APPDATA%\Claude\logs\
notepad "C:\Users\otrem\AppData\Roaming\Claude\logs\mcp.log"
```

### Issue 4: Python Command Not Found

**Solution:**
Update the config to use full Python path:

```json
{
  "mcpServers": {
    "databento": {
      "command": "C:\\Users\\otrem\\miniconda3\\envs\\databento-mcp\\python.exe",
      "args": [
        "C:\\Users\\otrem\\PycharmProjects\\databento-mcp\\server.py"
      ],
      "env": {
        "DATABENTO_API_KEY": "your_key_here"
      }
    }
  }
}
```

Find your Python path:
```powershell
where python
# Or for conda:
conda info --envs
```

## 📋 Verification Checklist

- [ ] Claude Desktop is completely closed (check Task Manager)
- [ ] Config file exists: `C:\Users\otrem\AppData\Roaming\Claude\claude_desktop_config.json`
- [ ] Config file is valid JSON (no syntax errors)
- [ ] Server path is correct and file exists
- [ ] API key is set (not placeholder)
- [ ] Python command works from terminal
- [ ] Dependencies installed (`pip list | findstr databento`)
- [ ] Claude Desktop restarted (not just minimized)

## 🧪 Manual Testing

Test the server independently:

```powershell
# Navigate to project
cd C:\Users\otrem\PycharmProjects\databento-mcp

# Run test script
python test_setup.py

# Expected output:
# ✓ DATABENTO_API_KEY is set
# ✓ databento 0.66.0
# ✓ mcp installed
# ✓ cache module available
# ✓ Databento client initialized
# ✓ Successfully connected to Databento API
```

If test_setup.py passes but Claude Desktop doesn't show the server:
- The issue is with Claude Desktop MCP integration, not the server
- Check Claude Desktop logs
- Try restarting your computer
- Verify Claude Desktop version supports MCP

## 📝 Check Claude Desktop Logs

Logs location: `C:\Users\otrem\AppData\Roaming\Claude\logs\`

```powershell
# View recent logs
Get-ChildItem "C:\Users\otrem\AppData\Roaming\Claude\logs\" | Sort-Object LastWriteTime -Descending | Select-Object -First 5

# Check MCP-specific logs
notepad "C:\Users\otrem\AppData\Roaming\Claude\logs\mcp.log"
```

Look for:
- Connection errors
- Python execution errors  
- JSON parsing errors
- Server startup failures

## 🔧 Quick Fix Commands

```powershell
# Reconfigure everything
cd C:\Users\otrem\PycharmProjects\databento-mcp
python configure_claude.py

# Kill Claude completely
taskkill /F /IM claude.exe /T

# Restart Claude
# (Use Start Menu to launch)

# Check if server file is accessible
python server.py
# Should start and wait for input (Ctrl+C to stop)
```

## 🆘 Still Not Working?

1. **Check Claude Desktop Version**
   - MCP support was added in recent versions
   - Update Claude Desktop to the latest version

2. **Verify MCP is Enabled**
   - Some Claude Desktop versions require enabling MCP in settings
   - Check Settings → Advanced → Model Context Protocol

3. **Try a Simple Test Server**
   ```powershell
   # Create a minimal test server
   python -m mcp.server.stdio
   ```

4. **Check Firewall/Antivirus**
   - Some security software blocks stdio communication
   - Try temporarily disabling to test

5. **Contact Support**
   - Databento: https://databento.com/support
   - Claude Desktop: Check app documentation

## ✅ Success Indicators

When everything works correctly:

1. **In Claude Desktop:**
   - 🔨 Hammer icon visible in bottom-right
   - "databento" appears in MCP servers list
   - Status shows as "Connected" or green indicator
   - 5 tools available: get_historical_data, get_symbol_metadata, search_instruments, list_datasets, clear_cache

2. **In Chat:**
   - Can ask Claude to use Databento tools
   - Example: "Get me the list of available Databento datasets"
   - Claude will use the server to fetch real data

## 📊 Expected Response

When working, you should be able to ask:

> "What datasets are available from Databento?"

And get a response showing ~27 datasets including:
- GLBX.MDP3
- XNAS.ITCH
- DBEQ.BASIC
- etc.

---

**Need more help?** Run `python configure_claude.py` to reconfigure the server.

