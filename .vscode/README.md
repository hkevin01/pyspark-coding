# VS Code Configuration for PySpark Development

This workspace is fully configured for professional PySpark development with optimal settings and recommended extensions.

## 📦 Recommended Extensions (Top 10)

When you open this workspace, VS Code will prompt you to install these extensions:

### Essential (Top 5):
1. **Python** (`ms-python.python`) - Core Python support
2. **Pylance** (`ms-python.vscode-pylance`) - Fast, feature-rich Python language server
3. **Jupyter** (`ms-toolsai.jupyter`) - Notebook support with PySpark integration
4. **Remote - SSH** (`ms-vscode-remote.remote-ssh`) - Develop on remote clusters
5. **Black Formatter** (`ms-python.black-formatter`) - Python code formatting

### Highly Recommended:
6. **GitLens** (`eamodio.gitlens`) - Advanced Git capabilities
7. **Docker** (`ms-azuretools.vscode-docker`) - Container development
8. **SQLTools** (`mtxr.sqltools`) - SQL query support (Hive, etc.)
9. **YAML** (`redhat.vscode-yaml`) - Config file support
10. **Markdown All in One** (`yzhang.markdown-all-in-one`) - Documentation

### Install All at Once:
```bash
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension ms-toolsai.jupyter
code --install-extension ms-vscode-remote.remote-ssh
code --install-extension ms-python.black-formatter
code --install-extension eamodio.gitlens
code --install-extension ms-azuretools.vscode-docker
code --install-extension mtxr.sqltools
code --install-extension redhat.vscode-yaml
code --install-extension yzhang.markdown-all-in-one
```

---

## ⚙️ Settings Configuration (settings.json)

### Python Analysis & IntelliSense
- **Pylance language server** - Best-in-class Python IntelliSense
- **Type checking** enabled (basic mode) - Catch errors early
- **Auto-import completions** - Suggests missing imports automatically
- **Function parameter completion** - Auto-completes function calls
- **Inlay hints** - Shows return types and variable types inline
- **Workspace analysis** - Analyzes entire project for better suggestions

### PySpark-Specific Settings
- **Source path** added to Python path (`src/` folder)
- **Spark metadata excluded** from file explorer:
  - `spark-warehouse/`, `metastore_db/`, `derby.log`
  - `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`
- **Large data files excluded** from search and watch:
  - `data/`, `logs/`, `*.parquet`, `*.orc`

### Code Formatting
- **Black formatter** - Industry standard (88 char line length)
- **Format on save** - Automatic formatting
- **Auto-organize imports** - Clean import statements
- **Rulers at 88 and 120 chars** - Visual line length guides

### Testing
- **pytest enabled** - Modern Python testing
- **Verbose output** - Detailed test results
- **Short tracebacks** - Easier error diagnosis

### Terminal Environment
Pre-configured environment variables for all platforms (Linux, macOS, Windows):
- `PYSPARK_PYTHON=python3`
- `PYSPARK_DRIVER_PYTHON=python3`
- `SPARK_LOCAL_IP=127.0.0.1`
- `PYTHONPATH` includes `src/` folder

### Jupyter Notebooks
- **No restart prompts** - Faster iteration
- **Workspace as notebook root** - Proper path resolution
- **Widget support** - Interactive visualizations

### File Associations
- `.hql`, `.q` files → SQL syntax highlighting
- `.spark` files → Python syntax highlighting

### Code Linting
- **Flake8 enabled** - Python style checking
- **120 char line length** - Reasonable for PySpark
- **Common warnings ignored** - Focus on real issues

---

## 🐛 Debug Configurations (launch.json)

### 1. PySpark: Current File (Local)
Debug the currently open file with PySpark in local mode.
- **Use when**: Testing PySpark code on your laptop
- **Shortcut**: F5

### 2. PySpark: Local Mode (4 cores)
Debug with explicit local mode using 4 CPU cores.
- **Use when**: Simulating parallel execution locally
- **Config**: `SPARK_MASTER=local[4]`

### 3. PySpark: Debug with Arguments
Debug with command-line arguments (input/output paths).
- **Use when**: Testing ETL pipelines with specific paths
- **Args**: `--input data/sample/ --output data/processed/`

### 4. PySpark: Attach to Remote
Attach debugger to remote PySpark job running on cluster.
- **Use when**: Debugging production issues on cluster
- **Port**: 5678 (configurable)
- **Setup**:
  ```python
  import debugpy
  debugpy.listen(("0.0.0.0", 5678))
  debugpy.wait_for_client()
  ```

### 5. Python: Current File
Standard Python debugging (no Spark).
- **Use when**: Testing utility functions or non-Spark code

### 6. Python: Module (pytest)
Run and debug pytest tests.
- **Use when**: Running unit tests
- **Args**: `tests/ -v`

---

## 🚀 Quick Start

### 1. Install Extensions
When you open this workspace, VS Code will prompt:
```
"This workspace recommends extensions. Would you like to install them?"
→ Click "Install All"
```

### 2. Verify Configuration
Open any Python file and type:
```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df = spark.read  # ← Type a dot and see autocomplete!
```

You should see:
- ✅ All PySpark methods suggested
- ✅ Parameter hints shown
- ✅ Documentation on hover
- ✅ Type checking warnings

### 3. Start Debugging
1. Open `src/spark_session/01_session_basics.py`
2. Set a breakpoint (click left gutter)
3. Press **F5** or select "PySpark: Current File (Local)"
4. Code pauses at breakpoint
5. Inspect variables, step through code

### 4. Use Jupyter Notebooks
1. Open `notebooks/examples/00_hello_world.ipynb`
2. VS Code opens it natively (not in browser)
3. Run cells with **Shift+Enter**
4. See output inline
5. Use variable explorer to inspect DataFrames

---

## 🎯 Common PySpark Tasks

### Run PySpark Script
```bash
# Terminal automatically has correct environment
python src/my_script.py
```

### Debug PySpark Script
1. Open file
2. Press **F5**
3. Choose "PySpark: Current File (Local)"

### Format Code
- **Save file** → Automatically formatted with Black
- **Or**: Right-click → "Format Document"

### Run Tests
```bash
# Terminal or use debug configuration
pytest tests/ -v
```

### Connect to Remote Cluster
1. Install Remote-SSH extension
2. **Cmd+Shift+P** → "Remote-SSH: Connect to Host"
3. Enter: `user@cluster-node.example.com`
4. VS Code reopens, now running on cluster
5. Edit files directly on cluster
6. Run `spark-submit` immediately (no upload needed)

---

## 📊 Spark UI Integration

When running PySpark locally, Spark UI is available at:
```
http://localhost:4040
```

VS Code terminal will show this URL. Click to open in browser and see:
- Job progress
- Stage details
- DAG visualization
- Task metrics
- SQL queries
- Storage info

---

## 🔧 Customization

### Change Spark Configuration
Edit `settings.json` → `terminal.integrated.env.linux`:
```json
"terminal.integrated.env.linux": {
  "SPARK_MASTER": "local[8]",  // Use 8 cores
  "SPARK_DRIVER_MEMORY": "4g",
  "SPARK_EXECUTOR_MEMORY": "2g"
}
```

### Add More Debug Configurations
Edit `launch.json` → add new configuration:
```json
{
  "name": "PySpark: My Custom Config",
  "type": "debugpy",
  "request": "launch",
  "program": "${file}",
  "env": {
    "SPARK_MASTER": "yarn",
    "SPARK_HOME": "/opt/spark"
  }
}
```

### Exclude More Files
Edit `settings.json` → `files.exclude`:
```json
"files.exclude": {
  "**/.mypy_cache": true,
  "**/my_custom_folder": true
}
```

---

## 💡 Pro Tips

### 1. Interactive Python (Jupyter in .py files)
Add `# %%` to create notebook-like cells in Python files:
```python
# %%
spark = SparkSession.builder.getOrCreate()

# %%
df = spark.read.parquet("data.parquet")
df.show()  # Run just this cell with Shift+Enter
```

### 2. Type Hints for Better IntelliSense
```python
from pyspark.sql import DataFrame

def process_data(df: DataFrame) -> DataFrame:
    # VS Code now knows df is PySpark DataFrame!
    return df.filter(...)  # Perfect autocomplete
```

### 3. Quick Fix Imports
Type `SparkSession` without import:
- VS Code underlines it
- Click lightbulb 💡
- Select "Import SparkSession from pyspark.sql"
- Auto-added to top of file

### 4. Multi-Cursor Editing
Hold **Alt** (or **Option** on Mac) and click multiple places.
Useful for editing multiple similar lines at once.

### 5. Command Palette
**Cmd+Shift+P** (Mac) or **Ctrl+Shift+P** (Linux/Windows):
- Access ALL VS Code commands
- Search: "Python: Select Interpreter"
- Search: "Format Document"
- Search: "Remote-SSH: Connect"

---

## 🆘 Troubleshooting

### Pylance Not Working
```bash
# Reload VS Code window
Cmd+Shift+P → "Developer: Reload Window"
```

### No PySpark Autocomplete
1. Check Python interpreter: Bottom-right corner
2. Should be: Python 3.x with pyspark installed
3. If not: **Cmd+Shift+P** → "Python: Select Interpreter"

### Debugger Not Starting
1. Check `launch.json` has correct paths
2. Verify `SPARK_HOME` environment variable set
3. Try: "Python: Current File" first (simpler)

### Extensions Not Installing
```bash
# Install manually
code --install-extension ms-python.python --force
```

### Spark UI Not Opening
1. Check if PySpark is running: `ps aux | grep spark`
2. Try: `http://localhost:4040`
3. If port taken, try: `http://localhost:4041`

---

## 📚 Learn More

- **VS Code Python**: https://code.visualstudio.com/docs/python/python-tutorial
- **Pylance Features**: https://devblogs.microsoft.com/python/pylance-features/
- **Remote Development**: https://code.visualstudio.com/docs/remote/remote-overview
- **Jupyter in VS Code**: https://code.visualstudio.com/docs/datascience/jupyter-notebooks

---

## ✨ What's Configured

✅ Pylance language server with PySpark types
✅ Auto-import suggestions
✅ Type checking (catches errors before running)
✅ Black code formatter (88 char line length)
✅ PySpark environment variables (all platforms)
✅ Spark metadata files excluded from search
✅ Jupyter notebook integration
✅ 6 debug configurations for different scenarios
✅ pytest testing framework
✅ Flake8 linting with PySpark-friendly rules
✅ Git integration and auto-fetch
✅ File associations (*.hql, *.spark)
✅ Remote SSH development ready

**You're all set for professional PySpark development! 🚀**
