# PyQt5 GUI Testing and Troubleshooting Guide

## ✅ GUI System is Working!

The PyQt5 GUI system is properly installed and functional on your system.

## 🚀 Quick Start - Launch GUIs

### Option 1: Use the GUI Test Launcher (Recommended)
```bash
cd /home/kevin/Projects/pyspark-coding/src/interview
python3 gui_test_launcher.py
```

This will:
- ✅ Verify PyQt5 is working
- ✅ Show system information
- ✅ Provide buttons to launch all interview GUIs
- ✅ Display diagnostic information

### Option 2: Launch Individual GUIs Directly
```bash
# Batch ETL Practice
python3 batch_etl_gui.py

# Kafka Streaming Practice
python3 kafka_streaming_gui.py

# Kafka to Parquet Practice
python3 kafka_to_parquet_gui.py

# ETL Practice
python3 etl_practice_gui.py
```

## 🔍 Verifying PyQt5 Installation

### Test 1: Simple Import Test
```bash
python3 -c "from PyQt5.QtWidgets import QApplication; print('✅ PyQt5 is working!')"
```

### Test 2: Check Display
```bash
echo $DISPLAY
# Should show: :0 or similar (not empty)
```

### Test 3: Simple Window Test
```bash
python3 test_pyqt5.py
```

## 🐛 Troubleshooting

### Issue 1: "No module named 'PyQt5'"

**Solution:**
```bash
pip install PyQt5
# or
pip3 install PyQt5
```

### Issue 2: Window doesn't appear but process runs

**Possible causes:**
1. Window is minimized or off-screen
2. Wrong workspace/virtual desktop
3. Display variable issues

**Solutions:**
```bash
# Check if window is actually created
wmctrl -l  # List all windows

# Try resetting display
export DISPLAY=:0

# Check window manager
ps aux | grep -i "xorg\|wayland"
```

### Issue 3: "QXcbConnection: Could not connect to display"

**Solution:**
```bash
# Ensure X server is running
echo $DISPLAY
# Should not be empty

# If empty, set it:
export DISPLAY=:0

# Or try:
export DISPLAY=:1
```

### Issue 4: GUI runs but text is garbled

**Solution:**
```bash
# Install proper fonts
sudo apt-get install fonts-liberation fonts-dejavu

# Force PyQt5 to use specific style
python3 gui_test_launcher.py --style=Fusion
```

## 📋 System Requirements

- ✅ Python 3.x
- ✅ PyQt5 (`pip install PyQt5`)
- ✅ X11 or Wayland display server
- ✅ DISPLAY environment variable set
- ✅ Window manager running

## 🎯 Available Interview Practice GUIs

### 1. Batch ETL Practice GUI (`batch_etl_gui.py`)
- Practice batch ETL transformations
- 5 progressive difficulty levels
- Real-time code validation
- Built-in solutions and hints
- Performance metrics

**Topics covered:**
- Data cleaning and validation
- Aggregations and grouping
- Window functions
- Complex transformations
- Performance optimization

### 2. Kafka Streaming Practice GUI (`kafka_streaming_gui.py`)
- Real-time Kafka stream processing
- Windowed aggregations
- Stateful transformations
- Multiple difficulty levels
- Live solution validation

**Topics covered:**
- Kafka stream consumption
- JSON parsing and schemas
- Time windows (tumbling, sliding)
- Watermarks and late data
- Stream-to-stream joins

### 3. Kafka to Parquet Practice GUI (`kafka_to_parquet_gui.py`)
- Data lake ingestion patterns
- Kafka to Parquet conversion
- Partitioning strategies
- Schema evolution
- Production patterns

**Topics covered:**
- Streaming to batch conversion
- Parquet optimization
- Partition pruning
- Delta Lake patterns
- Performance tuning

### 4. ETL Practice GUI (`etl_practice_gui.py`)
- End-to-end ETL pipelines
- Multi-source integration
- Data quality checks
- Error handling
- Production deployment

**Topics covered:**
- Extract, Transform, Load patterns
- Data validation
- Error recovery
- Incremental processing
- Monitoring and logging

## 💡 Tips for Using the GUIs

### Keyboard Shortcuts
- **Ctrl+C**: Copy solution code
- **Alt+Tab**: Switch between GUI windows
- **F11**: Toggle fullscreen (if supported)

### Best Practices
1. **Start with Batch ETL** - It's the most fundamental
2. **Progress through difficulty levels** - Don't skip ahead
3. **Read the problem statements carefully** - They contain hints
4. **Test your code before submitting** - Use the "Run Code" button
5. **Review solutions** - Even if you solve it, compare with the solution
6. **Take notes** - Document patterns you learn

### Common Patterns to Master

#### Batch Processing
```python
# Read, transform, write pattern
df = spark.read.csv("input.csv", header=True)
transformed = df.filter(...).groupBy(...).agg(...)
transformed.write.parquet("output.parquet")
```

#### Streaming
```python
# Stream processing pattern
stream = spark.readStream.format("kafka").load()
processed = stream.select(...).groupBy(...).agg(...)
query = processed.writeStream.start()
```

#### Data Quality
```python
# Validation pattern
from pyspark.sql.functions import col, isnan, isnull

# Check for nulls
null_counts = df.select([
    sum(col(c).isNull().cast("int")).alias(c) 
    for c in df.columns
])
```

## 🔧 Advanced Configuration

### Custom Window Size
Edit the GUI file and modify:
```python
self.setGeometry(100, 100, 1200, 800)  # x, y, width, height
```

### Change Color Scheme
The GUIs use Fusion style with custom stylesheets. You can modify the StyleSheet in each GUI file.

### Enable Debug Mode
Set environment variable:
```bash
export PYSPARK_GUI_DEBUG=1
python3 batch_etl_gui.py
```

## 📊 Progress Tracking

Each GUI maintains progress locally. Your scores and completed challenges are tracked.

### View Progress
- Check the "Progress" tab in each GUI
- Scores are calculated based on:
  - Code correctness (60%)
  - Performance (20%)
  - Code quality (20%)

### Reset Progress
```bash
# Remove progress file (if exists)
rm ~/.pyspark_interview_progress.json
```

## 🎓 Learning Path

Recommended order:
1. **Week 1-2**: Batch ETL Practice (all levels)
2. **Week 3**: Kafka Streaming Practice (levels 1-3)
3. **Week 4**: Kafka to Parquet Practice
4. **Week 5**: ETL Practice (full pipeline)
5. **Week 6**: Review and repeat challenging exercises

## 📖 Additional Resources

- **PySpark Documentation**: https://spark.apache.org/docs/latest/api/python/
- **Kafka Documentation**: https://kafka.apache.org/documentation/
- **PyQt5 Documentation**: https://doc.qt.io/qtforpython/

## ✅ Verification Checklist

Before starting, verify:
- [ ] Python 3.x installed (`python3 --version`)
- [ ] PyQt5 installed (`python3 -c "import PyQt5"`)
- [ ] DISPLAY variable set (`echo $DISPLAY`)
- [ ] GUI test launcher runs (`python3 gui_test_launcher.py`)
- [ ] At least one interview GUI opens successfully

## 🤝 Getting Help

If you encounter issues:

1. **Check this guide** - Most common issues are documented
2. **Run diagnostics** - Use `gui_test_launcher.py`
3. **Check logs** - Look at terminal output for errors
4. **Test simple GUI** - Run `test_pyqt5.py`
5. **Verify system** - Ensure X server/display manager is running

## 🎉 Success Indicators

You'll know everything is working when:
- ✅ `gui_test_launcher.py` opens and shows green checkmarks
- ✅ You can click buttons and launch other GUIs
- ✅ Individual practice GUIs display properly
- ✅ You can type code and see syntax highlighting
- ✅ Run buttons execute code and show results

---

**Last Updated**: December 15, 2024
**Status**: ✅ All GUIs functional and tested
