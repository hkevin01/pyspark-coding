# 🖥️ PyQt5 GUI Practice Systems

Professional graphical user interfaces for PySpark ETL practice!

## 📦 Available GUIs

### 1. 📊 Batch ETL GUI
**File**: `batch_etl_gui.py`

**Features**:
- **4 Tabs**: Guided, Timed, Interview, Reference
- **Progress Tracking**: Visual progress bar and step list
- **Code Validation**: Real-time validation with pattern matching
- **Hint System**: Show/hide hints for each step
- **Timers**: Built-in countdown and stopwatch
- **Modern UI**: Professional styling with Fusion theme

**Run**:
```bash
python batch_etl_gui.py
```

---

### 2. 🌊 Kafka Streaming GUI (Coming Soon)
**File**: `kafka_streaming_gui.py`

Features will include:
- Real-time Kafka integration
- Watermarking visualization
- Window aggregation tracking
- Stream monitoring

---

### 3. 💾 Kafka-to-Parquet GUI (Coming Soon)
**File**: `kafka_to_parquet_gui.py`

Features will include:
- Data lake ingestion workflow
- Partition visualization
- Checkpoint management
- Output verification

---

## 🎯 GUI Features

### Guided Mode Tab
- **Step-by-step progression** with visual indicators (⭕ → 👉 → ✅)
- **Hint system** - Click to reveal code examples
- **Code validation** - Checks if required patterns exist
- **Skip option** - Move to next step if stuck
- **Progress bar** - Visual completion tracking

### Timed Mode Tab
- **Countdown timer** - Real-time elapsed time
- **Target times** - 15 min target, 10 min expert
- **Color indicators** - Red when exceeding target
- **Performance feedback** - Expert/Target/Keep Practicing

### Interview Mode Tab
- **Countdown timer** - 20-minute limit
- **No hints** - Realistic interview simulation
- **Auto-submit** - Automatically submits when time expires
- **Pressure indication** - Timer turns red at 5 minutes

### Reference Tab
- **Complete solution** - Production-ready code
- **Copy button** - One-click copy to clipboard
- **Syntax highlighting** - Easy-to-read monospace font
- **Study mode** - Code then practice from memory

---

## 🚀 Quick Start

### Launch Batch ETL GUI
```bash
cd src/interview
python batch_etl_gui.py
```

### Requirements
- Python 3.6+
- PyQt5 (already installed)
- PySpark

---

## 💡 Usage Tips

1. **Start with Guided Mode**
   - Use hints liberally to learn
   - Validate after each step
   - Build muscle memory

2. **Progress to Timed Mode**
   - Try to beat 15 minutes
   - No hints allowed
   - Focus on speed

3. **Master Interview Mode**
   - 20-minute limit
   - No assistance
   - Interview-ready!

4. **Use Reference for Study**
   - Copy complete solution
   - Practice coding from memory
   - Compare your approach

---

## 🎨 UI Components

### Color Scheme
- **Green** (#4CAF50): Success, start buttons
- **Orange** (#ff6b35): Warnings, targets
- **Red** (#d32f2f): Errors, time pressure
- **Blue** (#0066cc): Information, links
- **Gray** (#f5f5f5): Background

### Icons
- 🚀 Start
- 💡 Hint
- ✅ Validate/Complete
- ⏭️ Skip
- ⏱️ Timer
- 🎤 Interview
- 📖 Reference

---

## 🔧 Technical Details

### Code Validation
The GUI uses background threads to validate code:
- Checks for required code patterns
- Non-blocking validation
- Real-time feedback

### File Management
Practice files are created in:
```
/tmp/etl_practice/batch/
├── my_etl.py           # Guided mode
├── timed_etl.py        # Timed mode
└── interview_etl.py    # Interview mode
```

### Timer System
- Uses QTimer for 1-second updates
- Separate timers for each mode
- Automatic cleanup on mode switch

---

## 🆚 GUI vs Terminal

**PyQt5 GUI Advantages**:
- ✅ Visual progress tracking
- ✅ Better UX with tabs
- ✅ Modern, professional appearance
- ✅ Easy button controls
- ✅ Color-coded feedback
- ✅ Built-in copy/paste

**Terminal Advantages**:
- ✅ Lightweight
- ✅ Works over SSH
- ✅ No GUI dependencies
- ✅ Faster startup

**Choose based on your preference!** Both offer the same practice functionality.

---

## 🐛 Troubleshooting

### GUI doesn't start
```bash
# Check PyQt5 installation
python3 -c "import PyQt5; print('PyQt5 OK')"

# Reinstall if needed
sudo apt install python3-pyqt5
```

### Display issues
```bash
# Ensure X server is running
echo $DISPLAY

# For WSL, install X server like VcXsrv
```

### Performance
- Close other GUIs before starting
- Use single-monitor setup for best experience

---

## 📝 Development Status

- [x] Batch ETL GUI - **COMPLETE**
- [ ] Kafka Streaming GUI - In Progress
- [ ] Kafka-to-Parquet GUI - Planned
- [ ] Master Launcher GUI - Planned

---

## 🎓 Learning Path

**Week 1-2**: Batch ETL GUI
- Master all 4 modes
- Achieve < 15 min consistently

**Week 3-4**: Kafka Streaming GUI
- Learn real-time patterns
- Target < 20 min

**Week 5-6**: Data Lake GUI
- Integrate all concepts
- Target < 25 min

**Result**: Interview-ready with visual confidence! 🎉

---

Happy practicing with the new GUIs! 🖥️✨
