#!/usr/bin/env python3
"""
================================================================================
KAFKA-TO-PARQUET DATA LAKE INGESTION - PYQT5 GUI
================================================================================
Professional PyQt5 GUI for practicing Kafka to Data Lake (Parquet) ingestion

FEATURES:
- 4 practice modes (Guided, Timed, Interview, Reference)
- Visual progress tracking with progress bar
- Step-by-step guidance with hints
- Real-time code validation
- Countdown/stopwatch timers
- Color-coded feedback (Purple theme)
- Professional Fusion theme

USAGE:
    python kafka_to_parquet_gui.py

TARGET TIME:
    - Timed Mode: 25 minutes
    - Expert: 18 minutes
    - Interview: 30 minutes

STEPS:
    1. SparkSession with Kafka packages
    2. Read from Kafka topic
    3. Parse JSON with full order schema (9 fields)
    4. Convert timestamp string to timestamp type
    5. Add partition columns (year, month, day, hour)
    6. Filter invalid records
    7. WriteStream to partitioned Parquet with checkpointing

COLOR THEME: Purple (#9C27B0) - Advanced level
================================================================================
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QTextEdit, QProgressBar, QTabWidget,
    QListWidget, QListWidgetItem, QMessageBox, QFileDialog
)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QIcon


class CodeValidator(QThread):
    """Background thread for validating code without blocking UI"""
    validation_complete = pyqtSignal(bool, str)
    
    def __init__(self, file_path, patterns):
        super().__init__()
        self.file_path = file_path
        self.patterns = patterns
    
    def run(self):
        """Run validation in background"""
        try:
            if not os.path.exists(self.file_path):
                self.validation_complete.emit(False, "Code file not found!")
                return
            
            with open(self.file_path, 'r') as f:
                code = f.read()
            
            missing = []
            for pattern in self.patterns:
                if pattern not in code:
                    missing.append(pattern)
            
            if missing:
                msg = f"Missing required code:\n" + "\n".join(f"  • {p}" for p in missing)
                self.validation_complete.emit(False, msg)
            else:
                self.validation_complete.emit(True, "✅ All patterns found! Great work!")
        
        except Exception as e:
            self.validation_complete.emit(False, f"Validation error: {str(e)}")


class KafkaToParquetGUI(QMainWindow):
    """Main GUI for Kafka-to-Parquet practice"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Kafka-to-Parquet Data Lake Ingestion Practice")
        self.setGeometry(100, 100, 1200, 800)
        
        # Practice directory
        self.practice_dir = Path("/tmp/etl_practice/kafka_parquet")
        self.practice_dir.mkdir(parents=True, exist_ok=True)
        self.code_file = self.practice_dir / "kafka_to_parquet_solution.py"
        
        # Practice state
        self.current_step = 0
        self.start_time = None
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_timer)
        self.elapsed_seconds = 0
        self.countdown_seconds = 0
        self.timer_mode = "stopwatch"  # or "countdown"
        
        # Steps for data lake ingestion
        self.steps = [
            {
                "title": "Step 1: SparkSession with Kafka packages",
                "hint": "Create SparkSession with:\n• appName('KafkaToParquetPipeline')\n• config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')\n• getOrCreate()",
                "validation": ["SparkSession", "spark-sql-kafka"]
            },
            {
                "title": "Step 2: Read from Kafka topic",
                "hint": "Use readStream with format('kafka'):\n• option('kafka.bootstrap.servers', 'localhost:9092')\n• option('subscribe', 'orders')\n• option('startingOffsets', 'earliest')\n• load()",
                "validation": ["readStream", "format(\"kafka\")", "subscribe"]
            },
            {
                "title": "Step 3: Parse JSON with full order schema",
                "hint": "Define StructType with 9 fields:\n• order_id (StringType)\n• customer_id (StringType)\n• product_id (StringType)\n• quantity (IntegerType)\n• price (DoubleType)\n• timestamp (StringType)\n• status (StringType)\n• payment_method (StringType)\n• shipping_address (StringType)\n\nThen: from_json(col('value').cast('string'), schema)",
                "validation": ["from_json", "StructType", "order_id", "customer_id"]
            },
            {
                "title": "Step 4: Convert timestamp string to timestamp type",
                "hint": "Use to_timestamp() to convert string to timestamp:\n• to_timestamp(col('data.timestamp'), 'yyyy-MM-dd HH:mm:ss')\n• Alias as 'event_time'",
                "validation": ["to_timestamp", "event_time"]
            },
            {
                "title": "Step 5: Add partition columns (year, month, day, hour)",
                "hint": "Extract partition columns from event_time:\n• year(col('event_time')).alias('year')\n• month(col('event_time')).alias('month')\n• dayofmonth(col('event_time')).alias('day')\n• hour(col('event_time')).alias('hour')",
                "validation": ["year", "month", "day", "hour"]
            },
            {
                "title": "Step 6: Filter invalid records",
                "hint": "Filter out invalid data:\n• quantity > 0\n• price > 0\n• order_id is not null\n• customer_id is not null",
                "validation": ["filter", "isNotNull"]
            },
            {
                "title": "Step 7: WriteStream to partitioned Parquet with checkpointing",
                "hint": "Write to data lake:\n• format('parquet')\n• option('path', '/tmp/data_lake/orders')\n• option('checkpointLocation', '/tmp/checkpoints/kafka_parquet')\n• partitionBy('year', 'month', 'day', 'hour')\n• outputMode('append')\n• start()",
                "validation": ["writeStream", ".parquet", "partitionBy", "checkpointLocation"]
            }
        ]
        
        self.init_ui()
        self.apply_styling()
    
    def init_ui(self):
        """Initialize the user interface"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Header
        header = QLabel("💾 Kafka-to-Parquet Data Lake Ingestion Practice")
        header.setFont(QFont("Arial", 20, QFont.Bold))
        header.setAlignment(Qt.AlignCenter)
        layout.addWidget(header)
        
        subtitle = QLabel("Advanced: Stream from Kafka → Transform → Partitioned Data Lake")
        subtitle.setFont(QFont("Arial", 11))
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setStyleSheet("color: #666; margin-bottom: 10px;")
        layout.addWidget(subtitle)
        
        target_label = QLabel("🎯 Target: 25 min | ⚡ Expert: 18 min | 🔥 Interview: 30 min")
        target_label.setAlignment(Qt.AlignCenter)
        target_label.setStyleSheet("color: #9C27B0; font-weight: bold; margin-bottom: 20px;")
        layout.addWidget(target_label)
        
        # Tab widget
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_guided_tab(), "📚 Guided Mode")
        self.tabs.addTab(self.create_timed_tab(), "⏱️ Timed Challenge")
        self.tabs.addTab(self.create_interview_tab(), "🎯 Interview Simulation")
        self.tabs.addTab(self.create_reference_tab(), "📖 Reference Solution")
        layout.addWidget(self.tabs)
        
        # Status bar
        self.statusBar().showMessage("Ready to practice Kafka-to-Parquet data lake ingestion")
    
    def create_guided_tab(self):
        """Create the guided practice tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Progress section
        progress_label = QLabel("Progress:")
        progress_label.setFont(QFont("Arial", 11, QFont.Bold))
        layout.addWidget(progress_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(len(self.steps))
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)
        
        # Steps list
        steps_label = QLabel("Steps:")
        steps_label.setFont(QFont("Arial", 11, QFont.Bold))
        layout.addWidget(steps_label)
        
        self.steps_list = QListWidget()
        for step in self.steps:
            item = QListWidgetItem(f"⭕ {step['title']}")
            self.steps_list.addItem(item)
        layout.addWidget(self.steps_list)
        
        # Current step info
        self.step_info = QLabel("Click 'Start Guided Mode' to begin")
        self.step_info.setWordWrap(True)
        self.step_info.setStyleSheet("background: #f0f0f0; padding: 10px; border-radius: 5px;")
        layout.addWidget(self.step_info)
        
        # Hint section
        hint_label = QLabel("💡 Hint:")
        hint_label.setFont(QFont("Arial", 10, QFont.Bold))
        layout.addWidget(hint_label)
        
        self.hint_text = QTextEdit()
        self.hint_text.setReadOnly(True)
        self.hint_text.setMaximumHeight(120)
        self.hint_text.setPlaceholderText("Hints will appear here...")
        layout.addWidget(self.hint_text)
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("▶️ Start Guided Mode")
        self.start_btn.clicked.connect(self.start_guided_mode)
        btn_layout.addWidget(self.start_btn)
        
        self.hint_btn = QPushButton("💡 Show Hint")
        self.hint_btn.clicked.connect(self.show_hint)
        self.hint_btn.setEnabled(False)
        btn_layout.addWidget(self.hint_btn)
        
        self.validate_btn = QPushButton("✅ Validate Code")
        self.validate_btn.clicked.connect(self.validate_code)
        self.validate_btn.setEnabled(False)
        btn_layout.addWidget(self.validate_btn)
        
        self.reset_btn = QPushButton("🔄 Reset")
        self.reset_btn.clicked.connect(self.reset_guided_mode)
        btn_layout.addWidget(self.reset_btn)
        
        layout.addLayout(btn_layout)
        
        return tab
    
    def create_timed_tab(self):
        """Create the timed challenge tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        info = QLabel("⏱️ Complete the pipeline as fast as you can!\n\n"
                     "Your time will be tracked. Try to beat:\n"
                     "• 🥉 Good: < 25 minutes\n"
                     "• 🥈 Great: < 22 minutes\n"
                     "• 🥇 Expert: < 18 minutes")
        info.setAlignment(Qt.AlignCenter)
        info.setStyleSheet("background: #f9f9f9; padding: 20px; border-radius: 5px;")
        layout.addWidget(info)
        
        self.timed_display = QLabel("00:00")
        self.timed_display.setFont(QFont("Arial", 48, QFont.Bold))
        self.timed_display.setAlignment(Qt.AlignCenter)
        self.timed_display.setStyleSheet("color: #9C27B0; margin: 30px;")
        layout.addWidget(self.timed_display)
        
        btn_layout = QHBoxLayout()
        
        self.timed_start_btn = QPushButton("▶️ Start Timer")
        self.timed_start_btn.clicked.connect(self.start_timed_mode)
        btn_layout.addWidget(self.timed_start_btn)
        
        self.timed_stop_btn = QPushButton("⏸️ Stop Timer")
        self.timed_stop_btn.clicked.connect(self.stop_timer)
        self.timed_stop_btn.setEnabled(False)
        btn_layout.addWidget(self.timed_stop_btn)
        
        self.timed_validate_btn = QPushButton("✅ Validate & Submit")
        self.timed_validate_btn.clicked.connect(self.validate_timed)
        self.timed_validate_btn.setEnabled(False)
        btn_layout.addWidget(self.timed_validate_btn)
        
        layout.addLayout(btn_layout)
        layout.addStretch()
        
        return tab
    
    def create_interview_tab(self):
        """Create the interview simulation tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        info = QLabel("🎯 Interview Simulation Mode\n\n"
                     "You have 30 minutes to complete the Kafka-to-Parquet pipeline.\n"
                     "Timer will count down. Code will auto-submit when time expires.\n\n"
                     "This simulates a real technical interview!")
        info.setAlignment(Qt.AlignCenter)
        info.setStyleSheet("background: #fff3e0; padding: 20px; border-radius: 5px; border: 2px solid #9C27B0;")
        layout.addWidget(info)
        
        self.interview_display = QLabel("30:00")
        self.interview_display.setFont(QFont("Arial", 48, QFont.Bold))
        self.interview_display.setAlignment(Qt.AlignCenter)
        self.interview_display.setStyleSheet("color: #9C27B0; margin: 30px;")
        layout.addWidget(self.interview_display)
        
        self.interview_status = QLabel("Ready to start")
        self.interview_status.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.interview_status)
        
        btn_layout = QHBoxLayout()
        
        self.interview_start_btn = QPushButton("▶️ Start Interview")
        self.interview_start_btn.clicked.connect(self.start_interview_mode)
        btn_layout.addWidget(self.interview_start_btn)
        
        self.interview_submit_btn = QPushButton("📤 Submit Early")
        self.interview_submit_btn.clicked.connect(self.submit_interview)
        self.interview_submit_btn.setEnabled(False)
        btn_layout.addWidget(self.interview_submit_btn)
        
        layout.addLayout(btn_layout)
        layout.addStretch()
        
        return tab
    
    def create_reference_tab(self):
        """Create the reference solution tab"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        info = QLabel("📖 Complete Reference Solution\n\n"
                     "Study this complete implementation of Kafka-to-Parquet data lake ingestion.")
        info.setStyleSheet("background: #f0f0f0; padding: 15px; border-radius: 5px;")
        layout.addWidget(info)
        
        self.reference_text = QTextEdit()
        self.reference_text.setReadOnly(True)
        self.reference_text.setFont(QFont("Courier New", 10))
        
        reference_solution = '''from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Step 1: Create SparkSession with Kafka packages
spark = SparkSession.builder \\
    .appName("KafkaToParquetPipeline") \\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
    .getOrCreate()

# Step 2: Read from Kafka topic
kafka_df = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "orders") \\
    .option("startingOffsets", "earliest") \\
    .load()

# Step 3: Parse JSON with full order schema (9 fields)
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("timestamp", StringType(), False),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address", StringType(), True)
])

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
)

# Step 4: Convert timestamp string to timestamp type
timestamped_df = parsed_df.select(
    col("data.*"),
    to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_time")
)

# Step 5: Add partition columns (year, month, day, hour)
partitioned_df = timestamped_df \\
    .withColumn("year", year(col("event_time"))) \\
    .withColumn("month", month(col("event_time"))) \\
    .withColumn("day", dayofmonth(col("event_time"))) \\
    .withColumn("hour", hour(col("event_time")))

# Step 6: Filter invalid records
filtered_df = partitioned_df.filter(
    (col("quantity") > 0) &
    (col("price") > 0) &
    col("order_id").isNotNull() &
    col("customer_id").isNotNull()
)

# Step 7: WriteStream to partitioned Parquet with checkpointing
query = filtered_df.writeStream \\
    .format("parquet") \\
    .option("path", "/tmp/data_lake/orders") \\
    .option("checkpointLocation", "/tmp/checkpoints/kafka_parquet") \\
    .partitionBy("year", "month", "day", "hour") \\
    .outputMode("append") \\
    .start()

print("✅ Kafka-to-Parquet pipeline started!")
print("📊 Data will be written to: /tmp/data_lake/orders")
print("📁 Partitioned by: year/month/day/hour")
print("💾 Checkpoint: /tmp/checkpoints/kafka_parquet")

# Keep the query running
query.awaitTermination()
'''
        
        self.reference_text.setPlainText(reference_solution)
        layout.addWidget(self.reference_text)
        
        copy_btn = QPushButton("📋 Copy to Clipboard")
        copy_btn.clicked.connect(self.copy_reference)
        layout.addWidget(copy_btn)
        
        return tab
    
    def start_guided_mode(self):
        """Start the guided practice mode"""
        self.current_step = 0
        self.progress_bar.setValue(0)
        
        # Create code file
        self.code_file.write_text("# Kafka-to-Parquet Data Lake Ingestion\n\n")
        
        self.start_btn.setEnabled(False)
        self.hint_btn.setEnabled(True)
        self.validate_btn.setEnabled(True)
        
        self.load_current_step()
        self.statusBar().showMessage(f"📝 Edit file: {self.code_file}")
        
        QMessageBox.information(
            self,
            "Guided Mode Started",
            f"Code file created at:\n{self.code_file}\n\n"
            "Open it in your editor and start coding!\n"
            "Use hints and validation as you progress."
        )
    
    def load_current_step(self):
        """Load the current step information"""
        if self.current_step >= len(self.steps):
            self.step_info.setText("🎉 All steps complete!")
            return
        
        step = self.steps[self.current_step]
        self.step_info.setText(f"Current: {step['title']}")
        
        # Update steps list
        for i in range(self.steps_list.count()):
            item = self.steps_list.item(i)
            if i < self.current_step:
                item.setText(f"✅ {self.steps[i]['title']}")
            elif i == self.current_step:
                item.setText(f"👉 {self.steps[i]['title']}")
            else:
                item.setText(f"⭕ {self.steps[i]['title']}")
    
    def show_hint(self):
        """Show hint for current step"""
        if self.current_step < len(self.steps):
            step = self.steps[self.current_step]
            self.hint_text.setPlainText(step['hint'])
    
    def validate_code(self):
        """Validate the code for current step"""
        if self.current_step >= len(self.steps):
            QMessageBox.information(self, "Complete", "All steps already complete!")
            return
        
        step = self.steps[self.current_step]
        self.statusBar().showMessage("⏳ Validating code...")
        
        # Run validation in background thread
        self.validator = CodeValidator(str(self.code_file), step['validation'])
        self.validator.validation_complete.connect(self.on_validation_complete)
        self.validator.start()
    
    def on_validation_complete(self, success, message):
        """Handle validation result"""
        if success:
            self.current_step += 1
            self.progress_bar.setValue(self.current_step)
            self.load_current_step()
            
            if self.current_step >= len(self.steps):
                QMessageBox.information(
                    self,
                    "🎉 Congratulations!",
                    "All steps complete! You've mastered Kafka-to-Parquet data lake ingestion!\n\n"
                    "You can now handle production data lake pipelines with:\n"
                    "• Kafka streaming sources\n"
                    "• JSON parsing with complex schemas\n"
                    "• Timestamp conversions\n"
                    "• Partition column generation\n"
                    "• Data quality filtering\n"
                    "• Partitioned Parquet writes\n"
                    "• Checkpoint management"
                )
                self.statusBar().showMessage("✅ All steps complete!")
                self.hint_btn.setEnabled(False)
                self.validate_btn.setEnabled(False)
                self.start_btn.setEnabled(True)
            else:
                self.statusBar().showMessage(f"✅ Step {self.current_step} complete!")
                QMessageBox.information(self, "Step Complete", message)
        else:
            QMessageBox.warning(self, "Validation Failed", message)
            self.statusBar().showMessage("❌ Validation failed")
    
    def reset_guided_mode(self):
        """Reset the guided mode"""
        reply = QMessageBox.question(
            self,
            "Reset?",
            "Reset progress and start over?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            self.current_step = 0
            self.progress_bar.setValue(0)
            self.hint_text.clear()
            self.load_current_step()
            self.start_btn.setEnabled(True)
            self.hint_btn.setEnabled(False)
            self.validate_btn.setEnabled(False)
            self.statusBar().showMessage("Reset complete")
    
    def start_timed_mode(self):
        """Start the timed challenge mode"""
        self.code_file.write_text("# Kafka-to-Parquet - Timed Challenge\n\n")
        
        self.elapsed_seconds = 0
        self.timer_mode = "stopwatch"
        self.timer.start(1000)
        
        self.timed_start_btn.setEnabled(False)
        self.timed_stop_btn.setEnabled(True)
        self.timed_validate_btn.setEnabled(True)
        
        self.statusBar().showMessage(f"⏱️ Timer started! Edit: {self.code_file}")
        
        QMessageBox.information(
            self,
            "Timed Challenge Started",
            f"Code file created at:\n{self.code_file}\n\n"
            "Timer is running! Complete all 7 steps as fast as you can.\n\n"
            "Targets:\n"
            "• 🥉 Good: < 25 minutes\n"
            "• 🥈 Great: < 22 minutes\n"
            "• 🥇 Expert: < 18 minutes"
        )
    
    def stop_timer(self):
        """Stop the timer"""
        self.timer.stop()
        self.timed_stop_btn.setEnabled(False)
        self.statusBar().showMessage("⏸️ Timer paused")
    
    def update_timer(self):
        """Update the timer display"""
        if self.timer_mode == "stopwatch":
            self.elapsed_seconds += 1
            minutes = self.elapsed_seconds // 60
            seconds = self.elapsed_seconds % 60
            self.timed_display.setText(f"{minutes:02d}:{seconds:02d}")
            
            # Color coding for timed mode
            if self.elapsed_seconds < 18 * 60:
                self.timed_display.setStyleSheet("color: #4CAF50; margin: 30px;")  # Green
            elif self.elapsed_seconds < 22 * 60:
                self.timed_display.setStyleSheet("color: #FFC107; margin: 30px;")  # Amber
            elif self.elapsed_seconds < 25 * 60:
                self.timed_display.setStyleSheet("color: #FF9800; margin: 30px;")  # Orange
            else:
                self.timed_display.setStyleSheet("color: #F44336; margin: 30px;")  # Red
        
        elif self.timer_mode == "countdown":
            self.countdown_seconds -= 1
            
            if self.countdown_seconds <= 0:
                self.timer.stop()
                self.interview_submit_btn.setEnabled(False)
                self.interview_display.setText("00:00")
                self.interview_status.setText("⏰ Time's up! Auto-submitting...")
                self.statusBar().showMessage("⏰ Interview time expired!")
                QMessageBox.warning(
                    self,
                    "Time's Up!",
                    "Interview time has expired!\n\nYour code will be auto-submitted."
                )
                self.submit_interview()
                return
            
            minutes = self.countdown_seconds // 60
            seconds = self.countdown_seconds % 60
            self.interview_display.setText(f"{minutes:02d}:{seconds:02d}")
            
            # Color coding for interview mode
            if self.countdown_seconds > 15 * 60:
                self.interview_display.setStyleSheet("color: #4CAF50; margin: 30px;")  # Green
            elif self.countdown_seconds > 5 * 60:
                self.interview_display.setStyleSheet("color: #FFC107; margin: 30px;")  # Amber
            elif self.countdown_seconds > 2 * 60:
                self.interview_display.setStyleSheet("color: #FF9800; margin: 30px;")  # Orange
            else:
                self.interview_display.setStyleSheet("color: #F44336; margin: 30px;")  # Red
    
    def validate_timed(self):
        """Validate code in timed mode"""
        self.timer.stop()
        
        # Validate all steps
        all_patterns = []
        for step in self.steps:
            all_patterns.extend(step['validation'])
        
        self.statusBar().showMessage("⏳ Validating final code...")
        self.validator = CodeValidator(str(self.code_file), all_patterns)
        self.validator.validation_complete.connect(self.on_timed_validation_complete)
        self.validator.start()
    
    def on_timed_validation_complete(self, success, message):
        """Handle timed validation result"""
        minutes = self.elapsed_seconds // 60
        seconds = self.elapsed_seconds % 60
        time_str = f"{minutes:02d}:{seconds:02d}"
        
        if success:
            # Determine rating
            if self.elapsed_seconds < 18 * 60:
                rating = "🥇 EXPERT"
                comment = "Incredible speed and precision!"
            elif self.elapsed_seconds < 22 * 60:
                rating = "🥈 GREAT"
                comment = "Excellent performance!"
            elif self.elapsed_seconds < 25 * 60:
                rating = "🥉 GOOD"
                comment = "Solid work!"
            else:
                rating = "✅ COMPLETE"
                comment = "You completed it!"
            
            QMessageBox.information(
                self,
                "Timed Challenge Complete!",
                f"✅ All validation passed!\n\n"
                f"Time: {time_str}\n"
                f"Rating: {rating}\n\n"
                f"{comment}\n\n"
                f"You've demonstrated mastery of:\n"
                f"• Kafka streaming ingestion\n"
                f"• Complex schema handling\n"
                f"• Partitioned data lake writes\n"
                f"• Production checkpoint patterns"
            )
        else:
            QMessageBox.warning(
                self,
                "Validation Failed",
                f"Time: {time_str}\n\n{message}\n\n"
                "Review the reference solution and try again!"
            )
        
        self.timed_start_btn.setEnabled(True)
        self.timed_stop_btn.setEnabled(False)
        self.timed_validate_btn.setEnabled(False)
        self.statusBar().showMessage(f"⏱️ Final time: {time_str}")
    
    def start_interview_mode(self):
        """Start the interview simulation mode"""
        self.code_file.write_text("# Kafka-to-Parquet - Interview Simulation\n\n")
        
        self.countdown_seconds = 30 * 60  # 30 minutes
        self.timer_mode = "countdown"
        self.timer.start(1000)
        
        self.interview_start_btn.setEnabled(False)
        self.interview_submit_btn.setEnabled(True)
        self.interview_status.setText("Interview in progress...")
        
        self.statusBar().showMessage(f"🎯 Interview started! Edit: {self.code_file}")
        
        QMessageBox.information(
            self,
            "Interview Simulation Started",
            f"Code file created at:\n{self.code_file}\n\n"
            "You have 30 minutes to complete the Kafka-to-Parquet pipeline.\n\n"
            "The timer will count down. Your code will be auto-submitted\n"
            "when time expires.\n\n"
            "Good luck! 🍀"
        )
    
    def submit_interview(self):
        """Submit interview code"""
        self.timer.stop()
        
        # Validate all steps
        all_patterns = []
        for step in self.steps:
            all_patterns.extend(step['validation'])
        
        self.statusBar().showMessage("⏳ Validating interview submission...")
        self.validator = CodeValidator(str(self.code_file), all_patterns)
        self.validator.validation_complete.connect(self.on_interview_validation_complete)
        self.validator.start()
    
    def on_interview_validation_complete(self, success, message):
        """Handle interview validation result"""
        elapsed = (30 * 60) - self.countdown_seconds
        minutes = elapsed // 60
        seconds = elapsed % 60
        time_str = f"{minutes:02d}:{seconds:02d}"
        
        if success:
            QMessageBox.information(
                self,
                "Interview Complete!",
                f"✅ Congratulations! You passed the interview!\n\n"
                f"Time: {time_str} / 30:00\n\n"
                f"You successfully demonstrated:\n"
                f"• Kafka stream processing\n"
                f"• Complex data transformations\n"
                f"• Data lake architecture\n"
                f"• Partitioning strategies\n"
                f"• Production-ready checkpointing\n\n"
                f"Excellent work! 🎉"
            )
            self.interview_status.setText("✅ Interview passed!")
        else:
            QMessageBox.warning(
                self,
                "Interview Failed",
                f"Time: {time_str} / 30:00\n\n{message}\n\n"
                "Don't worry! Review the reference solution and try again.\n"
                "Practice makes perfect!"
            )
            self.interview_status.setText("❌ Interview failed - try again!")
        
        self.interview_start_btn.setEnabled(True)
        self.interview_submit_btn.setEnabled(False)
        self.statusBar().showMessage(f"🎯 Interview completed in {time_str}")
    
    def copy_reference(self):
        """Copy reference solution to clipboard"""
        QApplication.clipboard().setText(self.reference_text.toPlainText())
        QMessageBox.information(self, "Copied", "Reference solution copied to clipboard!")
        self.statusBar().showMessage("📋 Reference copied to clipboard")
    
    def apply_styling(self):
        """Apply custom styling to the application"""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #fafafa;
            }
            QPushButton {
                background-color: #9C27B0;
                color: white;
                border: none;
                padding: 10px 20px;
                font-size: 11pt;
                font-weight: bold;
                border-radius: 5px;
                min-width: 120px;
            }
            QPushButton:hover {
                background-color: #7B1FA2;
            }
            QPushButton:pressed {
                background-color: #6A1B9A;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
            QProgressBar {
                border: 2px solid #9C27B0;
                border-radius: 5px;
                text-align: center;
                font-weight: bold;
                height: 25px;
            }
            QProgressBar::chunk {
                background-color: #9C27B0;
            }
            QTabWidget::pane {
                border: 1px solid #ddd;
                border-radius: 5px;
                background: white;
            }
            QTabBar::tab {
                background: #f0f0f0;
                border: 1px solid #ddd;
                padding: 10px 20px;
                margin-right: 2px;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
            }
            QTabBar::tab:selected {
                background: #9C27B0;
                color: white;
            }
            QListWidget {
                border: 1px solid #ddd;
                border-radius: 5px;
                padding: 5px;
                background: white;
            }
            QTextEdit {
                border: 1px solid #ddd;
                border-radius: 5px;
                padding: 5px;
                background: white;
            }
        """)


def main():
    """Main entry point"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = KafkaToParquetGUI()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
