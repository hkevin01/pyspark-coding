"""
================================================================================
KAFKA STREAMING PRACTICE - PyQt5 GUI
================================================================================
Professional GUI for mastering Kafka streaming pipelines
Kafka → Transform → Kafka
================================================================================
"""

import sys
import os
import time
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QTextEdit, 
                             QTabWidget, QProgressBar, QMessageBox,
                             QListWidget, QGroupBox, QFrame)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt5.QtGui import QFont


class CodeValidator(QThread):
    """Background thread for code validation"""
    validation_complete = pyqtSignal(bool, list)
    
    def __init__(self, code_file, patterns):
        super().__init__()
        self.code_file = code_file
        self.patterns = patterns
    
    def run(self):
        try:
            with open(self.code_file, 'r') as f:
                code = f.read()
            
            missing = [p for p in self.patterns if p not in code]
            self.validation_complete.emit(len(missing) == 0, missing)
        except FileNotFoundError:
            self.validation_complete.emit(False, ["File not found"])


class KafkaStreamingGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.practice_dir = "/tmp/etl_practice/kafka_stream"
        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0
        self.start_time = None
        self.timer = None
        
        self.steps = [
            {
                'title': 'SparkSession + Kafka',
                'description': 'Create SparkSession with Kafka packages',
                'hint': '''from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("KafkaStreaming") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
    .getOrCreate()''',
                'validation': ['SparkSession', 'spark-sql-kafka', 'builder']
            },
            {
                'title': 'Read from Kafka',
                'description': 'Read streaming data from "orders" topic',
                'hint': '''df_kafka = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "orders") \\
    .load()''',
                'validation': ['readStream', 'format("kafka")', 'subscribe']
            },
            {
                'title': 'Parse JSON',
                'description': 'Parse JSON with schema definition',
                'hint': '''from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")''',
                'validation': ['from_json', 'StructType', 'StructField']
            },
            {
                'title': 'Add Watermark',
                'description': 'Handle late data with 10-minute watermark',
                'hint': '''df_watermarked = df_parsed.withWatermark("timestamp", "10 minutes")''',
                'validation': ['withWatermark', 'timestamp']
            },
            {
                'title': 'Window Aggregation',
                'description': '5-minute tumbling window aggregation',
                'hint': '''from pyspark.sql.functions import window, sum as spark_sum

df_windowed = df_watermarked.groupBy(
    window("timestamp", "5 minutes"),
    "customer_id"
).agg(spark_sum("amount").alias("total_amount"))''',
                'validation': ['window(', 'groupBy', 'agg(']
            },
            {
                'title': 'Format for Kafka',
                'description': 'Convert to Kafka key-value format',
                'hint': '''from pyspark.sql.functions import to_json, struct

df_output = df_windowed.select(
    col("customer_id").alias("key"),
    to_json(struct("window", "customer_id", "total_amount")).alias("value")
)''',
                'validation': ['to_json', 'struct(', 'alias("key")', 'alias("value")']
            },
            {
                'title': 'Write to Kafka',
                'description': 'Write to "order_aggregates" topic',
                'hint': '''query = df_output.writeStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("topic", "order_aggregates") \\
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \\
    .outputMode("update") \\
    .start()

query.awaitTermination()''',
                'validation': ['writeStream', 'format("kafka")', 'checkpointLocation', 'awaitTermination']
            }
        ]
        
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle('🌊 Kafka Streaming Practice System')
        self.setGeometry(100, 100, 1400, 900)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        header = self.create_header()
        main_layout.addWidget(header)
        
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_guided_tab(), "📚 Guided Mode")
        self.tabs.addTab(self.create_timed_tab(), "⏱️ Timed Mode")
        self.tabs.addTab(self.create_interview_tab(), "🎤 Interview Mode")
        self.tabs.addTab(self.create_reference_tab(), "📖 Reference")
        main_layout.addWidget(self.tabs)
        
        self.statusBar().showMessage('Ready to practice Kafka streaming!')
        self.apply_styling()
        
    def create_header(self):
        header = QFrame()
        header.setFrameShape(QFrame.StyledPanel)
        header_layout = QVBoxLayout(header)
        
        title = QLabel('🌊 KAFKA STREAMING PRACTICE')
        title.setFont(QFont('Arial', 20, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        
        subtitle = QLabel('Master Kafka → Transform → Kafka real-time pipelines')
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setStyleSheet('color: #666; font-size: 14px;')
        
        target = QLabel('🎯 Target: Complete pipeline in < 20 minutes')
        target.setAlignment(Qt.AlignCenter)
        target.setStyleSheet('color: #ff6b35; font-weight: bold;')
        
        header_layout.addWidget(title)
        header_layout.addWidget(subtitle)
        header_layout.addWidget(target)
        
        return header
        
    def create_guided_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        progress_group = QGroupBox("📊 Progress")
        progress_layout = QVBoxLayout()
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximum(len(self.steps))
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar)
        
        self.steps_list = QListWidget()
        for i, step in enumerate(self.steps):
            self.steps_list.addItem(f"⭕ Step {i+1}: {step['title']}")
        progress_layout.addWidget(self.steps_list)
        
        progress_group.setLayout(progress_layout)
        layout.addWidget(progress_group)
        
        step_group = QGroupBox("📝 Current Step")
        step_layout = QVBoxLayout()
        
        self.step_title = QLabel()
        self.step_title.setFont(QFont('Arial', 14, QFont.Bold))
        step_layout.addWidget(self.step_title)
        
        self.step_description = QLabel()
        self.step_description.setWordWrap(True)
        step_layout.addWidget(self.step_description)
        
        self.hint_text = QTextEdit()
        self.hint_text.setReadOnly(True)
        self.hint_text.setMaximumHeight(150)
        self.hint_text.hide()
        step_layout.addWidget(self.hint_text)
        
        step_group.setLayout(step_layout)
        layout.addWidget(step_group)
        
        self.code_label = QLabel()
        self.code_label.setStyleSheet('color: #0066cc; font-weight: bold;')
        layout.addWidget(self.code_label)
        
        btn_layout = QHBoxLayout()
        
        self.start_btn = QPushButton('🚀 Start Guided Practice')
        self.start_btn.clicked.connect(self.start_guided_mode)
        btn_layout.addWidget(self.start_btn)
        
        self.hint_btn = QPushButton('💡 Show Hint')
        self.hint_btn.clicked.connect(self.show_hint)
        self.hint_btn.setEnabled(False)
        btn_layout.addWidget(self.hint_btn)
        
        self.validate_btn = QPushButton('✅ Validate Code')
        self.validate_btn.clicked.connect(self.validate_code)
        self.validate_btn.setEnabled(False)
        btn_layout.addWidget(self.validate_btn)
        
        self.skip_btn = QPushButton('⏭️ Skip Step')
        self.skip_btn.clicked.connect(self.skip_step)
        self.skip_btn.setEnabled(False)
        btn_layout.addWidget(self.skip_btn)
        
        layout.addLayout(btn_layout)
        
        return tab
        
    def create_timed_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        info = QLabel('''
        <h3>⏱️ Timed Challenge</h3>
        <p><b>Goal:</b> Complete Kafka streaming pipeline!</p>
        <p><b>Target:</b> 20 minutes | <b>Expert:</b> 15 minutes</p>
        <br>
        <p><b>Requirements:</b></p>
        <ul>
        <li>Read from "orders" Kafka topic</li>
        <li>Parse JSON with schema</li>
        <li>Add watermark (10 minutes)</li>
        <li>Window aggregation (5 minutes)</li>
        <li>Write to "order_aggregates" topic</li>
        </ul>
        <br>
        <p style="color: #ff6b35;"><b>⚠️ Prerequisite:</b> Kafka must be running!</p>
        ''')
        info.setWordWrap(True)
        layout.addWidget(info)
        
        self.timer_label = QLabel('Timer: 00:00')
        self.timer_label.setFont(QFont('Arial', 24, QFont.Bold))
        self.timer_label.setAlignment(Qt.AlignCenter)
        self.timer_label.setStyleSheet('color: #ff6b35; background: #f0f0f0; padding: 20px; border-radius: 10px;')
        layout.addWidget(self.timer_label)
        
        self.timed_code_label = QLabel()
        layout.addWidget(self.timed_code_label)
        
        btn_layout = QHBoxLayout()
        
        self.start_timer_btn = QPushButton('▶️ Start Timer')
        self.start_timer_btn.clicked.connect(self.start_timed_mode)
        btn_layout.addWidget(self.start_timer_btn)
        
        self.stop_timer_btn = QPushButton('⏹️ Stop & Submit')
        self.stop_timer_btn.clicked.connect(self.stop_timed_mode)
        self.stop_timer_btn.setEnabled(False)
        btn_layout.addWidget(self.stop_timer_btn)
        
        layout.addLayout(btn_layout)
        layout.addStretch()
        
        return tab
        
    def create_interview_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        info = QLabel('''
        <h3>🎤 Interview Simulation</h3>
        <p><b>Scenario:</b> "Build a real-time Kafka streaming pipeline that reads order data,
        performs windowed aggregations with watermarking for late data, and writes results
        back to Kafka for downstream consumers."</p>
        <br>
        <p style="color: red;"><b>⚠️ Rules:</b></p>
        <ul>
        <li>No hints available</li>
        <li>25-minute time limit</li>
        <li>Must handle late data properly</li>
        <li>Kafka must be running</li>
        </ul>
        ''')
        info.setWordWrap(True)
        layout.addWidget(info)
        
        self.interview_timer_label = QLabel('Time Remaining: 25:00')
        self.interview_timer_label.setFont(QFont('Arial', 24, QFont.Bold))
        self.interview_timer_label.setAlignment(Qt.AlignCenter)
        self.interview_timer_label.setStyleSheet('color: #d32f2f; background: #ffebee; padding: 20px; border-radius: 10px;')
        layout.addWidget(self.interview_timer_label)
        
        self.interview_code_label = QLabel()
        layout.addWidget(self.interview_code_label)
        
        btn_layout = QHBoxLayout()
        
        self.start_interview_btn = QPushButton('🎬 Start Interview')
        self.start_interview_btn.clicked.connect(self.start_interview_mode)
        btn_layout.addWidget(self.start_interview_btn)
        
        self.submit_interview_btn = QPushButton('📤 Submit Solution')
        self.submit_interview_btn.clicked.connect(self.submit_interview)
        self.submit_interview_btn.setEnabled(False)
        btn_layout.addWidget(self.submit_interview_btn)
        
        layout.addLayout(btn_layout)
        layout.addStretch()
        
        return tab
        
    def create_reference_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        label = QLabel('<h3>📖 Reference Solution</h3><p>Study this Kafka streaming solution:</p>')
        layout.addWidget(label)
        
        self.reference_text = QTextEdit()
        self.reference_text.setReadOnly(True)
        self.reference_text.setFont(QFont('Courier', 10))
        
        solution = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    # Create Spark session with Kafka
    spark = SparkSession.builder \\
        .appName("KafkaStreaming") \\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
        .getOrCreate()
    
    # Read from Kafka
    df_kafka = spark.readStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("subscribe", "orders") \\
        .option("startingOffsets", "latest") \\
        .load()
    
    # Define schema
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    # Parse JSON
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add watermark
    df_watermarked = df_parsed.withWatermark("timestamp", "10 minutes")
    
    # Window aggregation
    df_windowed = df_watermarked.groupBy(
        window("timestamp", "5 minutes"),
        "customer_id"
    ).agg(
        sum("amount").alias("total_amount"),
        count("order_id").alias("order_count")
    )
    
    # Format for Kafka
    df_output = df_windowed.select(
        col("customer_id").alias("key"),
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "customer_id",
            "total_amount",
            "order_count"
        )).alias("value")
    )
    
    # Write to Kafka
    query = df_output.writeStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("topic", "order_aggregates") \\
        .option("checkpointLocation", "/tmp/kafka_checkpoint") \\
        .outputMode("update") \\
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
'''
        self.reference_text.setPlainText(solution)
        layout.addWidget(self.reference_text)
        
        copy_btn = QPushButton('📋 Copy to Clipboard')
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(solution))
        layout.addWidget(copy_btn)
        
        return tab
        
    def start_guided_mode(self):
        os.makedirs(self.practice_dir, exist_ok=True)
        self.code_file = f"{self.practice_dir}/my_streaming.py"
        
        with open(self.code_file, 'w') as f:
            f.write('"""Kafka Streaming Practice"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import *\n')
            f.write('from pyspark.sql.types import *\n\n')
            f.write('# TODO: Write your Kafka streaming pipeline\n\n')
        
        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0
        
        self.code_label.setText(f'📝 Code file: {self.code_file}')
        self.start_btn.setEnabled(False)
        self.hint_btn.setEnabled(True)
        self.validate_btn.setEnabled(True)
        self.skip_btn.setEnabled(True)
        
        self.load_current_step()
        self.statusBar().showMessage('Guided mode started! Code the first step.')
        
    def load_current_step(self):
        if self.current_step >= len(self.steps):
            self.complete_guided_mode()
            return
            
        step = self.steps[self.current_step]
        self.step_title.setText(f"Step {self.current_step + 1}/{len(self.steps)}: {step['title']}")
        self.step_description.setText(f"📋 {step['description']}")
        self.hint_text.hide()
        
        self.progress_bar.setValue(len(self.steps_completed))
        
        for i in range(self.steps_list.count()):
            item = self.steps_list.item(i)
            if i in self.steps_completed:
                item.setText(f"✅ Step {i+1}: {self.steps[i]['title']}")
            elif i == self.current_step:
                item.setText(f"👉 Step {i+1}: {self.steps[i]['title']}")
            else:
                item.setText(f"⭕ Step {i+1}: {self.steps[i]['title']}")
        
    def show_hint(self):
        step = self.steps[self.current_step]
        self.hint_text.setPlainText(step['hint'])
        self.hint_text.show()
        self.hints_used += 1
        self.statusBar().showMessage(f'Hint shown! (Total hints used: {self.hints_used})')
        
    def validate_code(self):
        step = self.steps[self.current_step]
        validator = CodeValidator(self.code_file, step['validation'])
        validator.validation_complete.connect(self.on_validation_complete)
        validator.start()
        self.statusBar().showMessage('Validating code...')
        
    def on_validation_complete(self, success, missing):
        if success:
            QMessageBox.information(self, 'Success', f'✅ Step {self.current_step + 1} complete!')
            self.steps_completed.append(self.current_step)
            self.current_step += 1
            self.load_current_step()
        else:
            QMessageBox.warning(self, 'Validation Failed', 
                              f'❌ Missing patterns:\n' + '\n'.join(f'• {m}' for m in missing))
            
    def skip_step(self):
        reply = QMessageBox.question(self, 'Skip Step', 
                                    'Skip this step?',
                                    QMessageBox.Yes | QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.current_step += 1
            self.load_current_step()
            
    def complete_guided_mode(self):
        QMessageBox.information(self, 'Complete!', 
                               f'🎉 Guided Mode Complete!\n\n'
                               f'✅ Steps: {len(self.steps_completed)}/{len(self.steps)}\n'
                               f'💡 Hints: {self.hints_used}\n\n'
                               f'📝 Code: {self.code_file}')
        self.start_btn.setEnabled(True)
        self.hint_btn.setEnabled(False)
        self.validate_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)
        
    def start_timed_mode(self):
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/timed_streaming.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Timed Kafka Streaming Challenge"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import *\n')
            f.write('from pyspark.sql.types import *\n\n')
            f.write('# TODO: Complete the Kafka streaming pipeline\n\n')
        
        self.timed_code_label.setText(f'📝 Code file: {code_file}')
        self.start_time = time.time()
        
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_timer)
        self.timer.start(1000)
        
        self.start_timer_btn.setEnabled(False)
        self.stop_timer_btn.setEnabled(True)
        self.statusBar().showMessage('Timer started! Code your pipeline.')
        
    def update_timer(self):
        elapsed = int(time.time() - self.start_time)
        minutes = elapsed // 60
        seconds = elapsed % 60
        self.timer_label.setText(f'Timer: {minutes:02d}:{seconds:02d}')
        
        if minutes >= 20:
            self.timer_label.setStyleSheet('color: #d32f2f; background: #ffebee; padding: 20px; border-radius: 10px;')
        
    def stop_timed_mode(self):
        if self.timer:
            self.timer.stop()
        
        elapsed = int(time.time() - self.start_time)
        minutes = elapsed // 60
        seconds = elapsed % 60
        
        if elapsed < 900:
            msg = f'🏆 EXPERT LEVEL!\n\nTime: {minutes}m {seconds}s\nOutstanding speed!'
        elif elapsed < 1200:
            msg = f'✅ TARGET HIT!\n\nTime: {minutes}m {seconds}s\nGreat job!'
        else:
            msg = f'💪 Good Effort!\n\nTime: {minutes}m {seconds}s\nKeep practicing!'
            
        QMessageBox.information(self, 'Time\'s Up!', msg)
        
        self.start_timer_btn.setEnabled(True)
        self.stop_timer_btn.setEnabled(False)
        self.timer_label.setText('Timer: 00:00')
        self.timer_label.setStyleSheet('color: #ff6b35; background: #f0f0f0; padding: 20px; border-radius: 10px;')
        
    def start_interview_mode(self):
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/interview_streaming.py"
        
        with open(code_file, 'w') as f:
            f.write('# Interview Challenge: Kafka Streaming Pipeline\n\n')
        
        self.interview_code_label.setText(f'📝 Code file: {code_file}')
        self.start_time = time.time()
        self.interview_time_limit = 1500  # 25 minutes
        
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_interview_timer)
        self.timer.start(1000)
        
        self.start_interview_btn.setEnabled(False)
        self.submit_interview_btn.setEnabled(True)
        self.statusBar().showMessage('🎤 Interview started! 25-minute countdown.')
        
    def update_interview_timer(self):
        elapsed = int(time.time() - self.start_time)
        remaining = self.interview_time_limit - elapsed
        
        if remaining <= 0:
            self.timer.stop()
            QMessageBox.warning(self, 'Time Exceeded', '⏰ Time limit exceeded!')
            self.submit_interview()
            return
            
        minutes = remaining // 60
        seconds = remaining % 60
        self.interview_timer_label.setText(f'Time Remaining: {minutes:02d}:{seconds:02d}')
        
        if remaining < 300:
            self.interview_timer_label.setStyleSheet('color: #d32f2f; background: #ffebee; padding: 20px; border-radius: 10px; font-weight: bold;')
        
    def submit_interview(self):
        if self.timer:
            self.timer.stop()
        
        elapsed = int(time.time() - self.start_time)
        minutes = elapsed // 60
        
        if elapsed <= self.interview_time_limit:
            msg = f'🎉 SUBMITTED ON TIME!\n\nTime: {minutes} minutes\n✅ Interview complete!'
        else:
            msg = f'⏰ OVERTIME\n\nTook: {minutes} minutes\n(Limit: 25 minutes)'
            
        QMessageBox.information(self, 'Interview Complete', msg)
        
        self.start_interview_btn.setEnabled(True)
        self.submit_interview_btn.setEnabled(False)
        self.interview_timer_label.setText('Time Remaining: 25:00')
        self.interview_timer_label.setStyleSheet('color: #d32f2f; background: #ffebee; padding: 20px; border-radius: 10px;')
        
    def apply_styling(self):
        self.setStyleSheet('''
            QMainWindow {
                background-color: #f5f5f5;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QPushButton {
                background-color: #FFC107;
                color: white;
                border: none;
                padding: 10px 20px;
                font-size: 14px;
                font-weight: bold;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #FFB300;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
            QTextEdit {
                border: 1px solid #cccccc;
                border-radius: 5px;
                padding: 5px;
                background-color: #fafafa;
            }
            QListWidget {
                border: 1px solid #cccccc;
                border-radius: 5px;
            }
            QProgressBar {
                border: 2px solid #cccccc;
                border-radius: 5px;
                text-align: center;
                height: 25px;
            }
            QProgressBar::chunk {
                background-color: #FFC107;
            }
        ''')


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = KafkaStreamingGUI()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
