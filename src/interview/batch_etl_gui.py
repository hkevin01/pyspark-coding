"""
================================================================================
BATCH ETL PRACTICE - PyQt5 GUI
================================================================================
Professional GUI for mastering batch ETL pipelines
CSV → Transform → Parquet
================================================================================
"""

import sys
import os
import time
from datetime import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QLabel, QTextEdit, 
                             QTabWidget, QProgressBar, QMessageBox, QSplitter,
                             QListWidget, QGroupBox, QFrame)
from PyQt5.QtCore import Qt, QTimer, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QTextCursor, QColor, QPalette


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


class BatchETLGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.practice_dir = "/tmp/etl_practice/batch"
        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0
        self.start_time = None
        self.timer = None
        
        self.steps = [
            {
                'title': 'Create SparkSession',
                'description': 'Create SparkSession with AQE enabled',
                'hint': '''from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("BatchETL") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()''',
                'validation': ['SparkSession', 'builder', 'appName']
            },
            {
                'title': 'Read CSV Files',
                'description': 'Read customers.csv and orders.csv',
                'hint': '''df_customers = spark.read.csv("customers.csv", header=True, inferSchema=True)
df_orders = spark.read.csv("orders.csv", header=True, inferSchema=True)''',
                'validation': ['read.csv', 'header=True', 'inferSchema=True']
            },
            {
                'title': 'Clean Data',
                'description': 'Remove duplicates and null values',
                'hint': '''df_customers_clean = df_customers.dropDuplicates().dropna()
df_orders_clean = df_orders.dropDuplicates().dropna()''',
                'validation': ['dropDuplicates', 'dropna']
            },
            {
                'title': 'Join Tables',
                'description': 'Left join on customer_id',
                'hint': '''df_joined = df_orders_clean.join(df_customers_clean, "customer_id", "left")''',
                'validation': ['.join(', 'customer_id']
            },
            {
                'title': 'Aggregate Data',
                'description': 'Calculate total_sales, order_count per customer',
                'hint': '''from pyspark.sql.functions import sum, count

df_summary = df_joined.groupBy("customer_id").agg(
    sum("amount").alias("total_sales"),
    count("order_id").alias("order_count")
)''',
                'validation': ['groupBy', 'agg(', 'sum(', 'count(']
            },
            {
                'title': 'Write to Parquet',
                'description': 'Save to Parquet with overwrite mode',
                'hint': '''df_summary.write.mode("overwrite").parquet("output/customer_summary.parquet")''',
                'validation': ['.write', '.parquet', 'mode(']
            }
        ]
        
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle('🎯 Batch ETL Practice System')
        self.setGeometry(100, 100, 1400, 900)
        
        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Header
        header = self.create_header()
        main_layout.addWidget(header)
        
        # Tab widget for modes
        self.tabs = QTabWidget()
        self.tabs.addTab(self.create_guided_tab(), "📚 Guided Mode")
        self.tabs.addTab(self.create_timed_tab(), "⏱️ Timed Mode")
        self.tabs.addTab(self.create_interview_tab(), "🎤 Interview Mode")
        self.tabs.addTab(self.create_reference_tab(), "📖 Reference")
        main_layout.addWidget(self.tabs)
        
        # Status bar
        self.statusBar().showMessage('Ready to practice!')
        
        # Apply modern styling
        self.apply_styling()
        
    def create_header(self):
        header = QFrame()
        header.setFrameShape(QFrame.StyledPanel)
        header_layout = QVBoxLayout(header)
        
        title = QLabel('📊 BATCH ETL PRACTICE')
        title.setFont(QFont('Arial', 20, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        
        subtitle = QLabel('Master CSV → Transform → Parquet pipelines')
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setStyleSheet('color: #666; font-size: 14px;')
        
        target = QLabel('🎯 Target: Complete pipeline in < 15 minutes')
        target.setAlignment(Qt.AlignCenter)
        target.setStyleSheet('color: #ff6b35; font-weight: bold;')
        
        header_layout.addWidget(title)
        header_layout.addWidget(subtitle)
        header_layout.addWidget(target)
        
        return header
        
    def create_guided_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Progress section
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
        
        # Current step
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
        
        # Code file info
        self.code_label = QLabel()
        self.code_label.setStyleSheet('color: #0066cc; font-weight: bold;')
        layout.addWidget(self.code_label)
        
        # Buttons
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
        <p><b>Goal:</b> Complete full pipeline as fast as possible!</p>
        <p><b>Target:</b> 15 minutes | <b>Expert:</b> 10 minutes</p>
        <br>
        <p><b>Requirements:</b></p>
        <ul>
        <li>Read customers.csv and orders.csv</li>
        <li>Clean data (duplicates, nulls)</li>
        <li>Join tables on customer_id</li>
        <li>Aggregate: total_sales, order_count</li>
        <li>Write to Parquet</li>
        </ul>
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
        <p><b>Scenario:</b> "Build an ETL pipeline that reads customer and order data,
        joins them, calculates summary metrics, and saves to a data warehouse."</p>
        <br>
        <p style="color: red;"><b>⚠️ Rules:</b></p>
        <ul>
        <li>No hints available</li>
        <li>20-minute time limit</li>
        <li>Must run without errors</li>
        </ul>
        ''')
        info.setWordWrap(True)
        layout.addWidget(info)
        
        self.interview_timer_label = QLabel('Time Remaining: 20:00')
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
        
        label = QLabel('<h3>�� Reference Solution</h3><p>Study this production-ready solution:</p>')
        layout.addWidget(label)
        
        self.reference_text = QTextEdit()
        self.reference_text.setReadOnly(True)
        self.reference_text.setFont(QFont('Courier', 10))
        
        solution = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, col, current_timestamp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Create Spark session
    spark = SparkSession.builder \\
        .appName("BatchETL") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .getOrCreate()
    
    logger.info("Starting Batch ETL pipeline...")
    
    # Extract
    df_customers = spark.read.csv("customers.csv", header=True, inferSchema=True)
    df_orders = spark.read.csv("orders.csv", header=True, inferSchema=True)
    
    # Transform - Clean
    df_customers_clean = df_customers.dropDuplicates().dropna()
    df_orders_clean = df_orders.dropDuplicates().dropna()
    
    # Transform - Join
    df_joined = df_orders_clean.join(df_customers_clean, "customer_id", "left")
    
    # Transform - Aggregate
    df_summary = df_joined.groupBy("customer_id", "name").agg(
        sum("amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value")
    ).withColumn("processed_at", current_timestamp())
    
    # Load
    df_summary.write \\
        .mode("overwrite") \\
        .parquet("output/customer_summary.parquet")
    
    logger.info("ETL pipeline completed successfully!")
    spark.stop()

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
        self.code_file = f"{self.practice_dir}/my_etl.py"
        
        with open(self.code_file, 'w') as f:
            f.write('"""Batch ETL Practice"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import sum, count, avg\n\n')
            f.write('# TODO: Write your ETL pipeline\n\n')
        
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
        
        # Update list
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
        code_file = f"{self.practice_dir}/timed_etl.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Timed ETL Challenge"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import *\n\n')
            f.write('# TODO: Complete the pipeline\n\n')
        
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
        
        if minutes >= 15:
            self.timer_label.setStyleSheet('color: #d32f2f; background: #ffebee; padding: 20px; border-radius: 10px;')
        
    def stop_timed_mode(self):
        if self.timer:
            self.timer.stop()
        
        elapsed = int(time.time() - self.start_time)
        minutes = elapsed // 60
        seconds = elapsed % 60
        
        if elapsed < 600:
            msg = f'🏆 EXPERT LEVEL!\n\nTime: {minutes}m {seconds}s\nOutstanding speed!'
        elif elapsed < 900:
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
        code_file = f"{self.practice_dir}/interview_etl.py"
        
        with open(code_file, 'w') as f:
            f.write('# Interview Challenge: Complete ETL Pipeline\n\n')
        
        self.interview_code_label.setText(f'�� Code file: {code_file}')
        self.start_time = time.time()
        self.interview_time_limit = 1200  # 20 minutes
        
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_interview_timer)
        self.timer.start(1000)
        
        self.start_interview_btn.setEnabled(False)
        self.submit_interview_btn.setEnabled(True)
        self.statusBar().showMessage('🎤 Interview started! 20-minute countdown.')
        
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
        
        if remaining < 300:  # Less than 5 minutes
            self.interview_timer_label.setStyleSheet('color: #d32f2f; background: #ffebee; padding: 20px; border-radius: 10px; font-weight: bold;')
        
    def submit_interview(self):
        if self.timer:
            self.timer.stop()
        
        elapsed = int(time.time() - self.start_time)
        minutes = elapsed // 60
        
        if elapsed <= self.interview_time_limit:
            msg = f'🎉 SUBMITTED ON TIME!\n\nTime: {minutes} minutes\n✅ Interview complete!'
        else:
            msg = f'⏰ OVERTIME\n\nTook: {minutes} minutes\n(Limit: 20 minutes)'
            
        QMessageBox.information(self, 'Interview Complete', msg)
        
        self.start_interview_btn.setEnabled(True)
        self.submit_interview_btn.setEnabled(False)
        self.interview_timer_label.setText('Time Remaining: 20:00')
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
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 10px 20px;
                font-size: 14px;
                font-weight: bold;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #45a049;
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
                background-color: #4CAF50;
            }
        ''')


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = BatchETLGUI()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
