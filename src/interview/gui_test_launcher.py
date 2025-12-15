#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
GUI TEST LAUNCHER - Verify PyQt5 and Launch Interview GUIs
================================================================================

📖 PURPOSE:
Test PyQt5 installation and provide a simple launcher for all interview GUIs.

🚀 RUN:
python gui_test_launcher.py
================================================================================
"""

import sys
import os
import subprocess
from pathlib import Path

try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QPushButton, QLabel, QTextEdit, QGroupBox, QMessageBox
    )
    from PyQt5.QtCore import Qt, QTimer
    from PyQt5.QtGui import QFont, QColor, QPalette
    PYQT5_AVAILABLE = True
except ImportError as e:
    PYQT5_AVAILABLE = False
    IMPORT_ERROR = str(e)


class GUITestLauncher(QMainWindow):
    """Main window for testing and launching interview GUIs."""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PySpark Interview Practice - GUI Launcher")
        self.setGeometry(100, 100, 800, 600)
        
        # Set color scheme
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 10px;
                font-size: 14px;
                border-radius: 5px;
                min-width: 200px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3d8b40;
            }
            QPushButton:disabled {
                background-color: #cccccc;
                color: #666666;
            }
            QLabel {
                font-size: 14px;
                color: #333333;
            }
            QGroupBox {
                font-size: 16px;
                font-weight: bold;
                border: 2px solid #4CAF50;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                color: #4CAF50;
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
            }
        """)
        
        self.init_ui()
        
        # Test PyQt5 after a short delay
        QTimer.singleShot(500, self.run_tests)
    
    def init_ui(self):
        """Initialize the user interface."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        
        # Title
        title = QLabel("🎯 PySpark Interview Practice Suite")
        title.setStyleSheet("font-size: 24px; font-weight: bold; color: #2196F3; padding: 10px;")
        title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title)
        
        # Status label
        self.status_label = QLabel("✅ PyQt5 is working! GUI system operational.")
        self.status_label.setStyleSheet("font-size: 16px; color: green; padding: 10px;")
        self.status_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.status_label)
        
        # System info
        info_text = f"""
        <b>System Information:</b><br>
        • Python: {sys.version.split()[0]}<br>
        • PyQt5: Installed and working<br>
        • Display: {os.environ.get('DISPLAY', 'Not set')}<br>
        • Working Directory: {os.getcwd()}
        """
        info_label = QLabel(info_text)
        info_label.setStyleSheet("background-color: white; padding: 10px; border-radius: 5px;")
        main_layout.addWidget(info_label)
        
        # Launch buttons group
        launch_group = QGroupBox("Launch Interview Practice GUIs")
        launch_layout = QVBoxLayout()
        launch_group.setLayout(launch_layout)
        main_layout.addWidget(launch_group)
        
        # Batch ETL GUI button
        batch_btn = QPushButton("🗄️  Batch ETL Practice")
        batch_btn.clicked.connect(lambda: self.launch_gui("batch_etl_gui.py"))
        launch_layout.addWidget(batch_btn)
        
        # Kafka Streaming GUI button
        kafka_btn = QPushButton("🌊 Kafka Streaming Practice")
        kafka_btn.clicked.connect(lambda: self.launch_gui("kafka_streaming_gui.py"))
        launch_layout.addWidget(kafka_btn)
        
        # Kafka to Parquet GUI button
        parquet_btn = QPushButton("📦 Kafka to Parquet Practice")
        parquet_btn.clicked.connect(lambda: self.launch_gui("kafka_to_parquet_gui.py"))
        launch_layout.addWidget(parquet_btn)
        
        # ETL Practice GUI button
        etl_btn = QPushButton("🔄 ETL Practice")
        etl_btn.clicked.connect(lambda: self.launch_gui("etl_practice_gui.py"))
        launch_layout.addWidget(etl_btn)
        
        # Test output area
        test_group = QGroupBox("Test Results")
        test_layout = QVBoxLayout()
        test_group.setLayout(test_layout)
        main_layout.addWidget(test_group)
        
        self.test_output = QTextEdit()
        self.test_output.setReadOnly(True)
        self.test_output.setMaximumHeight(150)
        self.test_output.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: monospace;")
        test_layout.addWidget(self.test_output)
        
        # Close button
        close_btn = QPushButton("Close Launcher")
        close_btn.setStyleSheet("background-color: #f44336;")
        close_btn.clicked.connect(self.close)
        main_layout.addWidget(close_btn)
    
    def log(self, message):
        """Add message to test output."""
        self.test_output.append(message)
    
    def run_tests(self):
        """Run diagnostic tests."""
        self.log("=" * 60)
        self.log("Running PyQt5 Diagnostic Tests...")
        self.log("=" * 60)
        
        # Test 1: Window visibility
        self.log(f"✓ Window is visible: {self.isVisible()}")
        self.log(f"✓ Window size: {self.width()}x{self.height()}")
        self.log(f"✓ Window position: ({self.x()}, {self.y()})")
        
        # Test 2: Display environment
        display = os.environ.get('DISPLAY', 'NOT SET')
        self.log(f"✓ DISPLAY variable: {display}")
        
        # Test 3: Check for GUI files
        interview_dir = Path(__file__).parent
        gui_files = [
            "batch_etl_gui.py",
            "kafka_streaming_gui.py",
            "kafka_to_parquet_gui.py",
            "etl_practice_gui.py"
        ]
        
        self.log("\nChecking GUI files...")
        for gui_file in gui_files:
            file_path = interview_dir / gui_file
            exists = file_path.exists()
            status = "✓" if exists else "✗"
            self.log(f"{status} {gui_file}: {'Found' if exists else 'Missing'}")
        
        self.log("\n" + "=" * 60)
        self.log("All tests completed successfully!")
        self.log("=" * 60)
        self.log("\n💡 Click any button above to launch a practice GUI")
    
    def launch_gui(self, script_name):
        """Launch a GUI script in a new process."""
        script_path = Path(__file__).parent / script_name
        
        if not script_path.exists():
            QMessageBox.warning(
                self,
                "File Not Found",
                f"GUI script not found:\n{script_path}\n\nPlease make sure all GUI files are in the interview directory."
            )
            self.log(f"✗ Cannot launch {script_name}: File not found")
            return
        
        try:
            self.log(f"\n🚀 Launching {script_name}...")
            
            # Launch in background
            subprocess.Popen(
                [sys.executable, str(script_path)],
                cwd=str(script_path.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            self.log(f"✓ {script_name} launched successfully")
            self.status_label.setText(f"✅ Launched: {script_name}")
            
            QMessageBox.information(
                self,
                "GUI Launched",
                f"{script_name} has been launched in a new window.\n\nCheck your taskbar or window list."
            )
            
        except Exception as e:
            error_msg = f"Error launching {script_name}: {e}"
            self.log(f"✗ {error_msg}")
            QMessageBox.critical(self, "Launch Error", error_msg)


def main():
    """Main entry point."""
    print("=" * 80)
    print("PySpark Interview Practice - GUI Test Launcher")
    print("=" * 80)
    
    if not PYQT5_AVAILABLE:
        print(f"\n❌ ERROR: PyQt5 is not available!")
        print(f"   Import error: {IMPORT_ERROR}")
        print(f"\n📦 To install PyQt5:")
        print(f"   pip install PyQt5")
        print()
        return 1
    
    print("✅ PyQt5 is installed")
    print(f"✅ DISPLAY: {os.environ.get('DISPLAY', 'NOT SET')}")
    print(f"✅ Python: {sys.version.split()[0]}")
    print()
    print("🚀 Launching GUI Test Launcher...")
    print()
    
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    # Set application-wide font
    font = QFont("Arial", 10)
    app.setFont(font)
    
    window = GUITestLauncher()
    window.show()
    
    print(f"✓ Window created and shown")
    print(f"✓ Window is visible: {window.isVisible()}")
    print(f"✓ Window geometry: {window.geometry()}")
    print()
    print("If you see this but no window appears, there may be a window manager issue.")
    print("Try: alt+tab or check your taskbar/window list.")
    print()
    
    return app.exec_()


if __name__ == '__main__':
    sys.exit(main())
