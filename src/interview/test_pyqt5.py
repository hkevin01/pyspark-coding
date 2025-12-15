#!/usr/bin/env python3
"""Simple PyQt5 GUI test"""
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QLabel, QPushButton, QVBoxLayout, QWidget
from PyQt5.QtCore import Qt

class TestWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PyQt5 Test Window")
        self.setGeometry(100, 100, 400, 300)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout()
        central_widget.setLayout(layout)
        
        label = QLabel("✅ PyQt5 is working!")
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet("font-size: 24px; color: green; padding: 20px;")
        layout.addWidget(label)
        
        button = QPushButton("Click Me to Test")
        button.clicked.connect(self.on_click)
        layout.addWidget(button)
        
        self.status = QLabel("Status: Ready")
        self.status.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status)
    
    def on_click(self):
        self.status.setText("Status: Button clicked! ✓")
        print("Button clicked successfully!")

if __name__ == '__main__':
    print("Starting PyQt5 test...")
    print("If you see this but no window, there's a display issue.")
    
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    window = TestWindow()
    window.show()
    
    print(f"Window visible: {window.isVisible()}")
    print(f"Window size: {window.size()}")
    print("Window should now be displayed...")
    
    sys.exit(app.exec_())
