"""
================================================================================
PYSPARK ETL PRACTICE LAUNCHER
================================================================================
Master three essential ETL patterns through focused, repetitive practice!
================================================================================
"""

import os
import sys
import subprocess


class PracticeLauncher:
    """Main launcher for all practice systems."""
    
    def __init__(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Color codes
        self.GREEN = '\033[92m'
        self.YELLOW = '\033[93m'
        self.RED = '\033[91m'
        self.BLUE = '\033[94m'
        self.CYAN = '\033[96m'
        self.MAGENTA = '\033[95m'
        self.BOLD = '\033[1m'
        self.RESET = '\033[0m'
        
    def clear_screen(self):
        os.system('clear' if os.name != 'nt' else 'cls')
        
    def print_header(self):
        self.clear_screen()
        print(f"\n{self.CYAN}{self.BOLD}{'=' * 80}")
        print(f"{'🚀 PYSPARK ETL PRACTICE SYSTEM ��'.center(80)}")
        print(f"{'=' * 80}{self.RESET}\n")
        
    def show_menu(self):
        self.print_header()
        
        print(f"{self.BOLD}Choose Your Practice Path:{self.RESET}\n")
        
        print(f"{self.GREEN}1. 📊 BATCH ETL PRACTICE{self.RESET}")
        print(f"   {self.CYAN}Focus: CSV → Transform → Parquet{self.RESET}")
        print("   • Best for beginners")
        print("   • 6-step guided mode")
        print("   • Master traditional batch processing")
        print("   • Target: 15 minutes\n")
        
        print(f"{self.YELLOW}2. 🌊 KAFKA STREAMING PRACTICE{self.RESET}")
        print(f"   {self.CYAN}Focus: Kafka → Transform → Kafka{self.RESET}")
        print("   • Intermediate level")
        print("   • 7-step guided mode")
        print("   • Learn real-time processing")
        print("   • Watermarking & windowing")
        print("   • Target: 20 minutes\n")
        
        print(f"{self.MAGENTA}3. 💾 KAFKA-TO-PARQUET PRACTICE{self.RESET}")
        print(f"   {self.CYAN}Focus: Kafka → Transform → Data Lake{self.RESET}")
        print("   • Advanced level")
        print("   • 7-step guided mode")
        print("   • Streaming data lake ingestion")
        print("   • Partitioned Parquet writes")
        print("   • Target: 25 minutes\n")
        
        print(f"{self.BLUE}4. 📖 VIEW DOCUMENTATION{self.RESET}")
        print("   • Kafka setup guide")
        print("   • Architecture overview")
        print("   • Troubleshooting tips\n")
        
        print(f"{self.RED}5. 🚪 EXIT{self.RESET}\n")
        
        print(f"{self.BOLD}💡 Learning Path:{self.RESET}")
        print("   Week 1: Batch ETL (fundamentals)")
        print("   Week 2: Kafka Streaming (real-time)")
        print("   Week 3: Kafka-to-Parquet (integration)")
        print("   Goal: Interview-ready on all patterns!\n")
        
    def launch_practice(self, script_name):
        """Launch a practice script."""
        script_path = os.path.join(self.script_dir, script_name)
        
        if not os.path.exists(script_path):
            print(f"{self.RED}❌ Error: {script_name} not found!{self.RESET}")
            input("Press Enter...")
            return
            
        try:
            subprocess.run([sys.executable, script_path])
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"{self.RED}❌ Error launching practice: {e}{self.RESET}")
            input("Press Enter...")
            
    def show_docs(self):
        """Show documentation menu."""
        self.clear_screen()
        print(f"\n{self.BLUE}{self.BOLD}{'=' * 80}")
        print(f"{'📖 DOCUMENTATION'.center(80)}")
        print(f"{'=' * 80}{self.RESET}\n")
        
        docs_dir = self.script_dir
        readme_path = os.path.join(docs_dir, "KAFKA_ETL_README.md")
        
        if os.path.exists(readme_path):
            print(f"{self.GREEN}✅ Kafka ETL Guide:{self.RESET} {readme_path}\n")
            print("Quick Start:")
            print("  1. cd src/interview")
            print("  2. docker-compose up -d")
            print("  3. python kafka_producer_orders.py")
            print("  4. python kafka_to_parquet_example.py\n")
        else:
            print(f"{self.YELLOW}⚠️  Documentation not found{self.RESET}\n")
            
        print(f"{self.CYAN}Key Files:{self.RESET}")
        print("  • docker-compose.yml - Kafka infrastructure")
        print("  • kafka_producer_orders.py - Test data generator")
        print("  • kafka_to_parquet_example.py - Production pipeline")
        print("  • KAFKA_ETL_README.md - Complete guide\n")
        
        print(f"{self.BOLD}Practice Files:{self.RESET}")
        print("  • batch_etl_practice.py")
        print("  • kafka_streaming_practice.py")
        print("  • kafka_to_parquet_practice.py\n")
        
        input("Press Enter to continue...")
        
    def get_choice(self):
        """Get user choice."""
        while True:
            try:
                choice = input(f"{self.YELLOW}Choose [1-5]: {self.RESET}").strip()
                if choice in ['1', '2', '3', '4', '5']:
                    return choice
                print(f"{self.RED}❌ Invalid choice. Try again.{self.RESET}")
            except (EOFError, KeyboardInterrupt):
                print(f"\n{self.YELLOW}👋 Goodbye!{self.RESET}\n")
                sys.exit(0)
                
    def run(self):
        """Main loop."""
        while True:
            self.show_menu()
            choice = self.get_choice()
            
            if choice == '1':
                self.launch_practice("batch_etl_practice.py")
            elif choice == '2':
                self.launch_practice("kafka_streaming_practice.py")
            elif choice == '3':
                self.launch_practice("kafka_to_parquet_practice.py")
            elif choice == '4':
                self.show_docs()
            elif choice == '5':
                self.clear_screen()
                print(f"\n{self.GREEN}{self.BOLD}🎉 Happy Learning!{self.RESET}")
                print(f"{self.CYAN}Master all three patterns to become interview-ready!{self.RESET}\n")
                break


if __name__ == "__main__":
    try:
        launcher = PracticeLauncher()
        launcher.run()
    except KeyboardInterrupt:
        print(f"\n\n{'\033[93m'}👋 Goodbye!{'\033[0m'}\n")
        sys.exit(0)
