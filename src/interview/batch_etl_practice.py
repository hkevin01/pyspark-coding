"""
================================================================================
BATCH ETL PRACTICE - INTERACTIVE LEARNING SYSTEM
================================================================================

Master batch ETL pipelines through repetitive practice!
Focus: CSV → Transform → Parquet

GOAL: Code a complete batch ETL pipeline in < 15 minutes from memory!
================================================================================
"""

import os
import sys
import time


class BatchETLPractice:
    """Interactive practice system for batch ETL."""
    
    def __init__(self):
        self.practice_dir = "/tmp/etl_practice/batch"
        self.current_step = 0
        self.start_time = None
        self.steps_completed = []
        self.hints_used = 0
        
        # Color codes
        self.GREEN = '\033[92m'
        self.YELLOW = '\033[93m'
        self.RED = '\033[91m'
        self.BLUE = '\033[94m'
        self.CYAN = '\033[96m'
        self.BOLD = '\033[1m'
        self.RESET = '\033[0m'
        
    def clear_screen(self):
        os.system('clear' if os.name != 'nt' else 'cls')
        
    def print_header(self, text, color=None):
        color_code = color if color else self.CYAN
        print(f"\n{color_code}{self.BOLD}{'=' * 80}")
        print(f"{text.center(80)}")
        print(f"{'=' * 80}{self.RESET}\n")
        
    def print_section(self, text):
        print(f"\n{self.BLUE}{'-' * 80}{self.RESET}")
        print(f"{self.BOLD}{text}{self.RESET}")
        print(f"{self.BLUE}{'-' * 80}{self.RESET}\n")
        
    def show_welcome(self):
        self.clear_screen()
        self.print_header("🎯 BATCH ETL PRACTICE SYSTEM 🎯", self.GREEN)
        
        print(f"{self.BOLD}Master Batch ETL Pipelines!{self.RESET}\n")
        print("📋 What you'll build:")
        print("   • Read CSV files (customers, orders)")
        print("   • Clean data (duplicates, nulls)")
        print("   • Join tables")
        print("   • Aggregate metrics")
        print("   • Write to Parquet\n")
        
        print("📚 Practice Modes:")
        print("   1. GUIDED MODE    - Step-by-step with hints")
        print("   2. TIMED MODE     - 15-minute speed challenge")
        print("   3. INTERVIEW MODE - 20-minute no-hints simulation")
        print("   4. REFERENCE      - View complete solution")
        print("   5. EXIT\n")
        
    def show_step_menu(self, steps):
        print(f"\n{self.CYAN}📊 Progress:{self.RESET}")
        for i, step in enumerate(steps):
            status = "✅" if i in self.steps_completed else "⭕"
            current = "👉" if i == self.current_step else "  "
            print(f"  {current} {status} Step {i+1}: {step['title']}")
        print()
        
    def get_user_input(self, prompt, valid_options=None):
        while True:
            try:
                user_input = input(f"{self.YELLOW}{prompt}{self.RESET}").strip()
                if valid_options and user_input not in valid_options:
                    print(f"{self.RED}❌ Invalid choice. Try again.{self.RESET}")
                    continue
                return user_input
            except (EOFError, KeyboardInterrupt):
                print(f"\n{self.YELLOW}👋 Practice session ended.{self.RESET}")
                sys.exit(0)
                
    def show_hint(self, hint):
        print(f"\n{self.GREEN}{'='*80}{self.RESET}")
        print(f"{self.YELLOW}💡 HINT:{self.RESET}\n")
        print(f"{self.CYAN}{hint}{self.RESET}")
        print(f"{self.GREEN}{'='*80}{self.RESET}\n")
        self.hints_used += 1
        
    def validate_code(self, code_file, expected_patterns):
        try:
            with open(code_file, 'r') as f:
                code = f.read()
            
            missing = []
            for pattern in expected_patterns:
                if pattern not in code:
                    missing.append(pattern)
            
            return len(missing) == 0, missing
        except FileNotFoundError:
            return False, ["File not found"]
            
    def run_guided_mode(self):
        self.clear_screen()
        self.print_header("📚 GUIDED MODE - Learn Step by Step", self.GREEN)
        
        steps = [
            {
                'title': 'Setup: Create SparkSession',
                'description': 'Create SparkSession with app name "BatchETL"',
                'hint': '''from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("BatchETL") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()''',
                'validation': ['SparkSession', 'builder', 'appName']
            },
            {
                'title': 'Extract: Read CSV files',
                'description': 'Read customers.csv and orders.csv with headers and inferred schema',
                'hint': '''df_customers = spark.read.csv(
    "/tmp/etl_practice_data/customers.csv",
    header=True,
    inferSchema=True
)

df_orders = spark.read.csv(
    "/tmp/etl_practice_data/orders.csv",
    header=True,
    inferSchema=True
)''',
                'validation': ['read.csv', 'header=True', 'inferSchema=True']
            },
            {
                'title': 'Transform: Clean data',
                'description': 'Remove duplicates based on IDs and drop rows with null values',
                'hint': '''df_customers_clean = df_customers \\
    .dropDuplicates(["customer_id"]) \\
    .dropna(subset=["customer_id", "name"])

df_orders_clean = df_orders \\
    .dropDuplicates(["order_id"]) \\
    .dropna(subset=["order_id", "customer_id", "amount"])''',
                'validation': ['dropDuplicates', 'dropna', 'subset']
            },
            {
                'title': 'Transform: Join tables',
                'description': 'Perform left join between orders and customers on customer_id',
                'hint': '''df_joined = df_orders_clean.join(
    df_customers_clean,
    "customer_id",
    "left"
)''',
                'validation': ['.join(', 'customer_id', 'left']
            },
            {
                'title': 'Transform: Aggregate metrics',
                'description': 'Calculate total_sales, order_count, and avg_order_value per customer',
                'hint': '''from pyspark.sql.functions import sum as spark_sum, count, avg

df_summary = df_joined.groupBy("customer_id", "name").agg(
    spark_sum("amount").alias("total_sales"),
    count("order_id").alias("order_count"),
    avg("amount").alias("avg_order_value")
)''',
                'validation': ['groupBy', 'agg(', 'sum', 'count', 'avg']
            },
            {
                'title': 'Load: Write to Parquet',
                'description': 'Save results to Parquet with overwrite mode',
                'hint': '''df_summary.write \\
    .mode("overwrite") \\
    .parquet("/tmp/etl_output/customer_summary")

print("ETL completed successfully!")
spark.stop()''',
                'validation': ['.write', '.parquet', 'mode']
            }
        ]
        
        print("You'll code a batch ETL pipeline step by step.\n")
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/my_batch_etl.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Batch ETL Pipeline Practice"""\n\n')
            f.write('# TODO: Write your batch ETL pipeline here\n\n')
        
        print(f"📝 Your code file: {code_file}\n")
        print(f"✅ Template created! Open in your editor and start coding.\n")
        
        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0
        
        for i, step in enumerate(steps):
            self.current_step = i
            self.show_step_menu(steps)
            
            self.print_section(f"Step {i+1}/6: {step['title']}")
            print(f"📋 Task: {step['description']}\n")
            
            while True:
                choice = self.get_user_input(
                    "Options: [d]one | [h]int | [s]kip | [q]uit: ",
                    ['d', 'h', 's', 'q']
                )
                
                if choice == 'q':
                    return
                elif choice == 's':
                    print(f"{self.YELLOW}⏭️  Skipped step {i+1}{self.RESET}")
                    break
                elif choice == 'h':
                    self.show_hint(step['hint'])
                elif choice == 'd':
                    is_valid, missing = self.validate_code(code_file, step['validation'])
                    if is_valid:
                        print(f"{self.GREEN}✅ Step {i+1} complete!{self.RESET}")
                        self.steps_completed.append(i)
                        time.sleep(1)
                        break
                    else:
                        print(f"{self.RED}❌ Validation failed!{self.RESET}")
                        print(f"Missing: {missing}")
                        print(f"{self.YELLOW}💡 Type 'h' for hint{self.RESET}\n")
        
        self.clear_screen()
        self.print_header("🎉 GUIDED MODE COMPLETE!", self.GREEN)
        print(f"✅ Steps completed: {len(self.steps_completed)}/{len(steps)}")
        print(f"💡 Hints used: {self.hints_used}")
        print(f"\n📝 Your code: {code_file}")
        print(f"\n➡️  Next: Try TIMED MODE to build speed!\n")
        input("Press Enter to continue...")
        
    def run_timed_mode(self):
        self.clear_screen()
        self.print_header("⏱️  TIMED MODE - Speed Challenge", self.YELLOW)
        
        print("🎯 Complete the full batch ETL pipeline!\n")
        print("📋 Requirements:")
        print("   1. Read customers.csv and orders.csv")
        print("   2. Clean: remove duplicates and nulls")
        print("   3. Join on customer_id")
        print("   4. Aggregate: total_sales, order_count, avg_order_value")
        print("   5. Write to Parquet\n")
        
        print(f"{self.YELLOW}⏱️  Target: 15 minutes{self.RESET}")
        print(f"{self.GREEN}🏆 Expert: 10 minutes{self.RESET}\n")
        
        input("Press Enter to start timer...")
        
        self.start_time = time.time()
        print(f"\n{self.GREEN}⏱️  Timer started!{self.RESET}\n")
        
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/timed_batch_etl.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Timed Batch ETL Challenge"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import sum, count, avg\n\n')
            f.write('# TODO: Complete the ETL pipeline\n\n')
        
        print(f"📝 Code file: {code_file}")
        print(f"\n⏱️  Coding... Type 'done' when complete:\n")
        
        self.get_user_input("Type 'done': ", ['done'])
        
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        self.clear_screen()
        self.print_header("⏱️  TIME'S UP!", self.CYAN)
        
        print(f"⏱️  Your time: {minutes}m {seconds}s\n")
        
        if elapsed < 600:
            print(f"{self.GREEN}🏆 EXPERT! Outstanding!{self.RESET}")
        elif elapsed < 900:
            print(f"{self.GREEN}✅ TARGET HIT! Great job!{self.RESET}")
        else:
            print(f"{self.YELLOW}💪 Keep practicing!{self.RESET}")
        
        print(f"\n📝 Your code: {code_file}\n")
        input("Press Enter to continue...")
        
    def run_interview_mode(self):
        self.clear_screen()
        self.print_header("🎤 INTERVIEW MODE", self.RED)
        
        print(f"{self.BOLD}Real Interview Scenario!{self.RESET}\n")
        print("📋 Interviewer says:")
        print('   "Build a batch ETL pipeline that reads customer and order')
        print('    data from CSV, cleans it, joins them, calculates summary')
        print('    metrics, and writes to Parquet. Add error handling."\n')
        
        print(f"{self.RED}⚠️  Rules:{self.RESET}")
        print("   • No hints")
        print("   • 20-minute limit")
        print("   • Must handle errors\n")
        
        choice = self.get_user_input("Ready? [y/n]: ", ['y', 'n'])
        if choice == 'n':
            return
            
        self.start_time = time.time()
        print(f"\n{self.RED}🎤 Interview started!{self.RESET}\n")
        
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/interview_batch_etl.py"
        
        with open(code_file, 'w') as f:
            f.write('# Interview Challenge: Batch ETL Pipeline\n\n')
        
        print(f"📝 Code file: {code_file}")
        print(f"⏱️  Timer: 20 minutes\n")
        
        self.get_user_input("Type 'submit': ", ['submit'])
        
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        
        self.clear_screen()
        if elapsed <= 1200:
            self.print_header("🎉 SUBMITTED IN TIME!", self.GREEN)
            print(f"✅ Time: {minutes} minutes")
        else:
            self.print_header("⏰ TIME EXCEEDED", self.RED)
            print(f"⚠️  Took: {minutes} minutes")
        
        print(f"\n📝 Solution: {code_file}\n")
        input("Press Enter...")
        
    def show_reference(self):
        self.clear_screen()
        self.print_header("📖 REFERENCE SOLUTION", self.BLUE)
        
        print("Complete production-ready batch ETL:\n")
        
        solution = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count, avg, current_timestamp
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Create SparkSession
    spark = SparkSession.builder \\
        .appName("BatchETL") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .getOrCreate()
    
    try:
        logger.info("Starting batch ETL...")
        
        # Extract
        df_customers = spark.read.csv(
            "/tmp/etl_practice_data/customers.csv",
            header=True,
            inferSchema=True
        )
        df_orders = spark.read.csv(
            "/tmp/etl_practice_data/orders.csv",
            header=True,
            inferSchema=True
        )
        
        # Transform: Clean
        df_customers_clean = df_customers \\
            .dropDuplicates(["customer_id"]) \\
            .dropna(subset=["customer_id", "name"])
        
        df_orders_clean = df_orders \\
            .dropDuplicates(["order_id"]) \\
            .dropna(subset=["order_id", "customer_id", "amount"])
        
        # Transform: Join
        df_joined = df_orders_clean.join(
            df_customers_clean,
            "customer_id",
            "left"
        )
        
        # Transform: Aggregate
        df_summary = df_joined.groupBy("customer_id", "name").agg(
            spark_sum("amount").alias("total_sales"),
            count("order_id").alias("order_count"),
            avg("amount").alias("avg_order_value")
        ).withColumn("processed_at", current_timestamp())
        
        # Load
        df_summary.write \\
            .mode("overwrite") \\
            .parquet("/tmp/etl_output/customer_summary")
        
        logger.info("ETL completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
'''
        
        print(solution)
        print("\n" + "=" * 80)
        print(f"\n{self.YELLOW}💡 Study this, then code from memory!{self.RESET}\n")
        input("Press Enter...")
        
    def run(self):
        while True:
            self.show_welcome()
            
            choice = self.get_user_input("Choose [1-5]: ", ['1', '2', '3', '4', '5'])
            
            if choice == '1':
                self.run_guided_mode()
            elif choice == '2':
                self.run_timed_mode()
            elif choice == '3':
                self.run_interview_mode()
            elif choice == '4':
                self.show_reference()
            elif choice == '5':
                self.clear_screen()
                print(f"\n{self.GREEN}🎉 Keep practicing!{self.RESET}\n")
                break


if __name__ == "__main__":
    try:
        practice = BatchETLPractice()
        practice.run()
    except KeyboardInterrupt:
        print(f"\n\n{'\033[93m'}👋 Session ended!{'\033[0m'}\n")
        sys.exit(0)
