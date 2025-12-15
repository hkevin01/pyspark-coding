"""
================================================================================
INTERACTIVE ETL INTERVIEW PRACTICE SYSTEM
================================================================================

PURPOSE: Master coding a complete ETL pipeline through interactive practice.
Learn by doing - code the same pipeline over and over until it's muscle memory!

FEATURES:
- Step-by-step guided coding
- Real-time feedback
- Progressive difficulty levels
- Time tracking
- Solution hints
- Validates your code

GOAL: Code a production ETL pipeline in under 15 minutes from memory!

TIME: Practice until perfect! | DIFFICULTY: Interview Ready 🎯
================================================================================
"""

import os
import subprocess
import sys
import time
from datetime import datetime


class ETLPracticeGUI:
    """Interactive terminal-based practice system."""

    def __init__(self):
        self.practice_dir = "/tmp/etl_practice"
        self.current_step = 0
        self.start_time = None
        self.steps_completed = []
        self.hints_used = 0

        # Color codes
        self.GREEN = "\033[92m"
        self.YELLOW = "\033[93m"
        self.RED = "\033[91m"
        self.BLUE = "\033[94m"
        self.CYAN = "\033[96m"
        self.BOLD = "\033[1m"
        self.RESET = "\033[0m"

    def clear_screen(self):
        """Clear terminal screen."""
        os.system("clear" if os.name != "nt" else "cls")

    def print_header(self, text, color=None):
        """Print styled header."""
        color_code = color if color else self.CYAN
        print(f"\n{color_code}{self.BOLD}{'=' * 80}")
        print(f"{text.center(80)}")
        print(f"{'=' * 80}{self.RESET}\n")

    def print_section(self, text):
        """Print section divider."""
        print(f"\n{self.BLUE}{'-' * 80}{self.RESET}")
        print(f"{self.BOLD}{text}{self.RESET}")
        print(f"{self.BLUE}{'-' * 80}{self.RESET}\n")

    def show_welcome(self):
        """Display welcome screen."""
        self.clear_screen()
        self.print_header("🎯 ETL INTERVIEW PRACTICE SYSTEM 🎯", self.GREEN)

        print(f"{self.BOLD}Welcome to Interactive ETL Mastery!{self.RESET}\n")
        print("📋 What you'll build:")
        print("   • Extract data from CSV files")
        print("   • Transform: clean, join, aggregate")
        print("   • Load results to Parquet warehouse")
        print("   • Add data quality checks")
        print("   • Implement error handling\n")

        print(
            f"{self.YELLOW}🎯 Interview Goal: Code this in < 15 minutes from memory!{self.RESET}\n"
        )

        print("📚 Practice Modes:")
        print(f"\n{self.CYAN}━━━ BATCH ETL PIPELINE ━━━{self.RESET}")
        print("   1. GUIDED     - Step-by-step with hints (learning)")
        print("   2. TIMED      - Full pipeline with timer (practice)")
        print("   3. INTERVIEW  - No hints, just like real interview!")
        print("   4. REFERENCE  - View complete batch ETL solution")
        print(f"\n{self.YELLOW}━━━ STREAMING ETL (KAFKA) ━━━{self.RESET}")
        print("   5. KAFKA ETL  - 🔥 Real-time Kafka pipeline (with hints)")
        print(f"\n{self.RED}━━━━━━━━━━━━━━━━━━━━━━━━━━━{self.RESET}")
        print("   6. EXIT\n")

    def show_step_menu(self, steps):
        """Display current step progress."""
        print(f"\n{self.CYAN}📊 Progress:{self.RESET}")
        for i, step in enumerate(steps):
            status = "✅" if i in self.steps_completed else "⭕"
            current = "👉" if i == self.current_step else "  "
            print(f"  {current} {status} Step {i+1}: {step['title']}")
        print()

    def get_user_input(self, prompt, valid_options=None):
        """Get validated user input."""
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
        """Display a hint."""
        print(f"\n{self.YELLOW}💡 HINT:{self.RESET}")
        print(f"   {hint}\n")
        self.hints_used += 1

    def validate_code(self, code_file, expected_patterns):
        """Validate user's code contains expected patterns."""
        try:
            with open(code_file, "r") as f:
                code = f.read()

            missing = []
            for pattern in expected_patterns:
                if pattern not in code:
                    missing.append(pattern)

            return len(missing) == 0, missing
        except FileNotFoundError:
            return False, ["File not found"]

    def run_guided_mode(self):
        """Guided practice with step-by-step instructions."""
        self.clear_screen()
        self.print_header("📚 GUIDED MODE - Learn Step by Step", self.GREEN)

        steps = [
            {
                "title": "Setup: Create SparkSession",
                "description": 'Create a SparkSession with app name "ETL_Practice"',
                "hint": 'Use SparkSession.builder.appName("ETL_Practice").getOrCreate()',
                "validation": ["SparkSession", "builder", "appName"],
            },
            {
                "title": "Extract: Read CSV files",
                "description": "Read customers.csv and orders.csv into DataFrames",
                "hint": "Use spark.read.csv(path, header=True, inferSchema=True)",
                "validation": ["read.csv", "header=True", "inferSchema=True"],
            },
            {
                "title": "Transform: Clean data",
                "description": "Remove duplicates and null values",
                "hint": "Use .dropDuplicates() and .dropna()",
                "validation": ["dropDuplicates", "dropna"],
            },
            {
                "title": "Transform: Join tables",
                "description": "Join customers and orders on customer_id",
                "hint": 'Use .join(df2, "customer_id", "left")',
                "validation": [".join(", "customer_id"],
            },
            {
                "title": "Transform: Aggregate",
                "description": "Calculate total_sales and order_count per customer",
                "hint": 'Use .groupBy("customer_id").agg(sum("amount"), count("order_id"))',
                "validation": ["groupBy", "agg(", "sum(", "count("],
            },
            {
                "title": "Load: Write to Parquet",
                "description": "Save results to output/customer_summary.parquet",
                "hint": 'Use .write.mode("overwrite").parquet(path)',
                "validation": [".write", ".parquet", "mode("],
            },
        ]

        print("You'll code an ETL pipeline step by step.\n")
        print(f"📝 Your code will be saved to: {self.practice_dir}/my_etl.py\n")

        # Create practice directory
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/my_etl.py"

        # Initialize code file with template
        with open(code_file, "w") as f:
            f.write("from pyspark.sql import SparkSession\n")
            f.write("from pyspark.sql.functions import sum, count, avg\n\n")
            f.write("# TODO: Write your ETL pipeline here\n\n")

        print(f"✅ Template created! Open: {code_file}\n")

        self.current_step = 0
        for i, step in enumerate(steps):
            self.current_step = i
            self.show_step_menu(steps)

            self.print_section(f"Step {i+1}: {step['title']}")
            print(f"📋 Task: {step['description']}\n")

            while True:
                choice = self.get_user_input(
                    "Options: [d]one with step | [h]int | [s]kip | [q]uit: ",
                    ["d", "h", "s", "q"],
                )

                if choice == "q":
                    return
                elif choice == "s":
                    print(f"{self.YELLOW}⏭️  Skipped step {i+1}{self.RESET}")
                    break
                elif choice == "h":
                    self.show_hint(step["hint"])
                elif choice == "d":
                    # Validate code
                    is_valid, missing = self.validate_code(
                        code_file, step["validation"]
                    )
                    if is_valid:
                        print(f"{self.GREEN}✅ Step {i+1} complete!{self.RESET}")
                        self.steps_completed.append(i)
                        time.sleep(1)
                        break
                    else:
                        print(f"{self.RED}❌ Code validation failed!{self.RESET}")
                        print(f"Missing patterns: {missing}")
                        print(
                            f"{self.YELLOW}💡 Try again or ask for a [h]int{self.RESET}\n"
                        )

        # Final summary
        self.clear_screen()
        self.print_header("�� GUIDED MODE COMPLETE!", self.GREEN)
        print(f"✅ Steps completed: {len(self.steps_completed)}/{len(steps)}")
        print(f"💡 Hints used: {self.hints_used}")
        print(f"\n📝 Your code: {code_file}")
        print(f"\n➡️  Next: Try TIMED mode to practice speed!\n")
        input("Press Enter to continue...")

    def run_timed_mode(self):
        """Timed practice - full pipeline with timer."""
        self.clear_screen()
        self.print_header("⏱️  TIMED MODE - Speed Practice", self.YELLOW)

        print("🎯 Goal: Complete the full ETL pipeline as fast as possible!\n")
        print("📋 Requirements:")
        print("   1. Read customers.csv and orders.csv")
        print("   2. Clean data (remove duplicates, nulls)")
        print("   3. Join tables on customer_id")
        print("   4. Aggregate: total_sales, order_count per customer")
        print("   5. Write to Parquet")
        print("   6. Add at least one data quality check\n")

        print(f"{self.YELLOW}⏱️  Target time: 15 minutes{self.RESET}")
        print(f"{self.GREEN}🏆 Expert time: 10 minutes{self.RESET}\n")

        input("Press Enter when ready to start timer...")

        self.start_time = time.time()
        print(f"\n{self.GREEN}⏱️  Timer started!{self.RESET}\n")

        # Create practice file
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/timed_etl.py"

        with open(code_file, "w") as f:
            f.write(
                '''"""
Timed ETL Practice
Complete the ETL pipeline below!
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, col

def main():
    # TODO: Your code here
    pass

if __name__ == "__main__":
    main()
'''
            )

        print(f"📝 Code file: {code_file}")
        print(f"\n⏱️  Timer running... Type 'done' when finished:\n")

        self.get_user_input("Type 'done' when complete: ", ["done"])

        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)

        self.clear_screen()
        self.print_header("⏱️  TIME'S UP!", self.CYAN)

        print(f"⏱️  Your time: {minutes}m {seconds}s\n")

        if elapsed < 600:  # 10 minutes
            print(f"{self.GREEN}🏆 EXPERT LEVEL! Outstanding speed!{self.RESET}")
        elif elapsed < 900:  # 15 minutes
            print(f"{self.GREEN}✅ GREAT JOB! You hit the target!{self.RESET}")
        else:
            print(
                f"{self.YELLOW}💪 Good effort! Keep practicing to improve speed.{self.RESET}"
            )

        print(f"\n📝 Your code: {code_file}\n")
        input("Press Enter to continue...")

    def run_interview_mode(self):
        """Interview simulation - no hints, pressure on!"""
        self.clear_screen()
        self.print_header("🎤 INTERVIEW MODE - The Real Deal", self.RED)

        print(f"{self.BOLD}Simulate a real interview scenario!{self.RESET}\n")
        print("📋 Interviewer's request:")
        print('   "Build an ETL pipeline that reads customer and order data,')
        print("    joins them, calculates summary metrics, and saves to a")
        print('    data warehouse. Add error handling and data quality checks."\n')

        print(f"{self.RED}⚠️  Rules:{self.RESET}")
        print("   • No hints available")
        print("   • 20-minute time limit")
        print("   • Must run without errors")
        print("   • Code will be validated\n")

        choice = self.get_user_input("Ready for interview? [y/n]: ", ["y", "n"])
        if choice == "n":
            return

        self.start_time = time.time()
        print(f"\n{self.RED}🎤 Interview started! Timer: 20 minutes{self.RESET}\n")

        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/interview_etl.py"

        with open(code_file, "w") as f:
            f.write("# Interview Challenge: Complete ETL Pipeline\n\n")

        print(f"📝 Code file: {code_file}")
        print(f"⏱️  Timer running...\n")

        self.get_user_input("Type 'submit' when ready: ", ["submit"])

        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)

        self.clear_screen()
        if elapsed <= 1200:  # 20 minutes
            self.print_header("🎉 INTERVIEW COMPLETE!", self.GREEN)
            print(f"✅ Submitted in time: {minutes} minutes")
        else:
            self.print_header("⏰ TIME EXCEEDED", self.RED)
            print(f"⚠️  Took {minutes} minutes (limit: 20)")

        print(f"\n📝 Your solution: {code_file}\n")
        input("Press Enter to continue...")

    def show_reference(self):
        """Show complete solution."""
        self.clear_screen()
        self.print_header("📖 REFERENCE SOLUTION", self.BLUE)

        print("Complete, production-ready ETL pipeline:\n")

        solution = '''
"""
Production ETL Pipeline - Reference Solution
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, count, avg, col, current_timestamp
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create and configure SparkSession."""
    return SparkSession.builder \\
        .appName("ETL_Pipeline") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.shuffle.partitions", "200") \\
        .getOrCreate()


def extract_data(spark, input_dir):
    """Extract: Read raw data from CSV files."""
    logger.info("Starting data extraction...")
    
    try:
        df_customers = spark.read.csv(
            f"{input_dir}/customers.csv",
            header=True,
            inferSchema=True
        )
        
        df_orders = spark.read.csv(
            f"{input_dir}/orders.csv",
            header=True,
            inferSchema=True
        )
        
        logger.info(f"Extracted {df_customers.count()} customers")
        logger.info(f"Extracted {df_orders.count()} orders")
        
        return df_customers, df_orders
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise


def transform_data(df_customers, df_orders):
    """Transform: Clean, join, and aggregate data."""
    logger.info("Starting data transformation...")
    
    # Data quality: Remove duplicates and nulls
    df_customers_clean = df_customers \\
        .dropDuplicates(["customer_id"]) \\
        .dropna(subset=["customer_id", "name"])
    
    df_orders_clean = df_orders \\
        .dropDuplicates(["order_id"]) \\
        .dropna(subset=["order_id", "customer_id", "amount"])
    
    # Join tables
    df_joined = df_orders_clean.join(
        df_customers_clean,
        "customer_id",
        "left"
    )
    
    # Aggregate: Calculate customer metrics
    df_summary = df_joined.groupBy("customer_id", "name").agg(
        spark_sum("amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value")
    )
    
    # Add metadata
    df_summary = df_summary.withColumn(
        "processed_at",
        current_timestamp()
    )
    
    # Data quality check
    total_records = df_summary.count()
    null_records = df_summary.filter(col("total_sales").isNull()).count()
    
    logger.info(f"Transformed {total_records} customer records")
    logger.info(f"Data quality: {null_records} null records")
    
    if null_records > total_records * 0.1:  # >10% nulls
        logger.warning("High null rate detected!")
    
    return df_summary


def load_data(df, output_dir):
    """Load: Write to data warehouse."""
    logger.info("Starting data load...")
    
    try:
        df.write \\
            .mode("overwrite") \\
            .partitionBy("processed_at") \\
            .parquet(f"{output_dir}/customer_summary")
        
        logger.info(f"Data loaded to {output_dir}")
        
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise


def main():
    """Run complete ETL pipeline."""
    logger.info("="*60)
    logger.info("ETL PIPELINE STARTED")
    logger.info("="*60)
    
    spark = create_spark_session()
    
    try:
        # ETL Steps
        df_customers, df_orders = extract_data(spark, "/input")
        df_summary = transform_data(df_customers, df_orders)
        load_data(df_summary, "/output")
        
        logger.info("="*60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
'''

        print(solution)
        print("\n" + "=" * 80)
        print(
            f"\n{self.YELLOW}💡 Study this solution, then practice coding it from memory!{self.RESET}\n"
        )
        input("Press Enter to continue...")

    def run_kafka_etl_mode(self):
        """Kafka ETL practice - streaming with PySpark."""
        self.clear_screen()
        self.print_header("🔥 KAFKA ETL PIPELINE - Streaming Practice", self.CYAN)

        print(
            f"{self.BOLD}Build a Real-Time Kafka Streaming ETL Pipeline!{self.RESET}\n"
        )
        print("📋 Interview Scenario:")
        print('   "We need to process real-time order data from Kafka.')
        print("    Read from 'orders' topic, transform the data, ")
        print("    calculate running totals, and write to another Kafka topic.")
        print('    Handle late data and implement windowing."\n')

        print(f"{self.YELLOW}⚡ This is ADVANCED - but you got this!{self.RESET}\n")

        steps = [
            {
                "title": "Setup: Create SparkSession with Kafka",
                "description": "Create SparkSession with Kafka streaming packages configured",
                "hint": """spark = SparkSession.builder \\
    .appName("KafkaETL") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
    .getOrCreate()""",
                "validation": ["SparkSession", "builder", "spark-sql-kafka"],
            },
            {
                "title": "Extract: Read from Kafka topic",
                "description": 'Read streaming data from Kafka topic "orders"',
                "hint": """df_stream = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "orders") \\
    .option("startingOffsets", "latest") \\
    .load()""",
                "validation": [
                    "readStream",
                    'format("kafka")',
                    "subscribe",
                    "bootstrap.servers",
                ],
            },
            {
                "title": "Parse: Convert Kafka value to JSON",
                "description": "Parse the Kafka value column from binary to structured JSON",
                "hint": """from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")""",
                "validation": [
                    "from_json",
                    "StructType",
                    "StructField",
                    'cast("string")',
                ],
            },
            {
                "title": "Transform: Add watermark for late data",
                "description": "Add watermark to handle late-arriving events (10 minutes)",
                "hint": """df_watermarked = df_parsed.withWatermark("timestamp", "10 minutes")""",
                "validation": ["withWatermark", "timestamp"],
            },
            {
                "title": "Transform: Window aggregation",
                "description": "Calculate total sales per customer using tumbling window (5 minutes)",
                "hint": """from pyspark.sql.functions import window, sum as spark_sum

df_windowed = df_watermarked.groupBy(
    window("timestamp", "5 minutes"),
    "customer_id"
).agg(
    spark_sum("amount").alias("total_amount")
)""",
                "validation": ["window(", "groupBy", "agg("],
            },
            {
                "title": "Transform: Prepare for Kafka output",
                "description": "Convert to Kafka format: key and value as strings",
                "hint": """from pyspark.sql.functions import to_json, struct

df_output = df_windowed.select(
    col("customer_id").alias("key"),
    to_json(struct("window", "customer_id", "total_amount")).alias("value")
)""",
                "validation": ["to_json", "struct(", 'alias("key")', 'alias("value")'],
            },
            {
                "title": "Load: Write to Kafka topic",
                "description": 'Write streaming results to "order_aggregates" Kafka topic',
                "hint": """query = df_output.writeStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("topic", "order_aggregates") \\
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \\
    .outputMode("update") \\
    .start()

query.awaitTermination()""",
                "validation": [
                    "writeStream",
                    'format("kafka")',
                    "checkpointLocation",
                    "awaitTermination",
                ],
            },
        ]

        print("You'll code a REAL-TIME Kafka ETL pipeline step by step.\n")
        print(f"📝 Your code will be saved to: {self.practice_dir}/kafka_etl.py\n")
        print(
            f"{self.YELLOW}💡 HINTS are AVAILABLE for each step - this is advanced!{self.RESET}\n"
        )

        # Create practice directory
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/kafka_etl.py"

        # Initialize code file with template
        with open(code_file, "w") as f:
            f.write(
                '''"""
Kafka Streaming ETL Pipeline Practice
Build a real-time data processing pipeline!
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# TODO: Write your Kafka streaming ETL pipeline here

'''
            )

        print(f"✅ Template created! Open: {code_file}\n")
        print(f"{self.CYAN}{'='*80}{self.RESET}")
        print(f"{self.BOLD}📚 KAFKA ETL CONCEPTS YOU'LL PRACTICE:{self.RESET}")
        print("  • Structured Streaming with Kafka")
        print("  • Reading from Kafka topics")
        print("  • JSON parsing and schema definition")
        print("  • Watermarking for late data")
        print("  • Window aggregations (tumbling windows)")
        print("  • Writing back to Kafka")
        print("  • Checkpoint management")
        print(f"{self.CYAN}{'='*80}{self.RESET}\n")

        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0

        for i, step in enumerate(steps):
            self.current_step = i
            self.show_step_menu(steps)

            self.print_section(f"Step {i+1}/7: {step['title']}")
            print(f"📋 Task: {step['description']}\n")
            print(f"{self.YELLOW}💡 Hint available - type 'h' to see it{self.RESET}\n")

            while True:
                choice = self.get_user_input(
                    "Options: [d]one with step | [h]int | [s]kip | [q]uit: ",
                    ["d", "h", "s", "q"],
                )

                if choice == "q":
                    return
                elif choice == "s":
                    print(f"{self.YELLOW}⏭️  Skipped step {i+1}{self.RESET}")
                    break
                elif choice == "h":
                    print(f"\n{self.GREEN}{'='*80}{self.RESET}")
                    print(f"{self.YELLOW}💡 HINT - Copy this code:{self.RESET}\n")
                    print(f"{self.CYAN}{step['hint']}{self.RESET}")
                    print(f"{self.GREEN}{'='*80}{self.RESET}\n")
                    self.hints_used += 1
                elif choice == "d":
                    # Validate code
                    is_valid, missing = self.validate_code(
                        code_file, step["validation"]
                    )
                    if is_valid:
                        print(
                            f"{self.GREEN}✅ Step {i+1} complete! Great job!{self.RESET}"
                        )
                        self.steps_completed.append(i)
                        time.sleep(1)
                        break
                    else:
                        print(f"{self.RED}❌ Code validation failed!{self.RESET}")
                        print(f"Missing patterns: {missing}")
                        print(
                            f"{self.YELLOW}💡 Type 'h' to see the hint for this step{self.RESET}\n"
                        )

        # Final summary
        self.clear_screen()
        self.print_header("🎉 KAFKA ETL PIPELINE COMPLETE!", self.GREEN)
        print(f"✅ Steps completed: {len(self.steps_completed)}/{len(steps)}")
        print(f"💡 Hints used: {self.hints_used}")
        print(f"\n📝 Your code: {code_file}")
        print(f"\n{self.CYAN}{'='*80}{self.RESET}")
        print(f"{self.BOLD}🎓 WHAT YOU'VE LEARNED:{self.RESET}")
        print("  ✅ Kafka integration with PySpark Structured Streaming")
        print("  ✅ Real-time data processing")
        print("  ✅ Handling late-arriving data with watermarks")
        print("  ✅ Window-based aggregations")
        print("  ✅ Writing streaming results back to Kafka")
        print(f"{self.CYAN}{'='*80}{self.RESET}")
        print(
            f"\n{self.YELLOW}💪 CHALLENGE: Now try coding this from memory!{self.RESET}"
        )
        print(
            f"{self.YELLOW}   Goal: Complete without hints in < 20 minutes{self.RESET}\n"
        )
        input("Press Enter to continue...")

    def run(self):
        """Main program loop."""
        while True:
            self.show_welcome()

            choice = self.get_user_input(
                "Choose mode [1-6]: ", ["1", "2", "3", "4", "5", "6"]
            )

            if choice == "1":
                self.run_guided_mode()
            elif choice == "2":
                self.run_timed_mode()
            elif choice == "3":
                self.run_interview_mode()
            elif choice == "4":
                self.show_reference()
            elif choice == "5":
                self.run_kafka_etl_mode()
            elif choice == "6":
                self.clear_screen()
                print(
                    f"\n{self.GREEN}🎉 Happy practicing! Master that ETL pipeline!{self.RESET}\n"
                )
                break


if __name__ == "__main__":
    try:
        gui = ETLPracticeGUI()
        gui.run()
    except KeyboardInterrupt:
        print(f"\n\n{'\033[93m'}👋 Practice session ended. Keep coding!{'\033[0m'}\n")
        sys.exit(0)
