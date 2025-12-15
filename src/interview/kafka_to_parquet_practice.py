"""
================================================================================
KAFKA TO PARQUET PRACTICE - INTERACTIVE LEARNING SYSTEM
================================================================================

Master Kafka streaming to data lake ingestion!
Focus: Kafka Topic → Transform → Parquet Storage

GOAL: Code a Kafka-to-Parquet pipeline in < 25 minutes from memory!
================================================================================
"""

import os
import sys
import time


class KafkaToParquetPractice:
    """Interactive practice for Kafka to Parquet streaming."""
    
    def __init__(self):
        self.practice_dir = "/tmp/etl_practice/kafka_parquet"
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
        self.print_header("💾 KAFKA TO PARQUET PRACTICE 💾", self.GREEN)
        
        print(f"{self.BOLD}Master Streaming Data Lake Ingestion!{self.RESET}\n")
        print("📋 What you'll build:")
        print("   • Read from Kafka topic (orders)")
        print("   • Parse JSON with schema")
        print("   • Transform and enrich data")
        print("   • Add partition columns (year/month/day)")
        print("   • Write to partitioned Parquet files\n")
        
        print("�� Practice Modes:")
        print("   1. GUIDED MODE    - Step-by-step with hints")
        print("   2. TIMED MODE     - 25-minute speed challenge")
        print("   3. INTERVIEW MODE - 30-minute no-hints simulation")
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
        self.print_header("📚 GUIDED MODE - Kafka to Parquet", self.GREEN)
        
        steps = [
            {
                'title': 'Setup: SparkSession with Kafka',
                'description': 'Create SparkSession with Kafka packages',
                'hint': '''from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("KafkaToParquet") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
    .getOrCreate()''',
                'validation': ['SparkSession', 'spark-sql-kafka', 'builder']
            },
            {
                'title': 'Extract: Read from Kafka',
                'description': 'Read streaming data from Kafka topic "orders"',
                'hint': '''df_kafka = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "orders") \\
    .option("startingOffsets", "latest") \\
    .load()''',
                'validation': ['readStream', 'format("kafka")', 'subscribe']
            },
            {
                'title': 'Parse: JSON with Full Schema',
                'description': 'Parse Kafka value as JSON with complete order schema',
                'hint': '''from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("amount", DoubleType(), False),
    StructField("order_timestamp", StringType(), False),
    StructField("status", StringType(), True)
])

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")''',
                'validation': ['from_json', 'StructType', 'StructField', 'IntegerType', 'DoubleType']
            },
            {
                'title': 'Transform: Convert Timestamps',
                'description': 'Convert timestamp string to timestamp type',
                'hint': '''from pyspark.sql.functions import to_timestamp

df_with_ts = df_parsed.withColumn(
    "order_timestamp",
    to_timestamp(col("order_timestamp"))
)''',
                'validation': ['to_timestamp', 'withColumn']
            },
            {
                'title': 'Transform: Add Partition Columns',
                'description': 'Add year, month, day columns for partitioning',
                'hint': '''from pyspark.sql.functions import year, month, day, hour, current_timestamp

df_transformed = df_with_ts \\
    .withColumn("year", year(col("order_timestamp"))) \\
    .withColumn("month", month(col("order_timestamp"))) \\
    .withColumn("day", day(col("order_timestamp"))) \\
    .withColumn("hour", hour(col("order_timestamp"))) \\
    .withColumn("processing_timestamp", current_timestamp())''',
                'validation': ['year', 'month', 'day', 'hour', 'current_timestamp']
            },
            {
                'title': 'Transform: Data Quality',
                'description': 'Filter out invalid records (null IDs, zero/negative amounts)',
                'hint': '''df_clean = df_transformed \\
    .filter(col("order_id").isNotNull()) \\
    .filter(col("customer_id").isNotNull()) \\
    .filter(col("amount") > 0)''',
                'validation': ['filter', 'isNotNull', 'amount']
            },
            {
                'title': 'Load: Write to Parquet',
                'description': 'Write to partitioned Parquet with checkpointing',
                'hint': '''query = df_clean.writeStream \\
    .format("parquet") \\
    .outputMode("append") \\
    .option("path", "/tmp/data_lake/orders") \\
    .option("checkpointLocation", "/tmp/kafka_checkpoint/orders") \\
    .partitionBy("year", "month", "day") \\
    .trigger(processingTime="30 seconds") \\
    .start()

query.awaitTermination()''',
                'validation': ['writeStream', '.parquet', 'partitionBy', 'checkpointLocation', 'trigger']
            }
        ]
        
        print("Build a Kafka-to-Parquet streaming pipeline!\n")
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/my_kafka_parquet.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Kafka to Parquet Pipeline Practice"""\n\n')
            f.write('# TODO: Write your Kafka-to-Parquet pipeline\n\n')
        
        print(f"📝 Your code file: {code_file}\n")
        print(f"✅ Template created!\n")
        
        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0
        
        for i, step in enumerate(steps):
            self.current_step = i
            self.show_step_menu(steps)
            
            self.print_section(f"Step {i+1}/7: {step['title']}")
            print(f"📋 Task: {step['description']}\n")
            
            while True:
                choice = self.get_user_input(
                    "Options: [d]one | [h]int | [s]kip | [q]uit: ",
                    ['d', 'h', 's', 'q']
                )
                
                if choice == 'q':
                    return
                elif choice == 's':
                    print(f"{self.YELLOW}⏭️  Skipped{self.RESET}")
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
                        print(f"Missing: {missing}\n")
        
        self.clear_screen()
        self.print_header("🎉 GUIDED MODE COMPLETE!", self.GREEN)
        print(f"✅ Steps: {len(self.steps_completed)}/{len(steps)}")
        print(f"💡 Hints: {self.hints_used}")
        print(f"\n📝 Code: {code_file}\n")
        input("Press Enter...")
        
    def run_timed_mode(self):
        self.clear_screen()
        self.print_header("⏱️  TIMED MODE - Kafka to Parquet", self.YELLOW)
        
        print("🎯 Build complete Kafka-to-Parquet pipeline!\n")
        print("📋 Requirements:")
        print("   1. SparkSession with Kafka packages")
        print("   2. Read from 'orders' topic")
        print("   3. Parse JSON (9 fields: order_id, customer_id, etc.)")
        print("   4. Convert timestamp string to timestamp")
        print("   5. Add partition columns (year, month, day)")
        print("   6. Filter invalid data")
        print("   7. Write to partitioned Parquet with checkpointing\n")
        
        print(f"{self.YELLOW}⏱️  Target: 25 minutes{self.RESET}")
        print(f"{self.GREEN}🏆 Expert: 18 minutes{self.RESET}\n")
        
        input("Press Enter to start...")
        
        self.start_time = time.time()
        print(f"\n{self.GREEN}⏱️  Timer started!{self.RESET}\n")
        
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/timed_kafka_parquet.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Timed Kafka to Parquet Challenge"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import *\n')
            f.write('from pyspark.sql.types import *\n\n')
            f.write('# TODO: Complete the pipeline\n\n')
        
        print(f"📝 Code: {code_file}\n")
        
        self.get_user_input("Type 'done': ", ['done'])
        
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        self.clear_screen()
        self.print_header("⏱️  TIME'S UP!", self.CYAN)
        
        print(f"⏱️  Time: {minutes}m {seconds}s\n")
        
        if elapsed < 1080:  # 18 min
            print(f"{self.GREEN}🏆 EXPERT LEVEL!{self.RESET}")
        elif elapsed < 1500:  # 25 min
            print(f"{self.GREEN}✅ TARGET HIT!{self.RESET}")
        else:
            print(f"{self.YELLOW}💪 Keep practicing!{self.RESET}")
        
        print(f"\n📝 Code: {code_file}\n")
        input("Press Enter...")
        
    def run_interview_mode(self):
        self.clear_screen()
        self.print_header("🎤 INTERVIEW MODE - Data Lake Ingestion", self.RED)
        
        print(f"{self.BOLD}Real Interview!{self.RESET}\n")
        print("�� Interviewer:")
        print('   "We need to ingest real-time order data from Kafka into')
        print('    our data lake. Build a streaming pipeline that reads from')
        print('    Kafka, parses the JSON, transforms it by adding partition')
        print('    columns, filters invalid records, and writes to partitioned')
        print('    Parquet files with proper checkpointing for fault tolerance."')
        print()
        
        print(f"{self.RED}⚠️  Rules:{self.RESET}")
        print("   • No hints")
        print("   • 30-minute limit")
        print("   • Must handle schema properly")
        print("   • Must partition data efficiently\n")
        
        choice = self.get_user_input("Ready? [y/n]: ", ['y', 'n'])
        if choice == 'n':
            return
            
        self.start_time = time.time()
        print(f"\n{self.RED}🎤 Interview started!{self.RESET}\n")
        
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/interview_kafka_parquet.py"
        
        with open(code_file, 'w') as f:
            f.write('# Interview: Kafka to Parquet Data Lake Pipeline\n\n')
        
        print(f"📝 Code: {code_file}")
        print(f"⏱️  Timer: 30 minutes\n")
        
        self.get_user_input("Type 'submit': ", ['submit'])
        
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        
        self.clear_screen()
        if elapsed <= 1800:
            self.print_header("🎉 SUBMITTED ON TIME!", self.GREEN)
            print(f"✅ Time: {minutes} minutes")
        else:
            self.print_header("⏰ OVERTIME", self.RED)
            print(f"⚠️  Took: {minutes} minutes")
        
        print(f"\n📝 Code: {code_file}\n")
        input("Press Enter...")
        
    def show_reference(self):
        self.clear_screen()
        self.print_header("📖 REFERENCE - Kafka to Parquet", self.BLUE)
        
        print("Complete production Kafka-to-Parquet pipeline:\n")
        
        solution = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Create Spark session
    spark = SparkSession.builder \\
        .appName("KafkaToParquet") \\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .getOrCreate()
    
    logger.info("Starting Kafka to Parquet pipeline...")
    
    # Read from Kafka
    df_kafka = spark.readStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("subscribe", "orders") \\
        .option("startingOffsets", "latest") \\
        .option("failOnDataLoss", "false") \\
        .load()
    
    # Define schema
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("amount", DoubleType(), False),
        StructField("order_timestamp", StringType(), False),
        StructField("status", StringType(), True)
    ])
    
    # Parse JSON
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset")
    ).select("data.*", "topic", "partition", "offset")
    
    # Transform
    df_transformed = df_parsed \\
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \\
        .withColumn("processing_timestamp", current_timestamp()) \\
        .withColumn("revenue", col("quantity") * col("price")) \\
        .withColumn("year", year(col("order_timestamp"))) \\
        .withColumn("month", month(col("order_timestamp"))) \\
        .withColumn("day", day(col("order_timestamp"))) \\
        .withColumn("hour", hour(col("order_timestamp"))) \\
        .filter(col("order_id").isNotNull()) \\
        .filter(col("customer_id").isNotNull()) \\
        .filter(col("amount") > 0)
    
    # Write to Parquet
    query = df_transformed.writeStream \\
        .format("parquet") \\
        .outputMode("append") \\
        .option("path", "/tmp/data_lake/orders") \\
        .option("checkpointLocation", "/tmp/kafka_checkpoint/orders") \\
        .partitionBy("year", "month", "day") \\
        .trigger(processingTime="30 seconds") \\
        .start()
    
    logger.info("Pipeline running...")
    logger.info("Output: /tmp/data_lake/orders")
    logger.info("Checkpoint: /tmp/kafka_checkpoint/orders")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
'''
        
        print(solution)
        print("\n" + "=" * 80)
        print(f"\n{self.YELLOW}💡 Study and code from memory!{self.RESET}\n")
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
                print(f"\n{self.GREEN}🎉 Master the data lake!{self.RESET}\n")
                break


if __name__ == "__main__":
    try:
        practice = KafkaToParquetPractice()
        practice.run()
    except KeyboardInterrupt:
        print(f"\n\n{'\033[93m'}👋 Ended!{'\033[0m'}\n")
        sys.exit(0)
