"""
================================================================================
KAFKA STREAMING PRACTICE - INTERACTIVE LEARNING SYSTEM
================================================================================

Master real-time Kafka streaming with PySpark!
Focus: Kafka Topic → Transform → Kafka Topic

GOAL: Code a Kafka streaming pipeline in < 20 minutes from memory!
================================================================================
"""

import os
import sys
import time


class KafkaStreamingPractice:
    """Interactive practice for Kafka streaming (Kafka → Kafka)."""
    
    def __init__(self):
        self.practice_dir = "/tmp/etl_practice/kafka_stream"
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
        self.print_header("🔥 KAFKA STREAMING PRACTICE 🔥", self.CYAN)
        
        print(f"{self.BOLD}Master Real-Time Kafka Streaming!{self.RESET}\n")
        print("📋 What you'll build:")
        print("   • Read from Kafka topic (orders)")
        print("   • Parse JSON messages")
        print("   • Add watermark for late data")
        print("   • Window aggregations (5-min)")
        print("   • Write to Kafka topic (aggregates)\n")
        
        print("📚 Practice Modes:")
        print("   1. GUIDED MODE    - Step-by-step with hints")
        print("   2. TIMED MODE     - 20-minute speed challenge")
        print("   3. INTERVIEW MODE - 25-minute no-hints simulation")
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
        self.print_header("📚 GUIDED MODE - Kafka Streaming", self.GREEN)
        
        steps = [
            {
                'title': 'Setup: SparkSession with Kafka',
                'description': 'Create SparkSession with Kafka packages',
                'hint': '''from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("KafkaStreaming") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
    .getOrCreate()''',
                'validation': ['SparkSession', 'spark-sql-kafka', 'builder']
            },
            {
                'title': 'Extract: Read from Kafka',
                'description': 'Read streaming data from Kafka topic "orders"',
                'hint': '''df_stream = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "orders") \\
    .option("startingOffsets", "latest") \\
    .load()''',
                'validation': ['readStream', 'format("kafka")', 'subscribe', 'bootstrap.servers']
            },
            {
                'title': 'Parse: JSON to DataFrame',
                'description': 'Parse Kafka value column as JSON with schema',
                'hint': '''from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")''',
                'validation': ['from_json', 'StructType', 'StructField', 'cast("string")']
            },
            {
                'title': 'Transform: Add Watermark',
                'description': 'Add 10-minute watermark for late data',
                'hint': '''df_watermarked = df_parsed.withWatermark("timestamp", "10 minutes")''',
                'validation': ['withWatermark', 'timestamp']
            },
            {
                'title': 'Transform: Window Aggregation',
                'description': 'Aggregate sales per customer in 5-minute windows',
                'hint': '''from pyspark.sql.functions import window, sum as spark_sum, count

df_windowed = df_watermarked.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("customer_id")
).agg(
    spark_sum("amount").alias("total_amount"),
    count("order_id").alias("order_count")
)''',
                'validation': ['window(', 'groupBy', 'agg(', '5 minutes']
            },
            {
                'title': 'Prepare: Kafka Output Format',
                'description': 'Convert to Kafka format (key and value)',
                'hint': '''from pyspark.sql.functions import to_json, struct

df_output = df_windowed.select(
    col("customer_id").alias("key"),
    to_json(struct("window", "customer_id", "total_amount", "order_count")).alias("value")
)''',
                'validation': ['to_json', 'struct(', 'alias("key")', 'alias("value")']
            },
            {
                'title': 'Load: Write to Kafka',
                'description': 'Write results to "order_aggregates" Kafka topic',
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
        
        print("Build a real-time Kafka streaming pipeline!\n")
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/my_kafka_streaming.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Kafka Streaming Pipeline Practice"""\n\n')
            f.write('# TODO: Write your Kafka streaming pipeline\n\n')
        
        print(f"📝 Your code file: {code_file}\n")
        print(f"✅ Template created!\n")
        
        self.current_step = 0
        self.steps_completed = []
        self.hints_used = 0
        
        for i, step in enumerate(steps):
            self.current_step = i
            self.show_step_menu(steps)
            
            self.print_section(f"Step {i+1}/7: {step['title']}")
            print(f"�� Task: {step['description']}\n")
            
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
        self.print_header("⏱️  TIMED MODE - Kafka Streaming", self.YELLOW)
        
        print("🎯 Build complete Kafka streaming pipeline!\n")
        print("📋 Requirements:")
        print("   1. SparkSession with Kafka packages")
        print("   2. Read from 'orders' topic")
        print("   3. Parse JSON with schema")
        print("   4. Add 10-min watermark")
        print("   5. Window aggregation (5-min)")
        print("   6. Write to 'order_aggregates'\n")
        
        print(f"{self.YELLOW}⏱️  Target: 20 minutes{self.RESET}")
        print(f"{self.GREEN}🏆 Expert: 15 minutes{self.RESET}\n")
        
        input("Press Enter to start...")
        
        self.start_time = time.time()
        print(f"\n{self.GREEN}⏱️  Timer started!{self.RESET}\n")
        
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/timed_kafka_streaming.py"
        
        with open(code_file, 'w') as f:
            f.write('"""Timed Kafka Streaming Challenge"""\n\n')
            f.write('from pyspark.sql import SparkSession\n')
            f.write('from pyspark.sql.functions import *\n')
            f.write('from pyspark.sql.types import *\n\n')
            f.write('# TODO: Complete the streaming pipeline\n\n')
        
        print(f"📝 Code: {code_file}\n")
        
        self.get_user_input("Type 'done': ", ['done'])
        
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        self.clear_screen()
        self.print_header("⏱️  TIME'S UP!", self.CYAN)
        
        print(f"⏱️  Time: {minutes}m {seconds}s\n")
        
        if elapsed < 900:
            print(f"{self.GREEN}🏆 EXPERT LEVEL!{self.RESET}")
        elif elapsed < 1200:
            print(f"{self.GREEN}✅ TARGET HIT!{self.RESET}")
        else:
            print(f"{self.YELLOW}💪 Keep practicing!{self.RESET}")
        
        print(f"\n📝 Code: {code_file}\n")
        input("Press Enter...")
        
    def run_interview_mode(self):
        self.clear_screen()
        self.print_header("🎤 INTERVIEW MODE - Kafka", self.RED)
        
        print(f"{self.BOLD}Real Interview!{self.RESET}\n")
        print("📋 Interviewer:")
        print('   "Build a real-time streaming pipeline that reads order')
        print('    events from Kafka, aggregates sales per customer in')
        print('    5-minute windows, and writes results to another Kafka')
        print('    topic. Handle late data with watermarking."\n')
        
        print(f"{self.RED}⚠️  Rules:{self.RESET}")
        print("   • No hints")
        print("   • 25-minute limit")
        print("   • Must include checkpointing\n")
        
        choice = self.get_user_input("Ready? [y/n]: ", ['y', 'n'])
        if choice == 'n':
            return
            
        self.start_time = time.time()
        print(f"\n{self.RED}🎤 Interview started!{self.RESET}\n")
        
        os.makedirs(self.practice_dir, exist_ok=True)
        code_file = f"{self.practice_dir}/interview_kafka_streaming.py"
        
        with open(code_file, 'w') as f:
            f.write('# Interview: Kafka Streaming Pipeline\n\n')
        
        print(f"📝 Code: {code_file}")
        print(f"⏱️  Timer: 25 minutes\n")
        
        self.get_user_input("Type 'submit': ", ['submit'])
        
        elapsed = time.time() - self.start_time
        minutes = int(elapsed // 60)
        
        self.clear_screen()
        if elapsed <= 1500:
            self.print_header("🎉 ON TIME!", self.GREEN)
            print(f"✅ Time: {minutes} min")
        else:
            self.print_header("⏰ OVERTIME", self.RED)
            print(f"⚠️  {minutes} min")
        
        print(f"\n📝 Code: {code_file}\n")
        input("Press Enter...")
        
    def show_reference(self):
        self.clear_screen()
        self.print_header("📖 REFERENCE - Kafka Streaming", self.BLUE)
        
        print("Complete production Kafka streaming:\n")
        
        solution = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \\
    .appName("KafkaStreaming") \\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \\
    .getOrCreate()

# Read from Kafka
df_stream = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "orders") \\
    .option("startingOffsets", "latest") \\
    .load()

# Define schema
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Parse JSON
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Add watermark
df_watermarked = df_parsed.withWatermark("timestamp", "10 minutes")

# Window aggregation
df_windowed = df_watermarked.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("customer_id")
).agg(
    sum("amount").alias("total_amount"),
    count("order_id").alias("order_count")
)

# Prepare for Kafka
df_output = df_windowed.select(
    col("customer_id").alias("key"),
    to_json(struct("window", "customer_id", "total_amount", "order_count")).alias("value")
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
'''
        
        print(solution)
        print("\n" + "=" * 80)
        print(f"\n{self.YELLOW}💡 Study and practice from memory!{self.RESET}\n")
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
                print(f"\n{self.GREEN}🎉 Keep streaming!{self.RESET}\n")
                break


if __name__ == "__main__":
    try:
        practice = KafkaStreamingPractice()
        practice.run()
    except KeyboardInterrupt:
        print(f"\n\n{'\033[93m'}👋 Ended!{'\033[0m'}\n")
        sys.exit(0)
