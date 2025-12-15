"""
================================================================================
KAFKA PRODUCER - ORDER DATA GENERATOR
================================================================================

PURPOSE: Generate realistic order data and send to Kafka topic.
Use this to test the Kafka to Parquet ETL pipeline.

USAGE:
    python kafka_producer_orders.py

REQUIREMENTS:
    pip install kafka-python

NOTE: Ensure Kafka is running before executing this script.
      Use docker-compose.yml in this directory to start Kafka.

================================================================================
"""

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderGenerator:
    """Generate realistic e-commerce order data."""
    
    def __init__(self):
        self.customers = [f"CUST-{i:05d}" for i in range(1, 101)]
        self.products = [
            ("PROD-001", "Laptop Pro", 1299.99),
            ("PROD-002", "Wireless Mouse", 29.99),
            ("PROD-003", "Mechanical Keyboard", 149.99),
            ("PROD-004", "USB-C Hub", 49.99),
            ("PROD-005", "Monitor 27\"", 399.99),
            ("PROD-006", "Webcam HD", 79.99),
            ("PROD-007", "Headphones", 199.99),
            ("PROD-008", "Desk Lamp", 39.99),
            ("PROD-009", "Phone Stand", 19.99),
            ("PROD-010", "Cable Organizer", 14.99),
        ]
        self.statuses = ["pending", "processing", "completed", "cancelled"]
        self.order_counter = 1
    
    def generate_order(self):
        """Generate a single order."""
        product_id, product_name, price = random.choice(self.products)
        quantity = random.randint(1, 5)
        amount = round(price * quantity, 2)
        
        # Timestamp: mostly current, occasionally old (to test late data)
        if random.random() < 0.9:
            # 90% recent orders
            timestamp = datetime.utcnow()
        else:
            # 10% delayed orders (5-15 minutes old)
            timestamp = datetime.utcnow() - timedelta(minutes=random.randint(5, 15))
        
        order = {
            "order_id": f"ORD-{self.order_counter:06d}",
            "customer_id": random.choice(self.customers),
            "product_id": product_id,
            "product_name": product_name,
            "quantity": quantity,
            "price": price,
            "amount": amount,
            "order_timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": random.choice(self.statuses)
        }
        
        self.order_counter += 1
        return order


def create_producer(bootstrap_servers="localhost:9092"):
    """
    Create Kafka producer.
    
    Args:
        bootstrap_servers: Kafka broker address
    
    Returns:
        KafkaProducer instance
    """
    logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # Wait for all replicas
        retries=3,
        max_in_flight_requests_per_connection=1
    )
    
    logger.info("Kafka producer created successfully")
    return producer


def send_orders(producer, topic, generator, num_orders=None, delay=1.0):
    """
    Send orders to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic: Kafka topic name
        generator: OrderGenerator instance
        num_orders: Number of orders to send (None = infinite)
        delay: Delay between orders in seconds
    """
    logger.info(f"Sending orders to topic: {topic}")
    logger.info(f"Delay between orders: {delay} seconds")
    
    if num_orders:
        logger.info(f"Will send {num_orders} orders")
    else:
        logger.info("Will send orders continuously (Ctrl+C to stop)")
    
    count = 0
    
    try:
        while True:
            # Generate order
            order = generator.generate_order()
            
            # Use customer_id as key for partitioning
            key = order['customer_id']
            
            # Send to Kafka
            future = producer.send(topic, key=key, value=order)
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            count += 1
            
            if count % 10 == 0:
                logger.info(f"Sent {count} orders - Last: {order['order_id']} "
                           f"(partition: {record_metadata.partition}, "
                           f"offset: {record_metadata.offset})")
            
            # Check if we've sent enough
            if num_orders and count >= num_orders:
                break
            
            # Delay before next order
            time.sleep(delay)
    
    except KeyboardInterrupt:
        logger.info("\nStopping order generation...")
    
    finally:
        logger.info(f"Total orders sent: {count}")
        producer.flush()
        producer.close()
        logger.info("Producer closed")


def simulate_burst_traffic(producer, topic, generator):
    """
    Simulate burst traffic pattern.
    
    This creates realistic traffic:
    - Normal periods: 1 order per second
    - Peak periods: 10 orders per second
    - Quiet periods: 1 order per 5 seconds
    """
    logger.info("Starting burst traffic simulation...")
    
    patterns = [
        ("Normal", 1.0, 30),    # 30 orders at 1/sec
        ("Peak", 0.1, 50),      # 50 orders at 10/sec
        ("Quiet", 5.0, 10),     # 10 orders at 1/5sec
    ]
    
    try:
        while True:
            for pattern_name, delay, count in patterns:
                logger.info(f"Traffic pattern: {pattern_name} ({count} orders)")
                send_orders(producer, topic, generator, count, delay)
                time.sleep(2)  # Brief pause between patterns
    
    except KeyboardInterrupt:
        logger.info("\nStopping simulation...")


def main():
    """Main execution."""
    logger.info("="*80)
    logger.info("KAFKA ORDER PRODUCER STARTED")
    logger.info("="*80)
    
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "orders"
    
    try:
        # Create producer
        producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
        
        # Create order generator
        generator = OrderGenerator()
        
        # Menu
        print("\nSelect mode:")
        print("1. Continuous orders (1 per second)")
        print("2. Send N orders")
        print("3. Burst traffic simulation")
        print("4. Fast stream (10 orders per second)")
        
        choice = input("\nChoice [1-4]: ").strip()
        
        if choice == "1":
            send_orders(producer, KAFKA_TOPIC, generator, delay=1.0)
        
        elif choice == "2":
            n = int(input("How many orders? "))
            send_orders(producer, KAFKA_TOPIC, generator, num_orders=n, delay=1.0)
        
        elif choice == "3":
            simulate_burst_traffic(producer, KAFKA_TOPIC, generator)
        
        elif choice == "4":
            send_orders(producer, KAFKA_TOPIC, generator, delay=0.1)
        
        else:
            logger.warning("Invalid choice, using default (continuous)")
            send_orders(producer, KAFKA_TOPIC, generator, delay=1.0)
    
    except Exception as e:
        logger.error(f"Producer failed: {e}", exc_info=True)
        raise
    
    finally:
        logger.info("="*80)
        logger.info("KAFKA ORDER PRODUCER STOPPED")
        logger.info("="*80)


if __name__ == "__main__":
    main()
