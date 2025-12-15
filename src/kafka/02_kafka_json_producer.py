#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
KAFKA JSON PRODUCER - Generate Test Data for Kafka Streams
================================================================================

📖 OVERVIEW:
This script produces test messages to Kafka topics for testing PySpark
streaming applications. It generates realistic JSON events that can be
consumed by the consumer examples.

🎯 USE CASE:
Testing and development of Kafka streaming applications without needing
real production data.

📋 GENERATES:
• User events (page views, clicks, searches)
• E-commerce events (purchases, cart additions)
• IoT sensor readings
• Application logs

🚀 RUN:
python 02_kafka_json_producer.py

📦 REQUIRES:
pip install kafka-python faker
================================================================================
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    print("⚠️  Warning: faker not installed. Using simple random data.")
    fake = None


def create_producer():
    """
    Create Kafka producer with JSON serialization.
    """
    print("=" * 80)
    print("🚀 CREATING KAFKA PRODUCER")
    print("=" * 80)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
            compression_type='snappy'
        )
        
        print("✅ Connected to Kafka broker: localhost:9092")
        print("✅ Serialization: JSON")
        print("✅ Compression: snappy")
        print()
        
        return producer
    
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        print("   Make sure Kafka is running on localhost:9092")
        return None


def generate_user_event():
    """Generate realistic user event data."""
    event_types = ['page_view', 'button_click', 'form_submit', 'search', 'logout']
    devices = ['desktop', 'mobile', 'tablet']
    countries = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'IN', 'BR', 'JP', 'CN']
    pages = [
        '/home', '/products', '/products/laptop', '/products/phone',
        '/cart', '/checkout', '/account', '/support', '/about', '/blog'
    ]
    
    if fake:
        user_id = f"user_{fake.uuid4()[:8]}"
        session_id = f"sess_{fake.uuid4()[:12]}"
        referrer = fake.url()
    else:
        user_id = f"user_{random.randint(1000, 9999)}"
        session_id = f"sess_{random.randint(100000, 999999)}"
        referrer = "direct"
    
    event = {
        "user_id": user_id,
        "event_type": random.choice(event_types),
        "page_url": random.choice(pages),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "session_id": session_id,
        "device": random.choice(devices),
        "country": random.choice(countries),
        "referrer": referrer,
        "duration_seconds": random.randint(1, 300)
    }
    
    return event


def generate_purchase_event():
    """Generate e-commerce purchase event."""
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    payment_methods = ['credit_card', 'paypal', 'apple_pay', 'google_pay']
    
    if fake:
        user_id = f"user_{fake.uuid4()[:8]}"
        product_name = fake.word().capitalize()
    else:
        user_id = f"user_{random.randint(1000, 9999)}"
        product_name = f"Product_{random.randint(100, 999)}"
    
    event = {
        "user_id": user_id,
        "event_type": "purchase",
        "product_id": f"prod_{random.randint(1000, 9999)}",
        "product_name": product_name,
        "category": random.choice(categories),
        "price": round(random.uniform(9.99, 999.99), 2),
        "quantity": random.randint(1, 5),
        "payment_method": random.choice(payment_methods),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "order_id": f"order_{random.randint(100000, 999999)}"
    }
    
    event["total_amount"] = round(event["price"] * event["quantity"], 2)
    
    return event


def generate_iot_sensor_reading():
    """Generate IoT sensor reading."""
    sensor_types = ['temperature', 'humidity', 'pressure', 'motion', 'light']
    locations = ['warehouse_a', 'warehouse_b', 'office_1', 'factory_floor', 'parking_lot']
    
    sensor_id = f"sensor_{random.randint(1000, 9999)}"
    sensor_type = random.choice(sensor_types)
    
    # Generate value based on sensor type
    if sensor_type == 'temperature':
        value = round(random.uniform(15.0, 35.0), 2)
        unit = 'celsius'
    elif sensor_type == 'humidity':
        value = round(random.uniform(20.0, 80.0), 2)
        unit = 'percent'
    elif sensor_type == 'pressure':
        value = round(random.uniform(980.0, 1040.0), 2)
        unit = 'hPa'
    elif sensor_type == 'motion':
        value = random.choice([0, 1])
        unit = 'boolean'
    else:  # light
        value = round(random.uniform(0.0, 10000.0), 2)
        unit = 'lux'
    
    event = {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type,
        "value": value,
        "unit": unit,
        "location": random.choice(locations),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "battery_level": random.randint(0, 100),
        "status": "active" if random.random() > 0.05 else "warning"
    }
    
    return event


def generate_app_log():
    """Generate application log entry."""
    log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
    services = ['api-gateway', 'auth-service', 'payment-service', 'user-service', 'notification-service']
    
    messages = {
        'INFO': ['User logged in', 'Request processed successfully', 'Cache hit', 'Job completed'],
        'WARN': ['High memory usage', 'Slow query detected', 'Retry attempt', 'Rate limit approaching'],
        'ERROR': ['Database connection failed', 'Authentication failed', 'Timeout exceeded', 'Invalid request'],
        'DEBUG': ['Processing request', 'Calling external API', 'Validating input', 'Cache miss']
    }
    
    level = random.choice(log_levels)
    
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "service": random.choice(services),
        "message": random.choice(messages[level]),
        "request_id": f"req_{random.randint(100000, 999999)}",
        "duration_ms": random.randint(10, 5000),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "endpoint": f"/api/v1/{random.choice(['users', 'orders', 'products', 'payments'])}"
    }
    
    return event


def produce_messages(producer, topic, generator_func, count, delay=0.1):
    """
    Produce messages to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        topic: Topic name
        generator_func: Function to generate message data
        count: Number of messages to produce
        delay: Delay between messages in seconds
    """
    print(f"📤 Producing {count} messages to topic: {topic}")
    
    success_count = 0
    error_count = 0
    
    for i in range(count):
        try:
            # Generate message
            message = generator_func()
            key = message.get('user_id', message.get('sensor_id', f'key_{i}'))
            
            # Send to Kafka
            future = producer.send(topic, key=key, value=message)
            
            # Wait for confirmation (optional, for reliability)
            record_metadata = future.get(timeout=10)
            
            success_count += 1
            
            if (i + 1) % 100 == 0:
                print(f"   ✓ Sent {i + 1}/{count} messages")
            
            # Delay between messages
            if delay > 0:
                time.sleep(delay)
        
        except KafkaError as e:
            error_count += 1
            print(f"   ✗ Error sending message: {e}")
        
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
            break
    
    # Flush remaining messages
    producer.flush()
    
    print(f"\n✅ Completed: {success_count} successful, {error_count} errors")
    return success_count, error_count


def main():
    """
    Main execution function.
    """
    print("\n" + "🔥 " * 40)
    print("KAFKA JSON PRODUCER - TEST DATA GENERATOR")
    print("🔥 " * 40)
    print()
    
    producer = create_producer()
    
    if not producer:
        print("❌ Failed to create producer. Exiting.")
        return
    
    try:
        print("=" * 80)
        print("📊 PRODUCING TEST DATA")
        print("=" * 80)
        print()
        
        # Produce user events
        print("1️⃣  User Events (page views, clicks, searches)")
        produce_messages(producer, 'user-events', generate_user_event, count=50, delay=0.05)
        
        print("\n" + "-" * 80 + "\n")
        
        # Produce purchase events
        print("2️⃣  Purchase Events (e-commerce transactions)")
        produce_messages(producer, 'payment-events', generate_purchase_event, count=20, delay=0.1)
        
        print("\n" + "-" * 80 + "\n")
        
        # Produce IoT sensor readings
        print("3️⃣  IoT Sensor Readings (temperature, humidity, etc.)")
        produce_messages(producer, 'iot-sensors', generate_iot_sensor_reading, count=100, delay=0.02)
        
        print("\n" + "-" * 80 + "\n")
        
        # Produce application logs
        print("4️⃣  Application Logs (INFO, WARN, ERROR)")
        produce_messages(producer, 'system-logs', generate_app_log, count=30, delay=0.05)
        
        print("\n" + "=" * 80)
        print("✅ ALL MESSAGES PRODUCED SUCCESSFULLY")
        print("=" * 80)
        
        print("""
📋 Summary of Topics Created:

1. user-events (50 messages)
   • User interactions: page views, clicks, searches
   • Fields: user_id, event_type, page_url, device, country
   
2. payment-events (20 messages)
   • E-commerce purchases
   • Fields: user_id, product_id, price, quantity, payment_method
   
3. iot-sensors (100 messages)
   • Sensor readings: temperature, humidity, pressure, motion, light
   • Fields: sensor_id, sensor_type, value, unit, location
   
4. system-logs (30 messages)
   • Application logs: INFO, WARN, ERROR
   • Fields: level, service, message, request_id, duration_ms

🎯 Next Steps:
   1. Run Kafka consumer examples to read these messages:
      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
                   01_basic_kafka_consumer.py
   
   2. Check messages in Kafka:
      kafka-console-consumer --bootstrap-server localhost:9092 \\
                             --topic user-events --from-beginning
   
   3. Experiment with different message rates and volumes
   
   4. Modify generator functions for your specific use case
        """)
    
    except Exception as e:
        print(f"\n❌ Error during production: {e}")
    
    finally:
        producer.close()
        print("\n✅ Producer closed")


if __name__ == "__main__":
    main()
