"""
================================================================================
Kafka Message Simulator for ETL Practice
================================================================================

PURPOSE: Simulate streaming order data being sent to Kafka for practice.

This script generates realistic order events and sends them to Kafka
so you can test your streaming ETL pipeline.

USAGE:
  1. Start Kafka locally (or use docker-compose)
  2. Run this script: python kafka_simulator.py
  3. Run your Kafka ETL pipeline
  4. Watch real-time processing!

================================================================================
"""

import json
import time
import random
from datetime import datetime, timedelta


def generate_order_event():
    """Generate a single order event."""
    order_id = f"ORD-{random.randint(10000, 99999)}"
    customer_id = f"CUST-{random.randint(100, 999)}"
    amount = round(random.uniform(10.0, 500.0), 2)
    timestamp = datetime.now().isoformat()
    
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
        "timestamp": timestamp
    }


def simulate_with_kafka():
    """Send events to real Kafka."""
    try:
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print("✅ Connected to Kafka!")
        print("🔥 Sending order events to 'orders' topic...\n")
        
        event_count = 0
        while True:
            event = generate_order_event()
            producer.send('orders', value=event)
            
            event_count += 1
            print(f"📤 Event {event_count}: {event['order_id']} | "
                  f"Customer: {event['customer_id']} | "
                  f"Amount: ${event['amount']}")
            
            # Random delay between 1-3 seconds
            time.sleep(random.uniform(1, 3))
            
    except ImportError:
        print("❌ kafka-python not installed!")
        print("�� Install with: pip install kafka-python")
        print("\n⚠️  Falling back to mock mode...\n")
        simulate_mock_mode()
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
        print("⚠️  Make sure Kafka is running on localhost:9092")
        print("\n⚠️  Falling back to mock mode...\n")
        simulate_mock_mode()


def simulate_mock_mode():
    """Simulate without actual Kafka (for practice)."""
    print("🔄 MOCK MODE - Generating events (not sending to Kafka)\n")
    print("This shows you what data WOULD be sent to Kafka.")
    print("Use this to understand the data structure!\n")
    print("="*70 + "\n")
    
    event_count = 0
    while event_count < 20:  # Only show 20 events in mock mode
        event = generate_order_event()
        event_count += 1
        
        print(f"📊 Event {event_count}:")
        print(f"   Order ID:    {event['order_id']}")
        print(f"   Customer ID: {event['customer_id']}")
        print(f"   Amount:      ${event['amount']}")
        print(f"   Timestamp:   {event['timestamp']}")
        print()
        
        time.sleep(1)
    
    print("="*70)
    print("✅ Generated 20 sample events!")
    print("\n📚 Data Structure:")
    print("   • order_id: String (ORD-XXXXX)")
    print("   • customer_id: String (CUST-XXX)")
    print("   • amount: Double (10.00 - 500.00)")
    print("   • timestamp: Timestamp (ISO format)")
    print("\n💡 Use this schema in your PySpark code!")


def create_sample_events_file():
    """Create a JSON file with sample events."""
    output_file = "/tmp/sample_kafka_events.json"
    
    events = [generate_order_event() for _ in range(100)]
    
    with open(output_file, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    print(f"\n📝 Created sample events file: {output_file}")
    print("   You can use this to understand the data format!")


if __name__ == "__main__":
    print("="*70)
    print("🔥 KAFKA MESSAGE SIMULATOR".center(70))
    print("="*70 + "\n")
    
    print("Options:")
    print("  1. Send to real Kafka (requires Kafka running)")
    print("  2. Mock mode (just show sample data)")
    print("  3. Create sample events file")
    print("  4. Exit\n")
    
    choice = input("Choose option [1-4]: ").strip()
    
    if choice == '1':
        print("\n🚀 Starting Kafka producer...\n")
        simulate_with_kafka()
    elif choice == '2':
        simulate_mock_mode()
    elif choice == '3':
        create_sample_events_file()
    else:
        print("\n👋 Goodbye!")
