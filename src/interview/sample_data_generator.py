"""
================================================================================
Sample Data Generator for ETL Practice
================================================================================

PURPOSE: Generate realistic sample datasets for practicing ETL pipelines.

Creates:
- customers.csv: Customer master data
- orders.csv: Transaction data
- products.csv: Product catalog

Run this first to create practice data!
================================================================================
"""

import csv
import random
from datetime import datetime, timedelta
import os


def generate_customers(num_customers=100):
    """Generate customer master data."""
    first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 
                   'Henry', 'Iris', 'Jack', 'Kate', 'Leo', 'Mary', 'Nick', 'Olivia']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 
                  'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Anderson', 'Taylor']
    tiers = ['Premium', 'Standard', 'Basic']
    
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': i,
            'name': f"{random.choice(first_names)} {random.choice(last_names)}",
            'email': f"customer{i}@email.com",
            'tier': random.choice(tiers),
            'join_date': (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d')
        }
        customers.append(customer)
    
    # Add some duplicates for data quality practice
    if len(customers) > 10:
        customers.append(customers[5].copy())
        customers.append(customers[10].copy())
    
    return customers


def generate_orders(num_orders=500, num_customers=100):
    """Generate order transaction data."""
    orders = []
    order_id = 1
    
    for _ in range(num_orders):
        order = {
            'order_id': order_id,
            'customer_id': random.randint(1, num_customers),
            'product_id': random.randint(1, 50),
            'quantity': random.randint(1, 10),
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'order_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
        }
        orders.append(order)
        order_id += 1
    
    # Add some data quality issues
    # Duplicate orders
    if len(orders) > 20:
        orders.append(orders[15].copy())
        orders.append(orders[20].copy())
    
    # Orders with missing customer_id
    orders.append({
        'order_id': order_id,
        'customer_id': None,
        'product_id': 10,
        'quantity': 1,
        'amount': 50.0,
        'order_date': datetime.now().strftime('%Y-%m-%d')
    })
    
    return orders


def generate_products(num_products=50):
    """Generate product catalog data."""
    categories = ['Electronics', 'Furniture', 'Clothing', 'Books', 'Food', 'Toys']
    product_names = [
        'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Desk', 'Chair', 
        'T-Shirt', 'Jeans', 'Shoes', 'Novel', 'Cookbook', 'Coffee',
        'Tablet', 'Headphones', 'Speaker', 'Lamp', 'Bookshelf'
    ]
    
    products = []
    for i in range(1, num_products + 1):
        product = {
            'product_id': i,
            'product_name': f"{random.choice(product_names)} #{i}",
            'category': random.choice(categories),
            'unit_price': round(random.uniform(5.0, 999.99), 2)
        }
        products.append(product)
    
    return products


def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file."""
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Generated: {filename} ({len(data)} records)")


def main():
    """Generate all sample datasets."""
    print("\n" + "="*80)
    print("SAMPLE DATA GENERATOR FOR ETL PRACTICE")
    print("="*80 + "\n")
    
    # Create output directory
    output_dir = "/tmp/etl_practice_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate datasets
    print("📊 Generating sample data...\n")
    
    customers = generate_customers(100)
    save_to_csv(
        customers, 
        f"{output_dir}/customers.csv",
        ['customer_id', 'name', 'email', 'tier', 'join_date']
    )
    
    orders = generate_orders(500, 100)
    save_to_csv(
        orders,
        f"{output_dir}/orders.csv",
        ['order_id', 'customer_id', 'product_id', 'quantity', 'amount', 'order_date']
    )
    
    products = generate_products(50)
    save_to_csv(
        products,
        f"{output_dir}/products.csv",
        ['product_id', 'product_name', 'category', 'unit_price']
    )
    
    print(f"\n🎉 Sample data generated in: {output_dir}")
    print("\n💡 Data quality issues included (for practice):")
    print("   • Duplicate customer records")
    print("   • Duplicate orders")
    print("   • Orders with missing customer_id")
    print("\n🚀 Now run: python etl_practice_gui.py")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
