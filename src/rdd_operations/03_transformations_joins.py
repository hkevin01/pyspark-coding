"""
03_transformations_joins.py
============================

RDD Join Transformations

Join types for Pair RDDs (key-value pairs):
- join(): Inner join (returns matching pairs)
- leftOuterJoin(): Left outer join
- rightOuterJoin(): Right outer join
- fullOuterJoin(): Full outer join
- cogroup(): Group both RDDs by key
"""

from pyspark.sql import SparkSession


def demonstrate_inner_join():
    """Inner join - only matching keys."""
    print("=" * 80)
    print("INNER JOIN")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("InnerJoinDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example: Join users with their orders
    users = sc.parallelize([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David")
    ])
    
    orders = sc.parallelize([
        (1, "Order_A"),  # Alice's order
        (2, "Order_B"),  # Bob's order
        (2, "Order_C"),  # Bob's second order
        (5, "Order_D")   # Unknown user
    ])
    
    # Inner join: only keys present in BOTH RDDs
    joined = users.join(orders)
    
    print("\n👥 Users:")
    for uid, name in users.collect():
        print(f"   {uid}: {name}")
    
    print("\n📦 Orders:")
    for uid, order in orders.collect():
        print(f"   User {uid}: {order}")
    
    print("\n🔗 Inner Join (user_id, (name, order)):")
    for uid, (name, order) in joined.collect():
        print(f"   {uid}: {name} → {order}")
    
    print("\n💡 Note: User 3 (Charlie) has no orders - excluded")
    print("💡 Note: User 5 order has no user - excluded")
    
    spark.stop()


def demonstrate_left_outer_join():
    """Left outer join - all keys from left RDD."""
    print("\n" + "=" * 80)
    print("LEFT OUTER JOIN")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("LeftJoinDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    users = sc.parallelize([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),  # No orders
    ])
    
    orders = sc.parallelize([
        (1, "Order_A"),
        (2, "Order_B"),
    ])
    
    # Left join: ALL left keys, matched right values or None
    left_joined = users.leftOuterJoin(orders)
    
    print("\n🔗 Left Outer Join (keep all users):")
    for uid, (name, order) in left_joined.collect():
        order_str = order if order else "No orders"
        print(f"   {uid}: {name} → {order_str}")
    
    spark.stop()


def demonstrate_right_outer_join():
    """Right outer join - all keys from right RDD."""
    print("\n" + "=" * 80)
    print("RIGHT OUTER JOIN")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("RightJoinDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    users = sc.parallelize([
        (1, "Alice"),
        (2, "Bob"),
    ])
    
    orders = sc.parallelize([
        (1, "Order_A"),
        (2, "Order_B"),
        (5, "Order_C"),  # Unknown user
    ])
    
    # Right join: ALL right keys, matched left values or None
    right_joined = users.rightOuterJoin(orders)
    
    print("\n🔗 Right Outer Join (keep all orders):")
    for uid, (name, order) in right_joined.collect():
        name_str = name if name else "Unknown User"
        print(f"   {uid}: {name_str} → {order}")
    
    spark.stop()


def demonstrate_full_outer_join():
    """Full outer join - all keys from both RDDs."""
    print("\n" + "=" * 80)
    print("FULL OUTER JOIN")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("FullJoinDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    users = sc.parallelize([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),  # No orders
    ])
    
    orders = sc.parallelize([
        (1, "Order_A"),
        (2, "Order_B"),
        (5, "Order_C"),  # Unknown user
    ])
    
    # Full join: ALL keys from both, None for missing matches
    full_joined = users.fullOuterJoin(orders)
    
    print("\n🔗 Full Outer Join (keep everything):")
    for uid, (name, order) in full_joined.collect():
        name_str = name if name else "Unknown"
        order_str = order if order else "No orders"
        print(f"   {uid}: {name_str} → {order_str}")
    
    spark.stop()


def demonstrate_cogroup():
    """Cogroup - group both RDDs by key."""
    print("\n" + "=" * 80)
    print("COGROUP TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("CogroupDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    users = sc.parallelize([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
    ])
    
    orders = sc.parallelize([
        (1, "Order_A"),
        (1, "Order_B"),  # Alice has 2 orders
        (2, "Order_C"),
    ])
    
    # Cogroup: Groups all values by key from both RDDs
    grouped = users.cogroup(orders)
    
    print("\n🔗 Cogroup Result (key, (users_iter, orders_iter)):")
    for key, (users_iter, orders_iter) in grouped.collect():
        users_list = list(users_iter)
        orders_list = list(orders_iter)
        print(f"\n   Key {key}:")
        print(f"      Users: {users_list}")
        print(f"      Orders: {orders_list}")
    
    spark.stop()


def main():
    """Run all join demonstrations."""
    print("\n" + "🔗" * 40)
    print("RDD JOIN TRANSFORMATIONS")
    print("🔗" * 40)
    
    demonstrate_inner_join()
    demonstrate_left_outer_join()
    demonstrate_right_outer_join()
    demonstrate_full_outer_join()
    demonstrate_cogroup()
    
    print("\n" + "=" * 80)
    print("✅ JOIN TRANSFORMATIONS COMPLETE")
    print("=" * 80)
    
    print("\n📚 Join Type Summary:")
    print("   • join(): Inner join - only matching keys")
    print("   • leftOuterJoin(): All left keys + matched right")
    print("   • rightOuterJoin(): All right keys + matched left")
    print("   • fullOuterJoin(): All keys from both")
    print("   • cogroup(): Group all values by key")


if __name__ == "__main__":
    main()
