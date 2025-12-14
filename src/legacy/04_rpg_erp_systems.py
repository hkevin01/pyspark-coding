"""
================================================================================
RPG/AS400 ERP Systems to PySpark Migration
================================================================================

PURPOSE:
--------
Modernize legacy RPG/AS400 ERP and business applications to PySpark,
enabling cloud deployment and integration with modern data ecosystems.

WHAT THIS DOES:
---------------
- Migrate RPG business logic to PySpark
- Modernize DB2/400 database access
- Order processing and inventory management
- Customer relationship management (CRM)
- Supply chain analytics

WHY MODERNIZE RPG:
------------------
BUSINESS CHALLENGES:
- AS400/IBM i hardware costs ($50K-$500K/year)
- RPG developer shortage (language from 1970s)
- Difficult integration with modern systems
- Limited scalability for growing data
- Vendor lock-in to IBM ecosystem

PYSPARK BENEFITS:
- 70% infrastructure cost reduction
- Modern Python ecosystem
- Easy cloud migration (AWS, Azure, GCP)
- Real-time analytics capabilities
- Open-source, vendor-neutral

REAL-WORLD EXAMPLES:
- Manufacturers: Production planning and MRP
- Distributors: Order fulfillment optimization
- Retailers: Inventory management
- Healthcare: Patient billing systems

KEY PATTERNS:
-------------

PATTERN 1: File Processing
---------------------------
RPG:
     F*CUSTMAST IF   E             DISK
     C     READ      CUSTMAST
     C                   DOW       NOT %EOF(CUSTMAST)
     C                   EVAL      TOTAL = TOTAL + BALANCE
     C     READ      CUSTMAST
     C                   ENDDO
     
PySpark:
  df = spark.read.csv("customer_master.csv")
  total = df.agg({"balance": "sum"}).collect()[0][0]

PATTERN 2: Business Logic
--------------------------
RPG:
     C                   IF        AMOUNT > 1000
     C                   EVAL      DISCOUNT = 0.10
     C                   ELSE
     C                   EVAL      DISCOUNT = 0.05
     C                   ENDIF
     
PySpark:
  df.withColumn("discount",
      when(col("amount") > 1000, 0.10)
      .otherwise(0.05)
  )

PERFORMANCE:
------------
RPG/AS400:
- Process 1M orders: 1 hour
- Cost: $50,000/year hardware
- Scalability: Vertical only

PySpark:
- Process 1M orders: 3 minutes (20x faster)
- Cost: $5,000/year cloud
- Scalability: Horizontal elastic

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

def create_spark_session():
    """Create Spark session for ERP."""
    return SparkSession.builder         .appName("RPG_ERP_Migration")         .getOrCreate()

def example_1_order_processing(spark):
    """
    Example 1: Order processing with business rules.
    
    RPG equivalent:
    C                   IF        QUANTITY >= 100
    C                   EVAL      UNIT_PRICE = UNIT_PRICE * 0.90
    C                   ENDIF
    C                   EVAL      LINE_TOTAL = QUANTITY * UNIT_PRICE
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: ORDER PROCESSING")
    print("=" * 80)
    
    # Sample order data
    orders = spark.createDataFrame([
        (1, 50, 10.00),
        (2, 150, 10.00),
        (3, 200, 10.00),
    ], ["order_id", "quantity", "unit_price"])
    
    # Apply volume discount (RPG IF-THEN logic)
    result = orders.withColumn("final_price",
        when(col("quantity") >= 100, col("unit_price") * 0.90)
        .otherwise(col("unit_price"))
    ).withColumn("line_total",
        col("quantity") * col("final_price")
    )
    
    print("\n✅ Orders with discounts:")
    result.show()
    
    return result

def main():
    """Main execution."""
    print("\n" + "��" * 40)
    print("RPG/AS400 ERP SYSTEMS MIGRATION")
    print("📦" * 40)
    
    spark = create_spark_session()
    example_1_order_processing(spark)
    
    print("\n✅ RPG ERP migration complete!")
    spark.stop()

if __name__ == "__main__":
    main()
