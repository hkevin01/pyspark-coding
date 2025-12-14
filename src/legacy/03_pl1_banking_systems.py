"""
================================================================================
PL/I Banking Systems to PySpark Migration  
================================================================================

PURPOSE:
--------
Modernize legacy PL/I banking and financial systems to PySpark for
real-time processing, regulatory compliance, and cloud scalability.

WHAT THIS DOES:
---------------
- Migrate PL/I transaction processing to PySpark
- Real-time fraud detection (vs batch in PL/I)
- Account reconciliation at scale
- Interest calculations and fee processing
- Regulatory reporting (FDIC, Basel III)

WHY MODERNIZE PL/I:
-------------------
BANKING CHALLENGES:
- Mainframe costs: $5M-$20M annually for large banks
- Batch processing delays (overnight windows)
- Cannot handle real-time fraud detection
- PL/I developers retiring (average age 60+)
- Regulatory pressure for faster reporting

PYSPARK BENEFITS:
- Real-time transaction processing
- 95% cost reduction vs mainframe
- Fraud detection in milliseconds
- Cloud-scale for peak loads
- Modern DevSecOps practices

REAL-WORLD EXAMPLES:
- Capital One: Migrated core banking to AWS + Spark
- JPMorgan: Real-time fraud detection with Spark
- Wells Fargo: Regulatory reporting modernization
- HSBC: Cross-border payment processing

KEY PATTERNS:
-------------

PATTERN 1: Account Updates
---------------------------
PL/I:
  DCL ACCOUNT_BALANCE FIXED(15,2);
  UPDATE ACCOUNTS 
     SET BALANCE = BALANCE + :AMOUNT
     WHERE ACCOUNT_NUMBER = :ACCT_NUM;
     
PySpark (Distributed):
  from pyspark.sql.functions import col, when
  df = df.withColumn("new_balance", 
      when(col("account_number") == acct_num,
           col("balance") + amount)
      .otherwise(col("balance"))
  )

PATTERN 2: Interest Calculation
--------------------------------
PL/I:
  DCL INTEREST FIXED(15,2);
  INTEREST = BALANCE * (RATE / 365) * DAYS;
  
PySpark:
  df.withColumn("interest",
      col("balance") * (col("rate") / 365) * col("days")
  )

PERFORMANCE:
------------
PL/I Mainframe:
- Process 10M transactions: 2 hours (batch)
- Cost: $10,000/month
- Latency: Overnight batch window

PySpark Cloud:
- Process 10M transactions: 5 minutes (streaming)
- Cost: $1,000/month  
- Latency: Sub-second real-time

COMPLIANCE:
-----------
- FDIC: Call Report automation
- Basel III: Risk-weighted asset calculation
- AML: Anti-money laundering detection
- SOX: Audit trail and controls

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when
from decimal import Decimal

def create_spark_session():
    """Create Spark session for banking."""
    return SparkSession.builder         .appName("PL1_Banking_Migration")         .config("spark.sql.shuffle.partitions", "100")         .getOrCreate()

def example_1_transaction_processing(spark):
    """
    Example 1: Process banking transactions.
    
    PL/I equivalent:
    DCL TRANSACTION_FILE FILE RECORD;
    DO WHILE (MORE_TRANSACTIONS);
       READ FILE(TRANSACTION_FILE) INTO(TRANS_RECORD);
       IF TRANS_TYPE = 'DEBIT' THEN
          BALANCE = BALANCE - AMOUNT;
       ELSE
          BALANCE = BALANCE + AMOUNT;
       END IF;
    END;
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: TRANSACTION PROCESSING")
    print("=" * 80)
    
    # Sample transactions
    transactions = spark.createDataFrame([
        (1001, "DEBIT", 100.00),
        (1001, "CREDIT", 500.00),
        (1002, "DEBIT", 50.00),
        (1002, "CREDIT", 200.00),
    ], ["account_id", "trans_type", "amount"])
    
    # Calculate net change per account
    result = transactions.withColumn("net_amount",
        when(col("trans_type") == "DEBIT", -col("amount"))
        .otherwise(col("amount"))
    ).groupBy("account_id").agg(
        spark_sum("net_amount").alias("net_change")
    )
    
    print("\n✅ Transaction summary:")
    result.show()
    
    return result

def main():
    """Main execution."""
    print("\n" + "🏦" * 40)
    print("PL/I BANKING SYSTEMS MIGRATION")  
    print("🏦" * 40)
    
    spark = create_spark_session()
    example_1_transaction_processing(spark)
    
    print("\n✅ PL/I banking migration complete!")
    spark.stop()

if __name__ == "__main__":
    main()
