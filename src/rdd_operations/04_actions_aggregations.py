"""
04_actions_aggregations.py
===========================

RDD Actions - Total Aggregation

Actions trigger execution and return results to driver:
- reduce(): Combine elements with function
- fold(): Like reduce but with zero value
- aggregate(): Flexible aggregation with combiners
- count(): Count elements
- sum(), mean(), stdev(): Numeric aggregations
- collect(): Return all elements (dangerous!)
- take(): Return first n elements
- top(): Return top n elements
"""

from pyspark.sql import SparkSession


def demonstrate_reduce():
    """reduce(func): Combine all elements with binary function."""
    print("=" * 80)
    print("REDUCE ACTION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("ReduceDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Sum all numbers
    numbers = sc.parallelize([1, 2, 3, 4, 5])
    total = numbers.reduce(lambda a, b: a + b)
    
    print(f"\n🔢 Numbers: {numbers.collect()}")
    print(f"🔢 Sum: {total}")
    
    # Example 2: Find maximum
    maximum = numbers.reduce(lambda a, b: max(a, b))
    print(f"🔢 Maximum: {maximum}")
    
    # Example 3: String concatenation
    words = sc.parallelize(["Hello", "World", "from", "PySpark"])
    sentence = words.reduce(lambda a, b: f"{a} {b}")
    print(f"\n📝 Words: {words.collect()}")
    print(f"📝 Sentence: {sentence}")
    
    spark.stop()


def demonstrate_fold():
    """fold(zeroValue, func): Like reduce but with initial value."""
    print("\n" + "=" * 80)
    print("FOLD ACTION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("FoldDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    numbers = sc.parallelize([1, 2, 3, 4, 5])
    
    # fold with zero value
    total = numbers.fold(0, lambda a, b: a + b)
    print(f"\n🔢 Numbers: {numbers.collect()}")
    print(f"🔢 fold(0, +): {total}")
    
    # fold with starting value
    with_bonus = numbers.fold(100, lambda a, b: a + b)
    print(f"🔢 fold(100, +): {with_bonus}")
    
    spark.stop()


def demonstrate_aggregate():
    """aggregate(): Most flexible aggregation."""
    print("\n" + "=" * 80)
    print("AGGREGATE ACTION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("AggregateDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    numbers = sc.parallelize([1, 2, 3, 4, 5], 2)  # 2 partitions
    
    # Calculate sum and count together
    def seq_op(acc, value):
        """Combine value into accumulator within partition."""
        return (acc[0] + value, acc[1] + 1)
    
    def comb_op(acc1, acc2):
        """Combine accumulators from different partitions."""
        return (acc1[0] + acc2[0], acc1[1] + acc2[1])
    
    zero_value = (0, 0)  # (sum, count)
    result = numbers.aggregate(zero_value, seq_op, comb_op)
    
    total_sum, count = result
    average = total_sum / count
    
    print(f"\n🔢 Numbers: {numbers.collect()}")
    print(f"🔢 Sum: {total_sum}, Count: {count}, Average: {average}")
    
    spark.stop()


def demonstrate_collect_variants():
    """collect(), take(), top(), first()."""
    print("\n" + "=" * 80)
    print("COLLECT VARIANTS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("CollectDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    numbers = sc.parallelize([5, 2, 8, 1, 9, 3, 7, 4, 6])
    
    # collect(): Get ALL elements (dangerous for large RDDs!)
    all_elements = numbers.collect()
    print(f"\n📊 collect() - all elements: {all_elements}")
    
    # take(n): Get first n elements
    first_3 = numbers.take(3)
    print(f"📊 take(3) - first 3: {first_3}")
    
    # top(n): Get top n elements (sorted desc)
    top_3 = numbers.top(3)
    print(f"📊 top(3) - largest 3: {top_3}")
    
    # first(): Get first element
    first = numbers.first()
    print(f"📊 first() - first element: {first}")
    
    # takeSample(withReplacement, num, seed): Random sample
    sample = numbers.takeSample(False, 3, seed=42)
    print(f"📊 takeSample(3) - random: {sample}")
    
    print("\n⚠️  Warning: collect() brings ALL data to driver - use carefully!")
    
    spark.stop()


def demonstrate_numeric_stats():
    """count(), sum(), mean(), stdev(), etc."""
    print("\n" + "=" * 80)
    print("NUMERIC STATISTICS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("StatsDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    print(f"\n📊 Numbers: {numbers.collect()}")
    print(f"📊 count(): {numbers.count()}")
    print(f"📊 sum(): {numbers.sum()}")
    print(f"📊 mean(): {numbers.mean()}")
    print(f"📊 stdev(): {numbers.stdev():.2f}")
    print(f"📊 variance(): {numbers.variance():.2f}")
    print(f"📊 max(): {numbers.max()}")
    print(f"📊 min(): {numbers.min()}")
    
    # stats(): Get all at once
    stats = numbers.stats()
    print(f"\n📈 Complete statistics:")
    print(f"   Count: {stats.count()}")
    print(f"   Mean: {stats.mean()}")
    print(f"   Stdev: {stats.stdev():.2f}")
    print(f"   Max: {stats.max()}")
    print(f"   Min: {stats.min()}")
    
    spark.stop()


def main():
    """Run all action demonstrations."""
    print("\n" + "⚡" * 40)
    print("RDD ACTIONS - AGGREGATIONS")
    print("⚡" * 40)
    
    demonstrate_reduce()
    demonstrate_fold()
    demonstrate_aggregate()
    demonstrate_collect_variants()
    demonstrate_numeric_stats()
    
    print("\n" + "=" * 80)
    print("✅ ACTIONS COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Points:")
    print("   • Actions trigger execution (transformations are lazy)")
    print("   • reduce/fold: Simple aggregations")
    print("   • aggregate: Most flexible, handles complex logic")
    print("   • collect: Returns ALL data (use with caution!)")
    print("   • take/top: Safe alternatives to collect")


if __name__ == "__main__":
    main()
