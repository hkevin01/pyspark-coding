"""
Matplotlib & Seaborn Integration with PySpark

Visualize Spark data using Python's most popular plotting libraries.

WHAT ARE THESE LIBRARIES?
==========================
MATPLOTLIB:
- Publication-quality plots
- Low-level control
- 100+ plot types
- Foundation for other viz libraries

SEABORN:
- Statistical visualizations
- Built on matplotlib
- Beautiful default styles
- Perfect for data analysis

WHY USE WITH PYSPARK?
=====================
- Sample big data for visualization
- Statistical analysis of distributed data
- Create dashboards and reports
- Explore data before modeling
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, count
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (10, 6)

# =============================================================================
# 1. BASIC PLOTTING WITH MATPLOTLIB
# =============================================================================

def matplotlib_basics():
    """
    Basic plotting workflow: Spark → Pandas → Matplotlib
    
    WORKFLOW:
    1. Query/aggregate in Spark
    2. Sample to reasonable size
    3. Convert to Pandas
    4. Plot with Matplotlib
    """
    
    spark = SparkSession.builder \
        .appName("Matplotlib Basics") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate data
    df = spark.range(0, 10000).select(
        col("id"),
        (col("id") % 10).alias("category"),
        (rand() * 100).alias("value1"),
        (rand() * 50).alias("value2")
    )
    
    # ==========================================================================
    # Line Plot
    # ==========================================================================
    
    # Sample and convert to Pandas
    plot_data = df.limit(100).toPandas()
    
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    plt.plot(plot_data['id'], plot_data['value1'], label='Value 1', alpha=0.7)
    plt.plot(plot_data['id'], plot_data['value2'], label='Value 2', alpha=0.7)
    plt.xlabel('ID')
    plt.ylabel('Value')
    plt.title('Line Plot: Values over ID')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # ==========================================================================
    # Scatter Plot
    # ==========================================================================
    
    plt.subplot(1, 2, 2)
    plt.scatter(plot_data['value1'], plot_data['value2'], 
               c=plot_data['category'], cmap='viridis', alpha=0.6)
    plt.xlabel('Value 1')
    plt.ylabel('Value 2')
    plt.title('Scatter Plot: Value 1 vs Value 2')
    plt.colorbar(label='Category')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_matplotlib_basic.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_matplotlib_basic.png")
    plt.close()
    
    # ==========================================================================
    # Bar Plot (Aggregated Data)
    # ==========================================================================
    
    # Aggregate in Spark (efficient!)
    agg_data = df.groupBy("category").agg(
        {"value1": "avg", "value2": "avg"}
    ).toPandas()
    
    plt.figure(figsize=(10, 6))
    x = np.arange(len(agg_data))
    width = 0.35
    
    plt.bar(x - width/2, agg_data['avg(value1)'], width, label='Avg Value 1', alpha=0.8)
    plt.bar(x + width/2, agg_data['avg(value2)'], width, label='Avg Value 2', alpha=0.8)
    
    plt.xlabel('Category')
    plt.ylabel('Average Value')
    plt.title('Bar Plot: Average Values by Category')
    plt.xticks(x, agg_data['category'])
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_matplotlib_bar.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_matplotlib_bar.png")
    plt.close()
    
    spark.stop()

# =============================================================================
# 2. STATISTICAL PLOTS WITH SEABORN
# =============================================================================

def seaborn_statistical():
    """
    Statistical visualizations with Seaborn.
    
    SEABORN STRENGTHS:
    - Distribution plots
    - Relationship plots
    - Categorical plots
    - Regression plots
    """
    
    spark = SparkSession.builder \
        .appName("Seaborn Statistical") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate data
    df = spark.range(0, 5000).select(
        col("id"),
        (col("id") % 5).alias("group"),
        (rand() * 100).alias("score"),
        (rand() * 50 + col("id") % 5 * 10).alias("performance")
    )
    
    # Sample for plotting
    plot_data = df.sample(0.2).toPandas()
    
    # ==========================================================================
    # Distribution Plots
    # ==========================================================================
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Histogram
    sns.histplot(data=plot_data, x='score', bins=30, kde=True, ax=axes[0, 0])
    axes[0, 0].set_title('Distribution: Score')
    axes[0, 0].set_xlabel('Score')
    axes[0, 0].set_ylabel('Frequency')
    
    # Box Plot
    sns.boxplot(data=plot_data, x='group', y='performance', ax=axes[0, 1])
    axes[0, 1].set_title('Box Plot: Performance by Group')
    axes[0, 1].set_xlabel('Group')
    axes[0, 1].set_ylabel('Performance')
    
    # Violin Plot
    sns.violinplot(data=plot_data, x='group', y='score', ax=axes[1, 0])
    axes[1, 0].set_title('Violin Plot: Score by Group')
    axes[1, 0].set_xlabel('Group')
    axes[1, 0].set_ylabel('Score')
    
    # Scatter with regression
    sns.regplot(data=plot_data, x='score', y='performance', ax=axes[1, 1], 
                scatter_kws={'alpha': 0.3})
    axes[1, 1].set_title('Regression Plot: Score vs Performance')
    axes[1, 1].set_xlabel('Score')
    axes[1, 1].set_ylabel('Performance')
    
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_seaborn_statistical.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_seaborn_statistical.png")
    plt.close()
    
    # ==========================================================================
    # Correlation Heatmap
    # ==========================================================================
    
    # Get numeric columns
    numeric_data = plot_data[['id', 'group', 'score', 'performance']]
    correlation = numeric_data.corr()
    
    plt.figure(figsize=(8, 6))
    sns.heatmap(correlation, annot=True, cmap='coolwarm', center=0, 
                square=True, linewidths=1)
    plt.title('Correlation Heatmap')
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_seaborn_heatmap.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_seaborn_heatmap.png")
    plt.close()
    
    spark.stop()

# =============================================================================
# 3. TIME SERIES VISUALIZATION
# =============================================================================

def time_series_plots():
    """
    Visualize time series data from Spark.
    
    COMMON USE CASES:
    - Sales over time
    - User activity patterns
    - Sensor data
    - Financial data
    """
    
    spark = SparkSession.builder \
        .appName("Time Series") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate time series data
    from datetime import datetime, timedelta
    
    start_date = datetime(2024, 1, 1)
    dates = [(start_date + timedelta(days=i),) for i in range(365)]
    
    df = spark.createDataFrame(dates, ["date"]).select(
        col("date"),
        (rand() * 100 + 50).alias("sales"),
        (rand() * 20 + 30).alias("customers")
    )
    
    # Convert to Pandas
    ts_data = df.toPandas()
    ts_data['date'] = pd.to_datetime(ts_data['date'])
    ts_data = ts_data.sort_values('date')
    
    # ==========================================================================
    # Time Series Plot
    # ==========================================================================
    
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    
    # Sales over time
    axes[0].plot(ts_data['date'], ts_data['sales'], linewidth=1.5, color='steelblue')
    axes[0].fill_between(ts_data['date'], ts_data['sales'], alpha=0.3, color='steelblue')
    axes[0].set_title('Sales Over Time', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Date')
    axes[0].set_ylabel('Sales')
    axes[0].grid(True, alpha=0.3)
    
    # Customers over time
    axes[1].plot(ts_data['date'], ts_data['customers'], linewidth=1.5, color='coral')
    axes[1].fill_between(ts_data['date'], ts_data['customers'], alpha=0.3, color='coral')
    axes[1].set_title('Customers Over Time', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('Customers')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_timeseries.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_timeseries.png")
    plt.close()
    
    # ==========================================================================
    # Rolling Average
    # ==========================================================================
    
    ts_data['sales_ma7'] = ts_data['sales'].rolling(window=7).mean()
    ts_data['sales_ma30'] = ts_data['sales'].rolling(window=30).mean()
    
    plt.figure(figsize=(14, 6))
    plt.plot(ts_data['date'], ts_data['sales'], label='Daily Sales', alpha=0.4)
    plt.plot(ts_data['date'], ts_data['sales_ma7'], label='7-Day MA', linewidth=2)
    plt.plot(ts_data['date'], ts_data['sales_ma30'], label='30-Day MA', linewidth=2)
    plt.title('Sales with Moving Averages', fontsize=14, fontweight='bold')
    plt.xlabel('Date')
    plt.ylabel('Sales')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_ma.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_ma.png")
    plt.close()
    
    spark.stop()

# =============================================================================
# 4. ADVANCED SEABORN PLOTS
# =============================================================================

def advanced_seaborn():
    """
    Advanced Seaborn visualizations.
    
    FEATURES:
    - Pair plots for relationships
    - Facet grids for subplots
    - Joint plots for bivariate
    """
    
    spark = SparkSession.builder \
        .appName("Advanced Seaborn") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate multi-feature data
    df = spark.range(0, 2000).select(
        col("id"),
        (col("id") % 3).alias("category"),
        (rand() * 100).alias("feature_a"),
        (rand() * 50 + (col("id") % 3) * 20).alias("feature_b"),
        (rand() * 75).alias("feature_c")
    )
    
    # Sample for plotting
    plot_data = df.sample(0.5).toPandas()
    
    # ==========================================================================
    # Pair Plot
    # ==========================================================================
    
    print("Creating pair plot...")
    pair_plot = sns.pairplot(
        plot_data[['category', 'feature_a', 'feature_b', 'feature_c']], 
        hue='category', 
        diag_kind='kde',
        plot_kws={'alpha': 0.6}
    )
    pair_plot.fig.suptitle('Pair Plot: Feature Relationships', y=1.02, fontsize=14)
    plt.savefig('/tmp/pyspark_pairplot.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_pairplot.png")
    plt.close()
    
    # ==========================================================================
    # Joint Plot
    # ==========================================================================
    
    print("Creating joint plot...")
    joint_plot = sns.jointplot(
        data=plot_data, 
        x='feature_a', 
        y='feature_b', 
        kind='hex', 
        color='steelblue'
    )
    joint_plot.fig.suptitle('Joint Plot: Feature A vs B', y=1.02, fontsize=14)
    plt.savefig('/tmp/pyspark_jointplot.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_jointplot.png")
    plt.close()
    
    # ==========================================================================
    # Facet Grid
    # ==========================================================================
    
    print("Creating facet grid...")
    g = sns.FacetGrid(plot_data, col='category', height=4, aspect=1.2)
    g.map(sns.histplot, 'feature_a', bins=20, kde=True, color='steelblue')
    g.set_titles('Category {col_name}')
    g.set_axis_labels('Feature A', 'Count')
    g.fig.suptitle('Facet Grid: Feature A Distribution by Category', y=1.02, fontsize=14)
    plt.savefig('/tmp/pyspark_facetgrid.png', dpi=100, bbox_inches='tight')
    print("✅ Saved: /tmp/pyspark_facetgrid.png")
    plt.close()
    
    spark.stop()

# =============================================================================
# 5. BEST PRACTICES
# =============================================================================

def visualization_best_practices():
    """
    Best practices for visualizing Spark data.
    
    KEY TIPS:
    1. ALWAYS sample before plotting
    2. Aggregate in Spark (efficient!)
    3. Use appropriate sample sizes
    4. Save plots to files
    5. Use descriptive titles and labels
    """
    
    spark = SparkSession.builder \
        .appName("Viz Best Practices") \
        .master("local[*]") \
        .getOrCreate()
    
    # Simulate large dataset
    df = spark.range(0, 1_000_000)
    
    print("=" * 70)
    print("VISUALIZATION BEST PRACTICES")
    print("=" * 70)
    
    # ==========================================================================
    # DO: Sample Before Plotting
    # ==========================================================================
    
    print("\n✅ DO: Sample before plotting")
    print("-" * 70)
    
    # Good: Sample to reasonable size
    sample_size = 1000
    sample_df = df.limit(sample_size).toPandas()
    print(f"Sampled {len(sample_df)} rows for visualization")
    
    # ==========================================================================
    # DO: Aggregate in Spark
    # ==========================================================================
    
    print("\n✅ DO: Aggregate in Spark (not in Pandas)")
    print("-" * 70)
    
    # Good: Aggregate in Spark
    agg_df = df.groupBy((col("id") % 100).alias("bucket")).count()
    agg_pandas = agg_df.toPandas()
    print(f"Aggregated 1M rows → {len(agg_pandas)} groups")
    
    # ==========================================================================
    # DON'T: Collect All Data
    # ==========================================================================
    
    print("\n❌ DON'T: Collect all data to driver")
    print("-" * 70)
    print("# BAD:")
    print("# all_data = df.toPandas()  # Might crash!")
    print()
    print("# GOOD:")
    print("# sampled_data = df.sample(0.01).toPandas()  # Much safer!")
    
    # ==========================================================================
    # Recommended Sample Sizes
    # ==========================================================================
    
    print("\n📊 RECOMMENDED SAMPLE SIZES")
    print("-" * 70)
    print("Scatter plots:     1,000 - 10,000 points")
    print("Line plots:        500 - 5,000 points")
    print("Histograms:        10,000 - 100,000 points")
    print("Aggregated plots:  Any size (aggregate first!)")
    print("Heatmaps:          100 x 100 max")
    
    # ==========================================================================
    # Example: Efficient Workflow
    # ==========================================================================
    
    print("\n🎯 EXAMPLE: Efficient Workflow")
    print("-" * 70)
    print("""
    # 1. Aggregate in Spark (handles big data)
    agg_df = df.groupBy("category").agg(avg("value"), count("*"))
    
    # 2. Convert small result to Pandas
    plot_data = agg_df.toPandas()
    
    # 3. Plot with Matplotlib/Seaborn
    plt.bar(plot_data['category'], plot_data['avg(value)'])
    plt.savefig('output.png')
    """)
    print("-" * 70)
    
    print("\n" + "=" * 70)
    print("Remember: Spark for processing, Pandas for plotting!")
    print("=" * 70)
    
    spark.stop()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("MATPLOTLIB & SEABORN INTEGRATION WITH PYSPARK")
    print("=" * 70)
    
    print("\n1. Basic Plotting with Matplotlib")
    print("-" * 70)
    matplotlib_basics()
    
    print("\n2. Statistical Plots with Seaborn")
    print("-" * 70)
    seaborn_statistical()
    
    print("\n3. Time Series Visualization")
    print("-" * 70)
    time_series_plots()
    
    print("\n4. Advanced Seaborn Plots")
    print("-" * 70)
    advanced_seaborn()
    
    print("\n5. Visualization Best Practices")
    print("-" * 70)
    visualization_best_practices()
    
    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS:")
    print("=" * 70)
    print("✅ Sample data before plotting (avoid OOM)")
    print("✅ Aggregate in Spark, plot in Pandas")
    print("✅ Use Matplotlib for custom control")
    print("✅ Use Seaborn for statistical plots")
    print("✅ Save plots to files for reports")
    print("=" * 70 + "\n")
    
    print("\n📁 All plots saved to /tmp/")
    print("   - pyspark_matplotlib_basic.png")
    print("   - pyspark_matplotlib_bar.png")
    print("   - pyspark_seaborn_statistical.png")
    print("   - pyspark_seaborn_heatmap.png")
    print("   - pyspark_timeseries.png")
    print("   - pyspark_ma.png")
    print("   - pyspark_pairplot.png")
    print("   - pyspark_jointplot.png")
    print("   - pyspark_facetgrid.png")
