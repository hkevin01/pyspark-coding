"""
PySpark in Climate Science Research
====================================

Climate science generates massive datasets:
• Weather station data: Billions of observations
• Satellite data: Terabytes daily (MODIS, Landsat, Sentinel)
• Climate models: Petabytes of simulation output
• Ocean buoys: Continuous time-series data
• Ice core records: Paleoclimate data

PySpark enables climate researchers to:
1. Analyze global temperature trends
2. Process satellite imagery at scale
3. Model extreme weather events
4. Study precipitation patterns
5. Analyze ocean temperature and sea level rise
6. Predict climate change impacts

Real Research Examples:
• NOAA Climate Data Records
• NASA Earth Observing System
• IPCC Climate Models (CMIP6)
• European Space Agency Climate Change Initiative
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, stddev,
    year, month, dayofmonth, window, lag, lead, count, sum as spark_sum,
    when, expr, round as spark_round
)
from pyspark.sql.window import Window


def create_climate_spark_session():
    """Create Spark session for climate data analysis."""
    spark = SparkSession.builder \
        .appName("ClimateScience") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("=" * 70)
    print("PYSPARK IN CLIMATE SCIENCE")
    print("=" * 70)
    print("\nKey Applications:")
    print("• Global temperature trend analysis")
    print("• Extreme weather event detection")
    print("• Precipitation pattern analysis")
    print("• Sea level rise monitoring")
    print("• Carbon emission tracking")
    print("=" * 70)
    
    return spark


def example_1_global_temperature_trends(spark):
    """
    Example 1: Global Temperature Anomaly Analysis
    
    Research Question: How much has global temperature increased?
    
    Data: Temperature anomalies (deviation from baseline 1951-1980)
    Real Use Case: NOAA/NASA global temperature records
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: GLOBAL TEMPERATURE TRENDS")
    print("=" * 70)
    
    # Simulated global temperature anomalies (°C from baseline)
    # Showing clear warming trend from 1980 to 2023
    temp_data = [
        (1980, 0.26, 0.18, 0.32),
        (1985, 0.12, 0.05, 0.19),
        (1990, 0.45, 0.38, 0.52),
        (1995, 0.45, 0.39, 0.51),
        (2000, 0.42, 0.35, 0.49),
        (2005, 0.68, 0.61, 0.75),
        (2010, 0.72, 0.66, 0.78),
        (2015, 0.90, 0.84, 0.96),
        (2020, 1.02, 0.96, 1.08),
        (2023, 1.17, 1.11, 1.23),  # Record warmth
    ]
    
    temp_df = spark.createDataFrame(temp_data, 
        ["year", "temperature_anomaly", "lower_bound", "upper_bound"])
    
    print("\n🌡️  Global Temperature Anomalies (°C from 1951-1980 baseline):")
    temp_df.show()
    
    # Calculate warming rate
    print("\n📈 TEMPERATURE CHANGE ANALYSIS:")
    temp_stats = temp_df.agg(
        spark_min("temperature_anomaly").alias("min_anomaly"),
        spark_max("temperature_anomaly").alias("max_anomaly"),
        avg("temperature_anomaly").alias("avg_anomaly")
    )
    temp_stats.show()
    
    # Decade comparison
    temp_df_decades = temp_df.withColumn(
        "decade",
        (col("year") / 10).cast("int") * 10
    )
    
    print("\n📊 WARMING BY DECADE:")
    decade_avg = temp_df_decades.groupBy("decade").agg(
        avg("temperature_anomaly").alias("avg_anomaly")
    ).orderBy("decade")
    decade_avg.show()
    
    print("\n💡 Research Insight:")
    print("   • Global temperature has increased ~1.2°C since 1980")
    print("   • Warming accelerating: 2015-2023 warmest years on record")
    print("   • Consistent with IPCC projections")
    print("   • PySpark processes millions of weather station records")


def example_2_extreme_weather_events(spark):
    """
    Example 2: Extreme Weather Event Detection
    
    Research Question: Are extreme weather events increasing?
    
    Analyzes: Heat waves, cold snaps, heavy precipitation
    Real Use Case: NOAA Storm Events Database
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: EXTREME WEATHER EVENTS")
    print("=" * 70)
    
    # Simulated extreme weather events
    events_data = [
        ("2020-07-15", "Heat Wave", "California", 49.4, 8, 45000),
        ("2021-02-15", "Cold Wave", "Texas", -18.3, 5, 120000),
        ("2021-08-29", "Hurricane", "Louisiana", 240.0, 12, 250000),
        ("2022-07-20", "Heat Wave", "Europe", 46.0, 14, 85000),
        ("2022-08-15", "Drought", "Horn of Africa", 0.0, 180, 2000000),
        ("2023-07-01", "Wildfire", "Canada", 45.0, 60, 180000),
        ("2023-09-10", "Flood", "Libya", 414.0, 3, 35000),
    ]
    
    events_df = spark.createDataFrame(events_data,
        ["date", "event_type", "location", "intensity", "duration_days", "affected_people"])
    
    print("\n🌪️  Extreme Weather Events:")
    events_df.show(truncate=False)
    
    # Event frequency by type
    print("\n📊 EVENT FREQUENCY:")
    event_counts = events_df.groupBy("event_type").agg(
        count("*").alias("event_count"),
        spark_sum("affected_people").alias("total_affected"),
        avg("duration_days").alias("avg_duration")
    ).orderBy(col("total_affected").desc())
    event_counts.show(truncate=False)
    
    # High-impact events
    print("\n⚠️  HIGH-IMPACT EVENTS (>100,000 people affected):")
    high_impact = events_df.filter(col("affected_people") > 100000)
    high_impact.select("date", "event_type", "location", "affected_people").show(truncate=False)
    
    print("\n💡 Research Insight:")
    print("   • Extreme events increasing in frequency and intensity")
    print("   • Heat waves affecting millions globally")
    print("   • Climate change attribution science uses PySpark")
    print("   • Real datasets: NOAA Storm Events (millions of records)")


def example_3_precipitation_patterns(spark):
    """
    Example 3: Precipitation and Drought Analysis
    
    Research Question: How are rainfall patterns changing?
    
    Analyzes: Annual precipitation, drought indices
    Real Use Case: Global Precipitation Climatology Project (GPCP)
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: PRECIPITATION PATTERNS")
    print("=" * 70)
    
    # Simulated annual precipitation (mm) for different regions
    precip_data = [
        ("Amazon", 2020, 2850, "Wet"),
        ("Amazon", 2021, 2420, "Wet"),
        ("Amazon", 2022, 1980, "Drought"),  # Amazon drought
        ("Amazon", 2023, 2200, "Below normal"),
        
        ("Sahel", 2020, 450, "Normal"),
        ("Sahel", 2021, 380, "Drought"),
        ("Sahel", 2022, 340, "Severe drought"),
        ("Sahel", 2023, 420, "Normal"),
        
        ("Australia", 2020, 280, "Drought"),
        ("Australia", 2021, 520, "Above normal"),
        ("Australia", 2022, 650, "Flooding"),
        ("Australia", 2023, 480, "Normal"),
    ]
    
    precip_df = spark.createDataFrame(precip_data,
        ["region", "year", "annual_precip_mm", "classification"])
    
    print("\n💧 Annual Precipitation by Region:")
    precip_df.show(truncate=False)
    
    # Regional averages
    print("\n📊 REGIONAL PRECIPITATION SUMMARY:")
    regional_avg = precip_df.groupBy("region").agg(
        avg("annual_precip_mm").alias("avg_precip"),
        spark_min("annual_precip_mm").alias("min_precip"),
        spark_max("annual_precip_mm").alias("max_precip"),
        stddev("annual_precip_mm").alias("variability")
    )
    regional_avg.show(truncate=False)
    
    # Drought years
    print("\n🏜️  DROUGHT YEARS:")
    drought = precip_df.filter(col("classification").contains("Drought") | 
                                col("classification").contains("drought"))
    drought.show(truncate=False)
    
    print("\n💡 Research Insight:")
    print("   • Amazon experiencing reduced rainfall → forest stress")
    print("   • Sahel vulnerable to multi-year droughts")
    print("   • Australia swing from drought to floods (climate variability)")
    print("   • PySpark analyzes satellite precipitation data globally")


def example_4_sea_level_rise(spark):
    """
    Example 4: Sea Level Rise Monitoring
    
    Research Question: How fast are sea levels rising?
    
    Data: Satellite altimetry, tide gauge records
    Real Use Case: NASA/NOAA sea level monitoring
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: SEA LEVEL RISE")
    print("=" * 70)
    
    # Global mean sea level (mm above 1993 baseline)
    sea_level_data = [
        (1993, 0.0),
        (1995, 8.2),
        (2000, 24.5),
        (2005, 42.8),
        (2010, 61.2),
        (2015, 79.8),
        (2020, 96.5),
        (2023, 108.2),  # Current level
    ]
    
    sea_df = spark.createDataFrame(sea_level_data, ["year", "sea_level_mm"])
    
    print("\n🌊 Global Mean Sea Level Rise (mm above 1993):")
    sea_df.show()
    
    # Calculate rise rate
    window_spec = Window.orderBy("year")
    sea_df_rate = sea_df.withColumn(
        "prev_level",
        lag("sea_level_mm", 1).over(window_spec)
    ).withColumn(
        "prev_year",
        lag("year", 1).over(window_spec)
    ).withColumn(
        "rise_rate_mm_per_year",
        when(col("prev_level").isNotNull(),
             (col("sea_level_mm") - col("prev_level")) / (col("year") - col("prev_year"))
        )
    )
    
    print("\n📈 SEA LEVEL RISE RATE:")
    sea_df_rate.select("year", "sea_level_mm", "rise_rate_mm_per_year").show()
    
    # Average rate
    avg_rate = sea_df_rate.filter(col("rise_rate_mm_per_year").isNotNull()) \
        .agg(avg("rise_rate_mm_per_year").alias("avg_rate"))
    
    print("\n📊 AVERAGE RISE RATE:")
    avg_rate.show()
    
    print("\n💡 Research Insight:")
    print("   • Sea level rising ~3.4 mm/year (accelerating)")
    print("   • Total rise: ~108 mm since 1993")
    print("   • Threatens coastal cities (100M+ people)")
    print("   • PySpark processes satellite altimetry data globally")


def main():
    """Run all climate science examples."""
    spark = create_climate_spark_session()
    
    try:
        example_1_global_temperature_trends(spark)
        example_2_extreme_weather_events(spark)
        example_3_precipitation_patterns(spark)
        example_4_sea_level_rise(spark)
        
        print("\n" + "=" * 70)
        print("CLIMATE SCIENCE WITH PYSPARK - KEY TAKEAWAYS")
        print("=" * 70)
        print("\n✅ Research Applications:")
        print("   • Temperature trend analysis")
        print("   • Extreme weather detection")
        print("   • Drought and flood monitoring")
        print("   • Sea level rise tracking")
        print("   • Climate model validation")
        
        print("\n✅ Data Sources:")
        print("   • NOAA Climate Data Records")
        print("   • NASA Earth Observing System")
        print("   • European Space Agency satellites")
        print("   • Weather station networks")
        
        print("\n✅ Scale:")
        print("   • Petabytes of climate model output")
        print("   • Terabytes of daily satellite data")
        print("   • Billions of weather observations")
        
        print("\n" + "=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
