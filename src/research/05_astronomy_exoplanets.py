"""
Example 5: Astronomy - Exoplanet Detection
===========================================

Identify planets orbiting distant stars using transit method

Real-world application:
- Kepler mission (2,800+ confirmed exoplanets)
- TESS mission
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("AstronomyExoplanets").getOrCreate()
    
    try:
        print("=" * 70)
        print("ASTRONOMY: Exoplanet Detection")
        print("=" * 70)
        
        star_data = [
            ("Star-001", 1.000, "Stable"),
            ("Star-002", 0.987, "Planet transit"),
            ("Star-003", 0.999, "Stable"),
            ("Star-004", 0.982, "Planet transit"),
        ]
        
        df = spark.createDataFrame(star_data, ["star_id", "brightness", "classification"])
        planets = df.filter(col("classification") == "Planet transit")
        
        print("\n🪐 Detected Exoplanets (brightness dip during transit):")
        planets.show()
        print(f"Found {planets.count()} candidate exoplanets")
        print("✅ Real Use: Kepler mission (2,800+ confirmed exoplanets)")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
