"""
Example 8: Physics - Particle Collision Analysis
=================================================

Identify collision events from particle detectors

Real-world application:
- CERN LHC (40 million collisions/second)
- Higgs boson discovery
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("PhysicsParticles").getOrCreate()
    
    try:
        print("=" * 70)
        print("PHYSICS: Particle Collision Analysis")
        print("=" * 70)
        
        collision_data = [
            ("Event001", 125.3, "Higgs candidate"),
            ("Event002", 91.2, "Z boson"),
            ("Event003", 45.8, "Background"),
            ("Event004", 125.1, "Higgs candidate"),
        ]
        
        df = spark.createDataFrame(collision_data, ["event_id", "mass_GeV", "particle_type"])
        higgs = df.filter(col("particle_type") == "Higgs candidate")
        
        print("\n⚛️  Higgs Boson Candidates (mass ~125 GeV):")
        higgs.show()
        print("✅ Real Use: CERN LHC (40 million collisions/second)")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
