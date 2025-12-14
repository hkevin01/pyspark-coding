"""
Example 3: Genomics - GWAS Analysis
====================================

GENOMICS: Quick GWAS (Genome-Wide Association Study)
Find genetic variants associated with disease

Real-world application:
- UK Biobank (500,000 participants)
- 1000 Genomes Project
- Disease risk prediction
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    """Run genomics GWAS example."""
    spark = SparkSession.builder \
        .appName("GenomicsGWAS") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        print("=" * 70)
        print("GENOMICS: Disease Association Analysis")
        print("=" * 70)
        
        # Simulated SNP (genetic variant) data
        gwas_data = [
            ("rs123", "APOE", 850, 400, 2000, 2000),  # Strong association
            ("rs456", "BRCA1", 320, 180, 2000, 2000), # Moderate
            ("rs789", "GENE3", 500, 480, 2000, 2000), # No association
        ]
        
        df = spark.createDataFrame(gwas_data, [
            "snp_id", "gene", "cases_alt", "controls_alt", 
            "total_cases", "total_controls"
        ])
        
        # Calculate odds ratio (measure of disease association)
        result = df.withColumn(
            "cases_freq", col("cases_alt") / (col("total_cases") * 2)
        ).withColumn(
            "controls_freq", col("controls_alt") / (col("total_controls") * 2)
        ).withColumn(
            "odds_ratio", col("cases_freq") / col("controls_freq")
        )
        
        print("\n🧬 GWAS Results (Odds Ratio > 2 = strong disease association):")
        result.select("snp_id", "gene", "odds_ratio").show()
        
        print("\n✅ Real Use: UK Biobank (500,000 participants), 1000 Genomes Project")
        print("   • Identify genetic risk factors for diseases")
        print("   • Population-scale genomic studies")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
