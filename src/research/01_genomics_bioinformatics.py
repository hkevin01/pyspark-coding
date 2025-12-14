"""
PySpark in Genomics & Bioinformatics Research
==============================================

Genomics research involves massive datasets:
• Human genome: 3 billion base pairs
• Whole genome sequencing: 100-200 GB per individual
• Population studies: Thousands of genomes
• Variant calling: Millions of genetic variants

PySpark enables researchers to:
1. Process large-scale genomic datasets
2. Identify genetic variants across populations
3. Perform genome-wide association studies (GWAS)
4. Analyze gene expression data (RNA-seq)
5. Calculate allele frequencies
6. Detect structural variants

Real Research Examples:
• 1000 Genomes Project - 2,504 genomes analyzed
• UK Biobank - 500,000 participants with genomic data
• Cancer genomics - TCGA (The Cancer Genome Atlas)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, expr,
    concat_ws, substring, length, array_contains
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType


def create_genomics_spark_session():
    """
    Create Spark session optimized for genomics data processing.
    
    Genomics-specific configurations:
    • Large shuffle partitions for variant data
    • Memory optimization for large datasets
    • Broadcast joins for reference genome lookups
    """
    spark = SparkSession.builder \
        .appName("GenomicsBioinformatics") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    print("=" * 70)
    print("PYSPARK IN GENOMICS RESEARCH")
    print("=" * 70)
    print("\nKey Applications:")
    print("• Variant calling and annotation")
    print("• Population genetics analysis")
    print("• Genome-wide association studies (GWAS)")
    print("• RNA-seq gene expression analysis")
    print("• Structural variant detection")
    print("=" * 70)
    
    return spark


def example_1_variant_calling_analysis(spark):
    """
    Example 1: Genetic Variant Analysis
    
    Research Question: Identify disease-associated genetic variants
    
    VCF (Variant Call Format) data contains:
    • Chromosome position
    • Reference allele (A, C, G, T)
    • Alternate allele (mutation)
    • Quality scores
    • Genotypes for each sample
    
    Real Use Case: Finding variants associated with disease risk
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: GENETIC VARIANT ANALYSIS")
    print("=" * 70)
    
    # Simulated VCF data - genetic variants from 1000 individuals
    # Format: CHROM, POS, REF, ALT, QUALITY, GENE, CONSEQUENCE
    variants_data = []
    
    # Disease-associated variants (simulated)
    disease_variants = [
        ("chr1", 12345, "A", "G", 95.5, "BRCA1", "missense", 0.12),  # Breast cancer
        ("chr7", 67890, "C", "T", 98.2, "CFTR", "nonsense", 0.03),   # Cystic fibrosis
        ("chr17", 43091, "G", "A", 99.1, "TP53", "missense", 0.08),  # Tumor suppressor
        ("chr4", 15602, "T", "C", 92.3, "HD", "repeat_expansion", 0.001),  # Huntington's
        ("chr21", 27659, "A", "T", 88.9, "APP", "missense", 0.02),   # Alzheimer's
    ]
    
    # Common benign variants
    benign_variants = [
        ("chr2", 98765, "G", "A", 89.1, "GENE2", "synonymous", 0.45),
        ("chr3", 11111, "C", "G", 87.5, "GENE3", "intronic", 0.38),
        ("chr5", 22222, "T", "A", 85.2, "GENE5", "synonymous", 0.52),
    ]
    
    all_variants = disease_variants + benign_variants
    
    variants_df = spark.createDataFrame(all_variants, [
        "chromosome", "position", "ref_allele", "alt_allele",
        "quality", "gene", "consequence", "population_frequency"
    ])
    
    print("\n📊 Sample Genetic Variants:")
    variants_df.show(10, truncate=False)
    
    # Analysis 1: Filter high-quality, rare variants (potential disease-causing)
    print("\n🔬 RARE, HIGH-QUALITY VARIANTS (Potential Disease Association):")
    rare_variants = variants_df.filter(
        (col("quality") > 90) &
        (col("population_frequency") < 0.05)  # Rare variant: <5% frequency
    ).select(
        "gene", "chromosome", "position", 
        "consequence", "population_frequency", "quality"
    ).orderBy(col("population_frequency"))
    
    rare_variants.show(truncate=False)
    
    # Analysis 2: Count variants by consequence type
    print("\n📈 VARIANT CONSEQUENCES (Impact on Protein):")
    consequence_summary = variants_df.groupBy("consequence").agg(
        count("*").alias("variant_count"),
        avg("quality").alias("avg_quality"),
        avg("population_frequency").alias("avg_frequency")
    ).orderBy(col("variant_count").desc())
    
    consequence_summary.show(truncate=False)
    
    # Analysis 3: Identify high-impact variants
    print("\n⚠️  HIGH-IMPACT VARIANTS (Protein-Altering):")
    high_impact = variants_df.filter(
        col("consequence").isin(["missense", "nonsense", "frameshift", "repeat_expansion"])
    ).select("gene", "consequence", "population_frequency")
    
    high_impact.show(truncate=False)
    
    print("\n💡 Research Insight:")
    print("   • Rare variants (<5% frequency) with high quality scores are")
    print("     candidates for disease association studies")
    print("   • Missense/nonsense mutations directly affect protein function")
    print("   • This analysis scales to millions of variants across populations")


def example_2_gwas_analysis(spark):
    """
    Example 2: Genome-Wide Association Study (GWAS)
    
    Research Question: Find genetic variants associated with disease phenotype
    
    GWAS Process:
    1. Genotype millions of variants in cases (disease) and controls
    2. Calculate association statistics (p-values, odds ratios)
    3. Identify significant associations (p < 5×10⁻⁸)
    4. Replicate findings in independent cohorts
    
    Real Use Case: Identifying genetic risk factors for complex diseases
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: GENOME-WIDE ASSOCIATION STUDY (GWAS)")
    print("=" * 70)
    
    # Simulated GWAS data: Variant genotypes in cases vs. controls
    # Genotypes: 0 = homozygous reference, 1 = heterozygous, 2 = homozygous alternate
    
    gwas_data = [
        # (SNP_ID, Gene, Chromosome, Cases_Alt_Allele_Count, Controls_Alt_Allele_Count, Total_Cases, Total_Controls)
        ("rs123456", "APOE", "chr19", 450, 180, 1000, 1000),  # Strong association
        ("rs234567", "BRCA1", "chr17", 120, 50, 1000, 1000),  # Moderate association
        ("rs345678", "TP53", "chr17", 85, 72, 1000, 1000),    # Weak association
        ("rs456789", "GENE4", "chr4", 200, 195, 1000, 1000),  # No association
        ("rs567890", "CFTR", "chr7", 65, 15, 1000, 1000),     # Strong association
        ("rs678901", "HTT", "chr4", 5, 3, 1000, 1000),        # Very rare
    ]
    
    gwas_df = spark.createDataFrame(gwas_data, [
        "snp_id", "gene", "chromosome", 
        "cases_alt_count", "controls_alt_count",
        "total_cases", "total_controls"
    ])
    
    print("\n📊 GWAS Data (Disease Cases vs. Healthy Controls):")
    gwas_df.show(truncate=False)
    
    # Calculate allele frequencies and odds ratios
    gwas_analysis = gwas_df.withColumn(
        "cases_alt_freq",
        col("cases_alt_count") / (col("total_cases") * 2)  # Diploid
    ).withColumn(
        "controls_alt_freq",
        col("controls_alt_count") / (col("total_controls") * 2)
    ).withColumn(
        "frequency_diff",
        col("cases_alt_freq") - col("controls_alt_freq")
    ).withColumn(
        "odds_ratio_approx",
        when(col("controls_alt_freq") > 0,
             col("cases_alt_freq") / col("controls_alt_freq")
        ).otherwise(None)
    )
    
    print("\n🔬 GWAS ANALYSIS RESULTS:")
    print("   Allele frequency differences between cases and controls")
    print("   Odds Ratio > 2.0 indicates strong disease association\n")
    
    gwas_analysis.select(
        "snp_id", "gene", 
        "cases_alt_freq", "controls_alt_freq", 
        "frequency_diff", "odds_ratio_approx"
    ).orderBy(col("frequency_diff").desc()).show(truncate=False)
    
    # Identify significant associations
    print("\n🎯 SIGNIFICANT ASSOCIATIONS (Odds Ratio ≥ 2.0):")
    significant = gwas_analysis.filter(col("odds_ratio_approx") >= 2.0)
    significant.select("snp_id", "gene", "odds_ratio_approx").show(truncate=False)
    
    print("\n💡 Research Insight:")
    print("   • APOE shows strong association (OR = 2.5) - known Alzheimer's risk")
    print("   • CFTR shows strong association (OR = 4.3) - cystic fibrosis risk")
    print("   • Real GWAS analyze millions of SNPs; Spark handles the scale")
    print("   • Multiple testing correction required (Bonferroni, FDR)")


def example_3_rna_seq_gene_expression(spark):
    """
    Example 3: RNA-Seq Gene Expression Analysis
    
    Research Question: Which genes are differentially expressed in disease?
    
    RNA-Seq measures gene expression levels:
    • Count reads mapping to each gene
    • Normalize for sequencing depth and gene length
    • Compare expression between conditions
    • Identify differentially expressed genes (DEGs)
    
    Real Use Case: Cancer research - tumor vs. normal tissue
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: RNA-SEQ GENE EXPRESSION ANALYSIS")
    print("=" * 70)
    
    # Simulated RNA-seq data: Gene expression in tumor vs. normal samples
    # TPM = Transcripts Per Million (normalized expression)
    
    expression_data = [
        # (Gene, Normal_TPM, Tumor_TPM, Chromosome, Function)
        ("TP53", 150.5, 12.3, "chr17", "Tumor suppressor"),      # Downregulated
        ("MYC", 45.2, 320.8, "chr8", "Oncogene"),                # Upregulated
        ("BRCA1", 89.1, 25.4, "chr17", "DNA repair"),            # Downregulated
        ("EGFR", 60.3, 285.7, "chr7", "Growth factor receptor"), # Upregulated
        ("PTEN", 110.4, 18.9, "chr10", "Tumor suppressor"),      # Downregulated
        ("KRAS", 72.5, 198.4, "chr12", "Signaling protein"),     # Upregulated
        ("GAPDH", 500.2, 502.1, "chr12", "Housekeeping"),        # No change
        ("ACTB", 450.8, 448.3, "chr7", "Housekeeping"),          # No change
    ]
    
    expression_df = spark.createDataFrame(expression_data, [
        "gene", "normal_tpm", "tumor_tpm", "chromosome", "function"
    ])
    
    print("\n📊 Gene Expression Data (Tumor vs. Normal):")
    expression_df.show(truncate=False)
    
    # Calculate fold changes and log2 fold changes
    expression_analysis = expression_df.withColumn(
        "fold_change",
        col("tumor_tpm") / col("normal_tpm")
    ).withColumn(
        "log2_fold_change",
        expr("log2(tumor_tpm / normal_tpm)")
    ).withColumn(
        "expression_status",
        when(col("fold_change") >= 2.0, "Upregulated")
        .when(col("fold_change") <= 0.5, "Downregulated")
        .otherwise("No Change")
    )
    
    print("\n🔬 DIFFERENTIAL EXPRESSION ANALYSIS:")
    print("   Fold Change ≥ 2.0 = Upregulated in tumor")
    print("   Fold Change ≤ 0.5 = Downregulated in tumor\n")
    
    expression_analysis.select(
        "gene", "function", "normal_tpm", "tumor_tpm",
        "fold_change", "log2_fold_change", "expression_status"
    ).orderBy(col("log2_fold_change").desc()).show(truncate=False)
    
    # Group by expression status
    print("\n📈 EXPRESSION STATUS SUMMARY:")
    status_summary = expression_analysis.groupBy("expression_status").agg(
        count("*").alias("gene_count")
    )
    status_summary.show(truncate=False)
    
    # Identify oncogenes (upregulated) and tumor suppressors (downregulated)
    print("\n🎯 CANCER-RELEVANT GENES:")
    print("\nONCOGENES (Upregulated):")
    expression_analysis.filter(
        (col("expression_status") == "Upregulated") &
        (col("function").contains("Oncogene") | col("function").contains("Growth"))
    ).select("gene", "fold_change", "function").show(truncate=False)
    
    print("TUMOR SUPPRESSORS (Downregulated):")
    expression_analysis.filter(
        (col("expression_status") == "Downregulated") &
        col("function").contains("Tumor suppressor")
    ).select("gene", "fold_change", "function").show(truncate=False)
    
    print("\n💡 Research Insight:")
    print("   • Upregulated oncogenes (MYC, EGFR, KRAS) drive tumor growth")
    print("   • Downregulated tumor suppressors (TP53, BRCA1, PTEN) lose function")
    print("   • Real RNA-seq data: 20,000+ genes × 100+ samples")
    print("   • PySpark enables analysis at scale across large patient cohorts")


def example_4_population_genomics(spark):
    """
    Example 4: Population Genomics - Allele Frequency Analysis
    
    Research Question: How do genetic variants differ across populations?
    
    Population genomics studies:
    • Allele frequencies across populations
    • Genetic diversity and structure
    • Selective pressures and evolution
    • Migration patterns
    
    Real Use Case: 1000 Genomes Project - characterizing human genetic variation
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: POPULATION GENOMICS")
    print("=" * 70)
    
    # Simulated population genomics data
    # Different populations: African, European, Asian, American
    
    population_data = [
        # (Variant_ID, Gene, AFR_Freq, EUR_Freq, EAS_Freq, AMR_Freq, Global_Freq)
        ("rs334", "HBB", 0.15, 0.001, 0.001, 0.01, 0.04),      # Sickle cell (malaria resistance)
        ("rs1815739", "ACTN3", 0.75, 0.55, 0.95, 0.60, 0.71),  # Athletic performance
        ("rs4988235", "LCT", 0.10, 0.75, 0.05, 0.45, 0.34),    # Lactase persistence
        ("rs1426654", "SLC24A5", 0.02, 0.98, 0.65, 0.50, 0.54), # Skin pigmentation
        ("rs7412", "APOE", 0.20, 0.08, 0.12, 0.10, 0.12),      # Alzheimer's risk
        ("rs1799883", "FABP2", 0.35, 0.25, 0.45, 0.30, 0.34),  # Metabolic traits
    ]
    
    pop_df = spark.createDataFrame(population_data, [
        "variant_id", "gene", 
        "african_freq", "european_freq", "east_asian_freq", 
        "american_freq", "global_freq"
    ])
    
    print("\n🌍 Allele Frequencies Across Populations:")
    print("   AFR = African, EUR = European, EAS = East Asian, AMR = American\n")
    pop_df.show(truncate=False)
    
    # Calculate population differentiation (variance in frequencies)
    pop_analysis = pop_df.withColumn(
        "freq_variance",
        expr("""
            (pow(african_freq - global_freq, 2) +
             pow(european_freq - global_freq, 2) +
             pow(east_asian_freq - global_freq, 2) +
             pow(american_freq - global_freq, 2)) / 4
        """)
    ).withColumn(
        "max_freq_diff",
        expr("""
            greatest(african_freq, european_freq, east_asian_freq, american_freq) -
            least(african_freq, european_freq, east_asian_freq, american_freq)
        """)
    )
    
    print("\n🔬 POPULATION DIFFERENTIATION ANALYSIS:")
    print("   High variance = strong population differences (natural selection)\n")
    
    pop_analysis.select(
        "variant_id", "gene", "global_freq",
        "freq_variance", "max_freq_diff"
    ).orderBy(col("freq_variance").desc()).show(truncate=False)
    
    # Identify population-specific variants
    print("\n🎯 POPULATION-SPECIFIC VARIANTS:")
    
    print("\nHigh in African populations:")
    pop_df.filter(col("african_freq") > col("global_freq") + 0.1).select(
        "variant_id", "gene", "african_freq", "global_freq"
    ).show(truncate=False)
    
    print("High in European populations:")
    pop_df.filter(col("european_freq") > col("global_freq") + 0.2).select(
        "variant_id", "gene", "european_freq", "global_freq"
    ).show(truncate=False)
    
    print("\n�� Research Insight:")
    print("   • rs334 (HBB): High in Africa due to malaria resistance")
    print("   • rs4988235 (LCT): High in Europe - lactase persistence")
    print("   • rs1426654 (SLC24A5): High in Europe - light skin pigmentation")
    print("   • These patterns reveal human evolution and adaptation")
    print("   • PySpark enables analysis of millions of variants across populations")


def main():
    """Run all genomics research examples."""
    spark = create_genomics_spark_session()
    
    try:
        # Example 1: Variant calling and annotation
        example_1_variant_calling_analysis(spark)
        
        # Example 2: Genome-wide association study
        example_2_gwas_analysis(spark)
        
        # Example 3: RNA-seq gene expression
        example_3_rna_seq_gene_expression(spark)
        
        # Example 4: Population genomics
        example_4_population_genomics(spark)
        
        print("\n" + "=" * 70)
        print("GENOMICS RESEARCH WITH PYSPARK - KEY TAKEAWAYS")
        print("=" * 70)
        print("\n✅ Scale Advantages:")
        print("   • Process millions of variants across thousands of genomes")
        print("   • Analyze whole-genome sequencing data (100-200 GB/individual)")
        print("   • Parallel processing reduces analysis time from days to hours")
        
        print("\n✅ Research Applications:")
        print("   • Variant calling and annotation")
        print("   • Genome-wide association studies (GWAS)")
        print("   • RNA-seq differential expression")
        print("   • Population genetics and evolution")
        print("   • Cancer genomics (tumor vs. normal)")
        
        print("\n✅ Real-World Projects Using Spark for Genomics:")
        print("   • 1000 Genomes Project - population variation")
        print("   • UK Biobank - 500,000 participants")
        print("   • TCGA (The Cancer Genome Atlas) - cancer research")
        print("   • gnomAD - genomic variation database")
        
        print("\n✅ Integration with Genomics Tools:")
        print("   • GATK (Genome Analysis Toolkit) - variant calling")
        print("   • VCF/BCF file formats - standard genomics data")
        print("   • Bioconductor packages - statistical genomics")
        print("   • PLINK format - genetic association studies")
        
        print("\n" + "=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
