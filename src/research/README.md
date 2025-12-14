# PySpark in Scientific Research

This folder demonstrates how PySpark accelerates and enables research across various scientific domains. PySpark is increasingly used in academia and research institutions to handle datasets that are too large for traditional tools like pandas, R, or MATLAB.

## 🔬 Why PySpark for Research?

### Scale Advantages
- **Handle massive datasets**: Process terabytes to petabytes of data
- **Parallel processing**: Distribute computation across clusters
- **Cost-effective**: Use cloud spot instances for burst computation
- **Scalable**: From laptop (local mode) to massive clusters

### Research Benefits
- **Reproducible analysis**: Code-based workflows
- **Integration**: Works with scientific Python (NumPy, SciPy, pandas)
- **Fast iteration**: Interactive analysis with DataFrames
- **Open source**: No licensing fees for research labs

---

## 📂 File Structure

### Comprehensive Examples
- **`01_genomics_bioinformatics.py`** - Complete genomics research examples (458 lines)
- **`02_climate_science.py`** - Complete climate science examples (331 lines)
- **`99_all_examples_showcase.py`** - All 14 examples in one file (674 lines)

### Quick Research Domain Examples (Individual Files)
Each example is a standalone, runnable demonstration:

- **`03_genomics_gwas.py`** - Genome-Wide Association Studies (GWAS)
- **`04_climate_warming.py`** - Global temperature trend analysis
- **`05_astronomy_exoplanets.py`** - Exoplanet detection (transit method)
- **`06_social_sciences_survey.py`** - Large-scale demographic surveys
- **`07_medical_clinical_trial.py`** - Clinical trial efficacy analysis
- **`08_physics_particles.py`** - Particle collision analysis (Higgs boson)
- **`09_text_literature.py`** - PubMed publication trend analysis
- **`10_economics_market.py`** - Stock market data analysis

### Machine Learning Pipeline Examples (PySpark → Pandas → ML)
These demonstrate the complete pipeline from big data processing to ML model training:

- **`11_drug_discovery_ml.py`** - 1M compounds → PyTorch neural network
- **`12_cancer_genomics_ml.py`** - 10k genomes → sklearn Random Forest
- **`13_medical_imaging_ml.py`** - 100k images → PyTorch CNN (ResNet-50)
- **`14_recommendation_system_ml.py`** - 1B interactions → collaborative filtering
- **`15_fraud_detection_ml.py`** - 1B transactions → XGBoost classifier
- **`16_sentiment_analysis_ml.py`** - 10M reviews → BERT fine-tuning

---

## 📂 Research Field Examples

### 1. Genomics & Bioinformatics

**Files**: `01_genomics_bioinformatics.py`, `03_genomics_gwas.py`, `12_cancer_genomics_ml.py`

**Research Applications:**
- Variant calling and annotation (VCF processing)
- Genome-Wide Association Studies (GWAS)
- RNA-seq differential gene expression
- Population genomics and evolution
- Cancer risk prediction with ML

**Real-World Projects:**
- **1000 Genomes Project**: 2,504 whole genomes analyzed
- **UK Biobank**: 500,000 participants with genomic data
- **TCGA**: Cancer genome analysis (The Cancer Genome Atlas)
- **gnomAD**: Genomic variation database (125,748 exomes)
- **23andMe**: Genetic health reports

**Scale:** 
- Human genome: 3 billion base pairs
- Whole genome sequencing: 100-200 GB per individual
- Population studies: Millions of genetic variants

**Key Analyses:**
```python
# Example: Find rare disease-associated variants
rare_variants = variants_df.filter(
    (col("quality") > 90) &
    (col("population_frequency") < 0.05)
)

# GWAS: Calculate odds ratios for disease association
gwas_analysis = gwas_df.withColumn(
    "odds_ratio",
    col("cases_alt_freq") / col("controls_alt_freq")
)
```

---

### 2. Climate Science

**Files**: `02_climate_science.py`, `04_climate_warming.py`

**Research Applications:**
- Global temperature trend analysis
- Extreme weather event detection
- Precipitation and drought monitoring
- Sea level rise tracking
- Climate model validation

**Real-World Projects:**
- **NOAA Climate Data Records**: Billions of weather observations
- **NASA Earth Observing System**: Terabytes of daily satellite data
- **IPCC Climate Models (CMIP6)**: Petabytes of model output
- **ESA Climate Change Initiative**: Multi-satellite datasets

**Scale:**
- Weather stations: Billions of observations
- Satellite data: Terabytes daily (MODIS, Landsat, Sentinel)
- Climate models: Petabytes of simulation output

**Key Analyses:**
```python
# Example: Calculate global temperature trends
temp_trends = temperature_df.groupBy("decade").agg(
    avg("temperature_anomaly").alias("avg_warming")
)

# Detect extreme weather events
extreme_events = weather_df.filter(
    (col("temperature") > col("threshold_99th_percentile")) |
    (col("precipitation") > col("extreme_rainfall_threshold"))
)
```

---

### 3. Astronomy & Astrophysics

**Files**: `05_astronomy_exoplanets.py`

**Research Applications:**
- Sky survey data processing (SDSS, LSST)
- Exoplanet detection (Kepler, TESS missions)
- Galaxy classification and clustering
- Gravitational wave analysis
- Time-series analysis of variable stars

**Real-World Projects:**
- **Sloan Digital Sky Survey (SDSS)**: 500 million objects
- **Large Synoptic Survey Telescope (LSST)**: 15 TB/night
- **Kepler Mission**: 530,506 stars monitored for exoplanets
- **LIGO**: Gravitational wave data analysis

**Scale:**
- LSST: 15 terabytes per night of observation
- SDSS: 500 million astronomical objects
- Kepler: 4 years of light curves for 150,000 stars

---

### 4. Social Sciences

**Files**: `06_social_sciences_survey.py`

**Research Applications:**
- Large-scale survey analysis
- Social network analysis
- Behavioral pattern detection
- Demographic studies
- Public health epidemiology

**Real-World Projects:**
- **Census data**: National population surveys
- **Twitter/social media**: Sentiment and network analysis
- **COVID-19 tracking**: Case tracking and modeling
- **Election studies**: Polling data aggregation

**Scale:**
- Census: 300M+ individuals (US Census)
- Social media: Billions of posts/interactions
- Health records: Millions of patient records

---

### 5. Medical Research

**Files**: `07_medical_clinical_trial.py`, `13_medical_imaging_ml.py`, `11_drug_discovery_ml.py`

**Research Applications:**
- Clinical trial data analysis
- Electronic Health Record (EHR) mining
- Medical imaging analysis
- Drug discovery and pharmacovigilance
- Epidemiological studies

**Real-World Projects:**
- **UK Biobank**: 500,000 health records
- **All of Us**: NIH precision medicine (1M+ participants)
- **FDA Adverse Event Reporting**: Drug safety monitoring
- **Cancer registries**: SEER database

**Scale:**
- EHR data: Millions of patient records
- Medical imaging: Terabytes of CT/MRI scans
- Clinical trials: Thousands of patients × hundreds of variables

---

### 6. Physics Simulations

**Files**: `08_physics_particles.py`

**Research Applications:**
- Particle physics (LHC data analysis)
- Molecular dynamics simulations
- Computational fluid dynamics
- Materials science simulations

**Real-World Projects:**
- **CERN LHC**: Petabytes of collision data
- **IceCube**: Neutrino detection (1 TB/day)
- **Protein folding**: AlphaFold training data

**Scale:**
- LHC: 1 petabyte per second (filtered to ~1 PB/year stored)
- Simulations: Billions of particles, millions of timesteps

---

### 7. Text Analytics & NLP

**Files**: `09_text_literature.py`, `16_sentiment_analysis_ml.py`

**Research Applications:**
- Scientific literature mining
- Citation network analysis
- Topic modeling and trend detection
- Sentiment analysis
- Machine translation research

**Real-World Projects:**
- **PubMed**: 30M+ biomedical articles
- **arXiv**: 2M+ preprints across sciences
- **Google Scholar**: Citation network analysis
- **Common Crawl**: Web-scale text corpus

**Scale:**
- PubMed: 30+ million abstracts
- Common Crawl: Petabytes of web text
- Twitter: Hundreds of millions of tweets daily

---

### 8. Economics & Finance

**Files**: `10_economics_market.py`, `15_fraud_detection_ml.py`, `14_recommendation_system_ml.py`

**Research Applications:**
- High-frequency trading data analysis
- Econometric modeling
- Market microstructure research
- Risk modeling and stress testing
- Economic forecasting

**Real-World Projects:**
- **TAQ data**: NYSE trade and quote data
- **CRSP**: Stock market database
- **Central bank data**: Macroeconomic indicators
- **Cryptocurrency**: Blockchain transaction analysis

**Scale:**
- TAQ: Billions of trades and quotes daily
- Blockchain: Bitcoin 400GB+, Ethereum 1TB+
- Economic surveys: Millions of transactions

---

## 🎯 Common Research Patterns

### 1. **Large-Scale Data Aggregation**
```python
# Aggregate across millions of records
summary = df.groupBy("category").agg(
    count("*").alias("n"),
    avg("value").alias("mean"),
    stddev("value").alias("std")
)
```

### 2. **Time-Series Analysis**
```python
# Window functions for temporal patterns
from pyspark.sql.window import Window

window_spec = Window.partitionBy("id").orderBy("timestamp")
df_with_lag = df.withColumn("prev_value", lag("value", 1).over(window_spec))
```

### 3. **Join Large Datasets**
```python
# Join genomic data with clinical outcomes
combined = genotype_df.join(phenotype_df, "patient_id")
```

### 4. **Statistical Analysis at Scale**
```python
# Calculate correlations, p-values across large cohorts
from pyspark.ml.stat import Correlation

correlation_matrix = Correlation.corr(df, "features")
```

---

## 🚀 Getting Started with Research in PySpark

### Installation
```bash
pip install pyspark numpy pandas scipy scikit-learn
```

### Basic Research Workflow
```python
from pyspark.sql import SparkSession

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("Research") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

# 2. Load research data
df = spark.read.parquet("research_data.parquet")

# 3. Perform analysis
results = df.groupBy("condition").agg(
    avg("measurement").alias("mean"),
    count("*").alias("n")
)

# 4. Export results
results.toPandas().to_csv("results.csv")
```

---

## 📊 Research Data Formats

PySpark supports all major research data formats:

| Format | Use Case | Example |
|--------|----------|---------|
| **CSV/TSV** | Tabular data | Survey data, gene expression matrices |
| **Parquet** | Columnar storage | Large-scale genomics, climate data |
| **JSON** | Nested data | Social media, web APIs |
| **HDF5** | Scientific arrays | Astronomy images, physics simulations |
| **VCF/BCF** | Genomics | Genetic variant data |
| **FITS** | Astronomy | Telescope images |

---

## 🔧 Performance Tuning for Research

### Memory Configuration
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

### Partitioning for Large Datasets
```python
# Repartition for parallel processing
df_partitioned = df.repartition(200, "chromosome")
```

### Caching Intermediate Results
```python
# Cache frequently accessed data
df.cache()
df.count()  # Triggers caching
```

---

## 📚 Integration with Scientific Python

### NumPy/Pandas Integration
```python
# Convert to pandas for final analysis
pandas_df = spark_df.toPandas()

# Use pandas UDFs for NumPy operations
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np

@pandas_udf("double")
def numpy_calculation(s: pd.Series) -> pd.Series:
    return pd.Series(np.log(s))
```

### Machine Learning with MLlib
```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Prepare features
assembler = VectorAssembler(inputCols=["age", "bmi"], outputCol="features")
df_ml = assembler.transform(df)

# Train model
lr = LinearRegression(featuresCol="features", labelCol="disease_risk")
model = lr.fit(df_ml)
```

---

## 🏆 Success Stories

### Genomics
- **Broad Institute**: Uses Spark for GATK (Genome Analysis Toolkit)
- **23andMe**: Population genetics analysis at scale

### Climate Science
- **NOAA**: Weather forecasting and climate modeling
- **NASA**: Earth observation data processing

### Astronomy
- **LSST**: Next-generation sky survey
- **SKA**: Square Kilometre Array telescope

### Medical Research
- **NIH**: All of Us precision medicine initiative
- **UK Biobank**: Large-scale health research

---

## 📖 Additional Resources

### Official Documentation
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)

### Research-Specific Libraries
- **Hail**: Genomics data analysis on Spark
- **GeoSpark**: Geospatial data processing
- **SystemML**: Machine learning at scale

### Cloud Platforms
- **AWS EMR**: Elastic MapReduce for Spark
- **Google Cloud Dataproc**: Managed Spark clusters
- **Azure HDInsight**: Spark in Azure

---

## 🎓 Educational Use

These examples are designed for:
- **Researchers**: Learn to scale up analyses
- **Graduate students**: Modern data science skills
- **Data scientists**: Apply PySpark to domain science
- **Educators**: Teaching materials for courses

Each example includes:
- ✅ Real research questions
- ✅ Realistic (simulated) data
- ✅ Complete working code
- ✅ Research insights and interpretations
- ✅ References to actual research projects

---

## 🤝 Contributing

Research fields evolve rapidly. Contributions welcome:
- Add new research domains
- Update with latest projects/datasets
- Improve existing examples
- Share performance optimizations

---

## 📝 Citation

If you use these examples in research or teaching:

```
PySpark in Scientific Research Examples
https://github.com/[your-repo]/pyspark-coding
```

---

## ⚡ Quick Start: Run an Example

### Run Comprehensive Examples
```bash
cd src/research

# Comprehensive genomics examples (458 lines)
python3 01_genomics_bioinformatics.py

# Comprehensive climate science examples (331 lines)
python3 02_climate_science.py

# All 14 quick examples in one file
python3 99_all_examples_showcase.py
```

### Run Individual Quick Examples
```bash
# Research domain examples (03-10)
python3 03_genomics_gwas.py
python3 04_climate_warming.py
python3 05_astronomy_exoplanets.py
python3 06_social_sciences_survey.py
python3 07_medical_clinical_trial.py
python3 08_physics_particles.py
python3 09_text_literature.py
python3 10_economics_market.py

# ML pipeline examples (11-16)
python3 11_drug_discovery_ml.py
python3 12_cancer_genomics_ml.py
python3 13_medical_imaging_ml.py
python3 14_recommendation_system_ml.py
python3 15_fraud_detection_ml.py
python3 16_sentiment_analysis_ml.py
```

### Run All Examples
```bash
# Run all quick examples (03-16)
for file in {03..16}_*.py; do 
    echo "Running $file..."
    python3 "$file"
    echo "---"
done
```

---

## 🌟 Key Takeaway

**PySpark democratizes big data analysis for researchers**. What once required supercomputers or weeks of processing can now be done on modest clusters or even laptops (in local mode). This enables:

- **Faster discovery**: Iterate on hypotheses quickly
- **Larger studies**: Analyze entire populations, not samples
- **Reproducible science**: Code-based workflows
- **Collaboration**: Share analysis code across labs

**Science shouldn't be limited by data size. PySpark removes that barrier.**
