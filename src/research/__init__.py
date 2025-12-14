"""
PySpark in Research Fields
==========================

This module demonstrates how PySpark assists and accelerates research across various
scientific and academic domains.

File Structure:
• Comprehensive Examples: 01-02 (detailed, multi-example files)
• Quick Research Examples: 03-10 (standalone domain examples)
• ML Pipeline Examples: 11-16 (PySpark → Pandas → ML model training)
• All Examples Showcase: 99 (all quick examples in one file)

Key Research Applications:
1. Genomics & Bioinformatics - DNA sequencing, variant analysis, cancer genomics ML
2. Climate Science - Weather patterns, climate modeling
3. Astronomy & Astrophysics - Sky surveys, exoplanet detection
4. Social Sciences - Survey analysis, behavioral studies
5. Medical Research - Clinical trials, imaging ML, drug discovery
6. Physics - Particle collision analysis, simulations
7. Text Analytics - Literature reviews, sentiment analysis with BERT
8. Economics - Financial modeling, fraud detection, recommendation systems

Why PySpark for Research?
• Handle datasets too large for pandas/R
• Distributed processing for computationally intensive tasks
• Scalable from laptop to cluster
• Integration with scientific Python ecosystem (NumPy, SciPy, scikit-learn, PyTorch)
• ML pipeline: PySpark (big data) → Pandas (subset) → ML (PyTorch/sklearn/XGBoost)
• Reproducible analysis with documented transformations
• Cost-effective (cloud spot instances)
"""

__all__ = [
    # Comprehensive examples
    "01_genomics_bioinformatics",
    "02_climate_science",
    # Quick research domain examples
    "03_genomics_gwas",
    "04_climate_warming",
    "05_astronomy_exoplanets",
    "06_social_sciences_survey",
    "07_medical_clinical_trial",
    "08_physics_particles",
    "09_text_literature",
    "10_economics_market",
    # ML pipeline examples (PySpark → Pandas → ML)
    "11_drug_discovery_ml",
    "12_cancer_genomics_ml",
    "13_medical_imaging_ml",
    "14_recommendation_system_ml",
    "15_fraud_detection_ml",
    "16_sentiment_analysis_ml",
    # All examples showcase
    "99_all_examples_showcase",
]
