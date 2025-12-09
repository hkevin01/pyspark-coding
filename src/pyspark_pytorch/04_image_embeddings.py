"""
Example 4: Image Embeddings with PyTorch CNN and PySpark Aggregation
=====================================================================
Demonstrates extracting image embeddings with PyTorch and processing with PySpark.
"""

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from pyspark.sql import SparkSession
from pyspark.sql import functions as PF
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField, IntegerType, StringType
import io

print("=" * 80)
print("IMAGE EMBEDDINGS: PYTORCH CNN + PYSPARK PROCESSING")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("ImageEmbeddings") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# 1. DEFINE CNN FOR FEATURE EXTRACTION
# ============================================================================
print("\n1. DEFINE CNN FOR FEATURE EXTRACTION")
print("-" * 80)

class SimpleImageEncoder(nn.Module):
    """Simple CNN for extracting image embeddings"""
    def __init__(self, embedding_dim=128):
        super(SimpleImageEncoder, self).__init__()
        # Assuming 28x28 grayscale images (like MNIST)
        self.conv1 = nn.Conv2d(1, 32, kernel_size=3, padding=1)
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.conv3 = nn.Conv2d(64, 128, kernel_size=3, padding=1)
        
        self.pool = nn.MaxPool2d(2, 2)
        
        # After 3 pooling layers: 28 -> 14 -> 7 -> 3 (with padding adjustments)
        self.fc1 = nn.Linear(128 * 3 * 3, 256)
        self.fc2 = nn.Linear(256, embedding_dim)
        
        self.dropout = nn.Dropout(0.3)
    
    def forward(self, x):
        # Input: (batch, 1, 28, 28)
        x = self.pool(F.relu(self.conv1(x)))  # (batch, 32, 14, 14)
        x = self.pool(F.relu(self.conv2(x)))  # (batch, 64, 7, 7)
        x = self.pool(F.relu(self.conv3(x)))  # (batch, 128, 3, 3)
        
        x = x.view(-1, 128 * 3 * 3)
        x = F.relu(self.fc1(x))
        x = self.dropout(x)
        embedding = self.fc2(x)  # (batch, embedding_dim)
        
        return embedding

# Create model
embedding_dim = 128
encoder = SimpleImageEncoder(embedding_dim=embedding_dim)
encoder.eval()

print(f"Model architecture:")
print(encoder)
print(f"\nEmbedding dimension: {embedding_dim}")

# ============================================================================
# 2. CREATE SYNTHETIC IMAGE DATASET
# ============================================================================
print("\n2. CREATE SYNTHETIC IMAGE DATASET")
print("-" * 80)

def create_synthetic_images(n_images=100):
    """Create synthetic images with labels"""
    torch.manual_seed(42)
    np.random.seed(42)
    
    data = []
    for i in range(n_images):
        # Create random 28x28 image
        image = torch.randn(1, 28, 28)
        
        # Assign random category
        category = np.random.choice(['cat', 'dog', 'bird'])
        category_id = {'cat': 0, 'dog': 1, 'bird': 2}[category]
        
        # Convert image to bytes (simulating stored images)
        image_array = image.numpy().flatten().tolist()
        
        data.append((i, category, category_id, image_array))
    
    return data

# Create dataset
data = create_synthetic_images(n_images=500)

schema = StructType([
    StructField("image_id", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("category_id", IntegerType(), False),
    StructField("image_data", ArrayType(FloatType()), False)
])

df_images = spark.createDataFrame(data, schema)

print(f"Dataset size: {df_images.count()} images")
df_images.show(10)

print("\nCategory distribution:")
df_images.groupBy("category").count().show()

# ============================================================================
# 3. EXTRACT EMBEDDINGS USING PYTORCH
# ============================================================================
print("\n3. EXTRACT EMBEDDINGS USING PYTORCH")
print("-" * 80)

from pyspark.sql.functions import pandas_udf
import pandas as pd

# Broadcast model parameters
model_state = encoder.state_dict()
broadcast_encoder = spark.sparkContext.broadcast(model_state)

@pandas_udf(ArrayType(FloatType()))
def extract_embeddings_udf(image_data: pd.Series) -> pd.Series:
    """Extract image embeddings using CNN"""
    # Load model
    model = SimpleImageEncoder(embedding_dim=128)
    model.load_state_dict(broadcast_encoder.value)
    model.eval()
    
    # Process batch of images
    images_list = []
    for img_flat in image_data:
        # Reshape flat array back to image
        img_array = np.array(img_flat).reshape(1, 28, 28)
        images_list.append(img_array)
    
    # Stack into batch
    images_batch = np.array(images_list)
    images_tensor = torch.from_numpy(images_batch).float()
    
    # Extract embeddings
    with torch.no_grad():
        embeddings = model(images_tensor)
    
    # Convert to list format
    embeddings_list = embeddings.numpy().tolist()
    
    return pd.Series(embeddings_list)

# Apply UDF to extract embeddings
df_with_embeddings = df_images.withColumn(
    "embedding",
    extract_embeddings_udf(PF.col("image_data"))
)

print("Images with embeddings:")
df_with_embeddings.select("image_id", "category", "embedding").show(5, truncate=True)

# ============================================================================
# 4. CALCULATE EMBEDDING STATISTICS WITH PYSPARK
# ============================================================================
print("\n4. CALCULATE EMBEDDING STATISTICS WITH PYSPARK")
print("-" * 80)

# Calculate mean embedding per category
def calculate_mean_embedding(embeddings):
    """Calculate mean of embedding vectors"""
    embeddings_array = np.array(embeddings)
    return embeddings_array.mean(axis=0).tolist()

from pyspark.sql.functions import collect_list, udf

mean_embedding_udf = udf(calculate_mean_embedding, ArrayType(FloatType()))

category_embeddings = df_with_embeddings.groupBy("category") \
    .agg(
        PF.count("*").alias("count"),
        collect_list("embedding").alias("all_embeddings")
    ) \
    .withColumn("mean_embedding", mean_embedding_udf(PF.col("all_embeddings"))) \
    .drop("all_embeddings")

print("Mean embeddings per category:")
category_embeddings.show(truncate=False)

# ============================================================================
# 5. CALCULATE PAIRWISE SIMILARITIES
# ============================================================================
print("\n5. CALCULATE PAIRWISE SIMILARITIES")
print("-" * 80)

def cosine_similarity_arrays(arr1, arr2):
    """Calculate cosine similarity between two arrays"""
    if arr1 is None or arr2 is None:
        return 0.0
    a = np.array(arr1)
    b = np.array(arr2)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

cosine_sim_udf = udf(cosine_similarity_arrays, FloatType())

# Self-join to calculate similarities
df_pairs = df_with_embeddings.alias("a").crossJoin(
    df_with_embeddings.alias("b")
).filter(PF.col("a.image_id") < PF.col("b.image_id"))

df_similarities = df_pairs.withColumn(
    "similarity",
    cosine_sim_udf(PF.col("a.embedding"), PF.col("b.embedding"))
).select(
    PF.col("a.image_id").alias("image1"),
    PF.col("a.category").alias("category1"),
    PF.col("b.image_id").alias("image2"),
    PF.col("b.category").alias("category2"),
    "similarity"
)

print("Top 20 most similar image pairs:")
df_similarities.orderBy(PF.desc("similarity")).show(20)

# ============================================================================
# 6. FIND NEAREST NEIGHBORS
# ============================================================================
print("\n6. FIND NEAREST NEIGHBORS")
print("-" * 80)

# For a specific image, find its K nearest neighbors
target_image_id = 0

target_embedding = df_with_embeddings \
    .filter(PF.col("image_id") == target_image_id) \
    .select("embedding", "category") \
    .first()

print(f"Finding nearest neighbors for image {target_image_id} (category: {target_embedding.category})")

# Calculate similarity with all other images
df_neighbors = df_with_embeddings \
    .filter(PF.col("image_id") != target_image_id) \
    .withColumn(
        "similarity",
        cosine_sim_udf(PF.lit(target_embedding.embedding), PF.col("embedding"))
    ) \
    .orderBy(PF.desc("similarity")) \
    .select("image_id", "category", "similarity") \
    .limit(10)

print(f"\nTop 10 nearest neighbors:")
df_neighbors.show()

# ============================================================================
# 7. CLUSTERING ANALYSIS
# ============================================================================
print("\n7. CLUSTERING ANALYSIS")
print("-" * 80)

# Calculate within-category and cross-category similarities
within_category_sim = df_similarities \
    .filter(PF.col("category1") == PF.col("category2")) \
    .agg(
        PF.avg("similarity").alias("avg_within_similarity"),
        PF.stddev("similarity").alias("std_within_similarity")
    )

cross_category_sim = df_similarities \
    .filter(PF.col("category1") != PF.col("category2")) \
    .agg(
        PF.avg("similarity").alias("avg_cross_similarity"),
        PF.stddev("similarity").alias("std_cross_similarity")
    )

print("Within-category similarity (same class):")
within_category_sim.show()

print("Cross-category similarity (different classes):")
cross_category_sim.show()

# Per-category pair analysis
print("\nPer-category pair similarity:")
df_similarities.groupBy("category1", "category2") \
    .agg(
        PF.count("*").alias("pairs"),
        PF.avg("similarity").alias("avg_similarity"),
        PF.min("similarity").alias("min_similarity"),
        PF.max("similarity").alias("max_similarity")
    ) \
    .orderBy("category1", "category2") \
    .show()

# ============================================================================
# 8. DIMENSIONALITY REDUCTION WITH PYSPARK
# ============================================================================
print("\n8. DIMENSIONALITY REDUCTION WITH PYSPARK")
print("-" * 80)

# Calculate PCA-like statistics (simplified)
def calculate_variance(embeddings_list):
    """Calculate variance for each embedding dimension"""
    embeddings_array = np.array(embeddings_list)
    variances = np.var(embeddings_array, axis=0)
    return variances.tolist()

variance_udf = udf(calculate_variance, ArrayType(FloatType()))

embedding_stats = df_with_embeddings.agg(
    collect_list("embedding").alias("all_embeddings")
).withColumn("variances", variance_udf(PF.col("all_embeddings")))

print("Embedding dimension variances:")
variance_row = embedding_stats.first()
variances = variance_row.variances[:10]  # Show first 10
print(f"First 10 dimensions: {[f'{v:.4f}' for v in variances]}")

# ============================================================================
# 9. SAVE EMBEDDINGS FOR DOWNSTREAM USE
# ============================================================================
print("\n9. SAVE EMBEDDINGS FOR DOWNSTREAM USE")
print("-" * 80)

# Select and prepare final format
df_final = df_with_embeddings.select(
    "image_id",
    "category",
    "category_id",
    "embedding"
)

# Show sample
print("Final embeddings DataFrame:")
df_final.show(5, truncate=True)

# In production, save to storage
# df_final.write.parquet("embeddings_output.parquet")
print("\n(In production: save to Parquet for reuse)")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("IMAGE EMBEDDING PIPELINE SUMMARY:")
print("=" * 80)
print("""
1. PyTorch Stage (Feature Extraction):
   ✓ CNN model extracts semantic embeddings
   ✓ Fixed-size vector representation (128D)
   ✓ Captures image features

2. PySpark Stage (Distributed Processing):
   ✓ Process large image datasets
   ✓ Extract embeddings in parallel
   ✓ Compute similarity metrics at scale

3. Analytics Capabilities:
   ✓ Nearest neighbor search
   ✓ Clustering analysis
   ✓ Similarity computations
   ✓ Category-wise statistics

4. Real-World Applications:
   ✓ Image search and retrieval
   ✓ Duplicate detection
   ✓ Content-based recommendations
   ✓ Visual similarity analysis

5. Best Practices:
   ✓ Broadcast model for efficiency
   ✓ Batch processing with Pandas UDF
   ✓ Save embeddings for reuse
   ✓ Use appropriate similarity metrics
   ✓ Consider dimensionality reduction
""")

spark.stop()
print("\n" + "=" * 80)
print("Example completed!")
print("=" * 80)
