"""
Example 1: PySpark DataFrame to PyTorch Tensor Conversion
==========================================================
Demonstrates basic conversion between PySpark DataFrames and PyTorch tensors.
"""

import numpy as np
import torch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

print("=" * 80)
print("PYSPARK DATAFRAME TO PYTORCH TENSOR CONVERSION")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("DataFrameToTensor") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# 1. CREATE SAMPLE DATAFRAME
# ============================================================================
print("\n1. CREATE SAMPLE DATAFRAME")
print("-" * 80)

# Sample numerical data
data = [
    (1, 2.5, 3.7, 4.2, 0),
    (2, 3.1, 4.5, 5.1, 1),
    (3, 1.8, 2.9, 3.5, 0),
    (4, 4.2, 5.3, 6.1, 1),
    (5, 2.9, 3.8, 4.7, 0),
    (6, 5.1, 6.2, 7.3, 1),
    (7, 3.5, 4.1, 5.2, 0),
    (8, 4.8, 5.9, 6.8, 1)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("feature1", DoubleType(), True),
    StructField("feature2", DoubleType(), True),
    StructField("feature3", DoubleType(), True),
    StructField("label", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)
print("Original DataFrame:")
df.show()

# ============================================================================
# 2. CONVERT DATAFRAME TO NUMPY (INTERMEDIATE STEP)
# ============================================================================
print("\n2. CONVERT DATAFRAME TO NUMPY")
print("-" * 80)

# Select feature columns
feature_cols = ["feature1", "feature2", "feature3"]
features_df = df.select(feature_cols)

# Convert to Pandas then NumPy
features_pandas = features_df.toPandas()
features_numpy = features_pandas.values

print(f"NumPy array shape: {features_numpy.shape}")
print(f"NumPy array dtype: {features_numpy.dtype}")
print(f"\nFirst 3 rows:\n{features_numpy[:3]}")

# ============================================================================
# 3. CONVERT NUMPY TO PYTORCH TENSOR
# ============================================================================
print("\n3. CONVERT NUMPY TO PYTORCH TENSOR")
print("-" * 80)

# Method 1: Using torch.from_numpy()
features_tensor = torch.from_numpy(features_numpy).float()
print(f"PyTorch tensor shape: {features_tensor.shape}")
print(f"PyTorch tensor dtype: {features_tensor.dtype}")
print(f"\nTensor:\n{features_tensor}")

# Method 2: Using torch.tensor() (creates a copy)
features_tensor_copy = torch.tensor(features_numpy, dtype=torch.float32)
print(f"\nTensor (copy) shape: {features_tensor_copy.shape}")

# ============================================================================
# 4. CONVERT LABELS TO TENSOR
# ============================================================================
print("\n4. CONVERT LABELS TO TENSOR")
print("-" * 80)

labels_numpy = df.select("label").toPandas().values.flatten()
labels_tensor = torch.from_numpy(labels_numpy).long()

print(f"Labels tensor shape: {labels_tensor.shape}")
print(f"Labels tensor dtype: {labels_tensor.dtype}")
print(f"Labels: {labels_tensor}")

# ============================================================================
# 5. COMPLETE WORKFLOW: DATAFRAME -> TENSOR
# ============================================================================
print("\n5. COMPLETE WORKFLOW: DATAFRAME -> TENSOR")
print("-" * 80)

def df_to_tensors(spark_df, feature_cols, label_col):
    """
    Convert PySpark DataFrame to PyTorch tensors
    
    Args:
        spark_df: PySpark DataFrame
        feature_cols: List of feature column names
        label_col: Label column name
        
    Returns:
        Tuple of (features_tensor, labels_tensor)
    """
    # Extract features
    features = spark_df.select(feature_cols).toPandas().values
    features_tensor = torch.from_numpy(features).float()
    
    # Extract labels
    labels = spark_df.select(label_col).toPandas().values.flatten()
    labels_tensor = torch.from_numpy(labels).long()
    
    return features_tensor, labels_tensor

X, y = df_to_tensors(df, feature_cols, "label")

print(f"Features tensor: {X.shape}")
print(f"Labels tensor: {y.shape}")
print(f"\nSample features:\n{X[:3]}")
print(f"\nSample labels: {y[:3]}")

# ============================================================================
# 6. TENSOR BACK TO DATAFRAME
# ============================================================================
print("\n6. TENSOR BACK TO DATAFRAME")
print("-" * 80)

def tensor_to_df(spark, tensor, column_names):
    """
    Convert PyTorch tensor back to PySpark DataFrame
    
    Args:
        spark: SparkSession
        tensor: PyTorch tensor
        column_names: List of column names
        
    Returns:
        PySpark DataFrame
    """
    # Convert to NumPy
    numpy_array = tensor.numpy()
    
    # Convert to Pandas DataFrame
    import pandas as pd
    pandas_df = pd.DataFrame(numpy_array, columns=column_names)
    
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)
    
    return spark_df

# Convert tensor back to DataFrame
reconstructed_df = tensor_to_df(spark, features_tensor, feature_cols)
print("Reconstructed DataFrame:")
reconstructed_df.show(5)

# ============================================================================
# 7. BATCH PROCESSING
# ============================================================================
print("\n7. BATCH PROCESSING WITH TENSORS")
print("-" * 80)

batch_size = 3
num_batches = (len(X) + batch_size - 1) // batch_size

print(f"Total samples: {len(X)}")
print(f"Batch size: {batch_size}")
print(f"Number of batches: {num_batches}")

for i in range(num_batches):
    start_idx = i * batch_size
    end_idx = min((i + 1) * batch_size, len(X))
    
    batch_X = X[start_idx:end_idx]
    batch_y = y[start_idx:end_idx]
    
    print(f"\nBatch {i + 1}:")
    print(f"  Features shape: {batch_X.shape}")
    print(f"  Labels shape: {batch_y.shape}")
    print(f"  Labels: {batch_y.tolist()}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY TAKEAWAYS:")
print("=" * 80)
print("""
1. Conversion Path: DataFrame -> Pandas -> NumPy -> Tensor
   - df.toPandas().values -> np.ndarray
   - torch.from_numpy(array) -> torch.Tensor

2. Data Types:
   - Use .float() for features (torch.float32)
   - Use .long() for labels (torch.int64)

3. Memory Considerations:
   - torch.from_numpy() shares memory (view)
   - torch.tensor() creates a copy (safer for modifications)

4. Reverse Conversion: Tensor -> DataFrame
   - tensor.numpy() -> np.ndarray
   - Use Pandas as intermediate step

5. Best Practices:
   - Collect only what fits in driver memory
   - Use sampling for large datasets
   - Consider partitioned processing for big data
""")

spark.stop()
print("\n" + "=" * 80)
print("Example completed!")
print("=" * 80)
