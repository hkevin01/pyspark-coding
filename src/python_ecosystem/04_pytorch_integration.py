"""
PyTorch Integration with PySpark

PyTorch is the leading deep learning framework. This module shows how to
use PyTorch models with PySpark for distributed deep learning inference.

WHAT IS PYTORCH?
================
- Deep learning and neural networks
- Dynamic computational graphs
- GPU acceleration
- Rich ecosystem (torchvision, torchtext, etc.)
- Production deployment (TorchScript, ONNX)

WHY USE PYTORCH WITH PYSPARK?
==============================
- Train on GPU, inference on distributed data
- Leverage pre-trained models (BERT, ResNet, etc.)
- Scale deep learning to big data
- Combine structured data (Spark) with deep learning (PyTorch)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType, ArrayType, FloatType
import pandas as pd
import numpy as np

# =============================================================================
# 1. PYTORCH BASICS WITH PANDAS UDFS
# =============================================================================

def pytorch_basic_inference():
    """
    Use PyTorch models in Pandas UDFs for distributed inference.
    
    WORKFLOW:
    1. Train PyTorch model (locally or on GPU)
    2. Save model weights
    3. Load in Pandas UDF
    4. Apply to distributed Spark data
    """
    
    spark = SparkSession.builder \
        .appName("PyTorch Basic") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create test data
    df = spark.range(0, 100).select(
        col("id"),
        (col("id") % 10).cast("double").alias("feature1"),
        ((col("id") * 2) % 10).cast("double").alias("feature2"),
        ((col("id") * 3) % 10).cast("double").alias("feature3")
    )
    
    # ==========================================================================
    # Define PyTorch Model
    # ==========================================================================
    
    import torch
    import torch.nn as nn
    
    class SimpleNN(nn.Module):
        """Simple feedforward neural network"""
        def __init__(self, input_size=3, hidden_size=16, output_size=1):
            super(SimpleNN, self).__init__()
            self.fc1 = nn.Linear(input_size, hidden_size)
            self.relu = nn.ReLU()
            self.fc2 = nn.Linear(hidden_size, output_size)
        
        def forward(self, x):
            x = self.fc1(x)
            x = self.relu(x)
            x = self.fc2(x)
            return x
    
    # Train a simple model
    model = SimpleNN()
    model.eval()  # Set to evaluation mode
    
    print("PyTorch Model Architecture:")
    print(model)
    
    # ==========================================================================
    # Distributed Inference with Pandas UDF
    # ==========================================================================
    
    @pandas_udf(DoubleType())
    def pytorch_inference(f1: pd.Series, f2: pd.Series, f3: pd.Series) -> pd.Series:
        """
        Apply PyTorch model to distributed data.
        Model is loaded once per batch!
        """
        # Stack features
        features = np.column_stack([f1.values, f2.values, f3.values])
        
        # Convert to PyTorch tensor
        X_tensor = torch.FloatTensor(features)
        
        # Inference (no gradient computation)
        with torch.no_grad():
            predictions = model(X_tensor)
        
        # Convert back to pandas
        return pd.Series(predictions.numpy().flatten())
    
    # Apply to full dataset
    result = df.withColumn(
        "prediction",
        pytorch_inference(col("feature1"), col("feature2"), col("feature3"))
    )
    
    print("\nPyTorch Predictions:")
    result.show(10)
    
    spark.stop()

# =============================================================================
# 2. PYTORCH MODEL TRAINING
# =============================================================================

def pytorch_model_training():
    """
    Train PyTorch models on sampled Spark data.
    
    WORKFLOW:
    1. Sample data from Spark
    2. Convert to Pandas/NumPy
    3. Train PyTorch model
    4. Apply to full Spark dataset
    """
    
    spark = SparkSession.builder \
        .appName("PyTorch Training") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate synthetic data
    from sklearn.datasets import make_regression
    
    X, y = make_regression(n_samples=10000, n_features=10, noise=10, random_state=42)
    
    data = [(int(i),) + tuple(float(x) for x in X[i]) + (float(y[i]),)
            for i in range(len(y))]
    
    cols = ["id"] + [f"feature{i}" for i in range(10)] + ["target"]
    df = spark.createDataFrame(data, cols)
    
    # ==========================================================================
    # Train PyTorch Model
    # ==========================================================================
    
    import torch
    import torch.nn as nn
    import torch.optim as optim
    
    class RegressionNN(nn.Module):
        """Neural network for regression"""
        def __init__(self):
            super(RegressionNN, self).__init__()
            self.fc1 = nn.Linear(10, 64)
            self.fc2 = nn.Linear(64, 32)
            self.fc3 = nn.Linear(32, 1)
            self.relu = nn.ReLU()
        
        def forward(self, x):
            x = self.relu(self.fc1(x))
            x = self.relu(self.fc2(x))
            x = self.fc3(x)
            return x
    
    # Sample and prepare data
    train_df = df.sample(0.8, seed=42).toPandas()
    
    feature_cols = [f"feature{i}" for i in range(10)]
    X_train = torch.FloatTensor(train_df[feature_cols].values)
    y_train = torch.FloatTensor(train_df["target"].values).view(-1, 1)
    
    # Initialize model
    model = RegressionNN()
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    
    # Train
    print("Training PyTorch Model...")
    model.train()
    
    for epoch in range(50):
        optimizer.zero_grad()
        outputs = model(X_train)
        loss = criterion(outputs, y_train)
        loss.backward()
        optimizer.step()
        
        if (epoch + 1) % 10 == 0:
            print(f"Epoch [{epoch+1}/50], Loss: {loss.item():.4f}")
    
    model.eval()
    print("\n✅ Model trained successfully!")
    
    # ==========================================================================
    # Distributed Inference
    # ==========================================================================
    
    @pandas_udf(DoubleType())
    def pytorch_predict(*features):
        """Apply trained model"""
        X = pd.DataFrame({f'f{i}': features[i] for i in range(len(features))})
        X_tensor = torch.FloatTensor(X.values)
        
        with torch.no_grad():
            predictions = model(X_tensor)
        
        return pd.Series(predictions.numpy().flatten())
    
    # Apply to full dataset
    feature_cols_spark = [col(f"feature{i}") for i in range(10)]
    result = df.withColumn("prediction", pytorch_predict(*feature_cols_spark))
    
    print("\nPredictions vs Actual:")
    result.select("id", "target", "prediction").show(10)
    
    spark.stop()

# =============================================================================
# 3. PRETRAINED MODELS (TRANSFER LEARNING)
# =============================================================================

def pytorch_pretrained_models():
    """
    Use pre-trained PyTorch models for feature extraction.
    
    EXAMPLES:
    - ResNet for image features
    - BERT for text embeddings
    - GPT for text generation
    """
    
    spark = SparkSession.builder \
        .appName("PyTorch Pretrained") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    print("Pre-trained Model Example:")
    print("=" * 70)
    print("For production use, you would:")
    print("1. Load pre-trained model (e.g., ResNet, BERT)")
    print("2. Create Pandas UDF for feature extraction")
    print("3. Apply to distributed data")
    print("=" * 70)
    
    # ==========================================================================
    # Example: Using a Pre-trained Model (Simplified)
    # ==========================================================================
    
    import torch
    import torch.nn as nn
    
    # Simulate a feature extractor (in practice, load ResNet/BERT)
    class FeatureExtractor(nn.Module):
        """Simulates a pre-trained feature extractor"""
        def __init__(self):
            super(FeatureExtractor, self).__init__()
            self.encoder = nn.Sequential(
                nn.Linear(100, 64),
                nn.ReLU(),
                nn.Linear(64, 32)
            )
        
        def forward(self, x):
            return self.encoder(x)
    
    # Create model
    extractor = FeatureExtractor()
    extractor.eval()
    
    # Create test data (e.g., image vectors)
    df = spark.range(0, 100).select(
        col("id"),
        # Simulate 100-dim input (could be image pixels, text vectors, etc.)
    )
    
    print("\n✅ Feature Extractor Ready!")
    print("In production: Use torchvision.models or transformers library")
    
    # Example usage pattern:
    print("\nExample Code Pattern:")
    print("-" * 70)
    print("""
    @pandas_udf(ArrayType(FloatType()))
    def extract_features(input_col: pd.Series) -> pd.Series:
        # Load pre-trained model once per batch
        model = load_pretrained_model()
        model.eval()
        
        # Process batch
        with torch.no_grad():
            features = model(torch.tensor(input_col.tolist()))
        
        return pd.Series(features.tolist())
    """)
    print("-" * 70)
    
    spark.stop()

# =============================================================================
# 4. GPU ACCELERATION
# =============================================================================

def pytorch_gpu_inference():
    """
    Use GPU for PyTorch inference (if available).
    
    BENEFITS:
    - 10-100x faster inference
    - Especially for deep models
    - Batch processing for efficiency
    """
    
    spark = SparkSession.builder \
        .appName("PyTorch GPU") \
        .master("local[*]") \
        .getOrCreate()
    
    import torch
    
    # Check GPU availability
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    print("GPU Acceleration:")
    print("=" * 70)
    print(f"Device: {device}")
    print(f"CUDA Available: {torch.cuda.is_available()}")
    
    if torch.cuda.is_available():
        print(f"GPU Name: {torch.cuda.get_device_name(0)}")
        print(f"GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    else:
        print("Running on CPU (GPU not available)")
    
    print("=" * 70)
    
    # ==========================================================================
    # GPU-Accelerated Inference Pattern
    # ==========================================================================
    
    print("\nGPU Inference Pattern:")
    print("-" * 70)
    print("""
    @pandas_udf(DoubleType())
    def gpu_inference(feature: pd.Series) -> pd.Series:
        # Load model to GPU
        device = torch.device("cuda")
        model.to(device)
        model.eval()
        
        # Process batch on GPU
        X_tensor = torch.FloatTensor(feature.tolist()).to(device)
        
        with torch.no_grad():
            predictions = model(X_tensor)
        
        # Move back to CPU
        return pd.Series(predictions.cpu().numpy())
    """)
    print("-" * 70)
    
    print("\n⚠️  Note: Each executor needs its own GPU!")
    print("Configure Spark: spark.executor.resource.gpu.amount=1")
    
    spark.stop()

# =============================================================================
# 5. MODEL PERSISTENCE AND LOADING
# =============================================================================

def pytorch_model_persistence():
    """
    Save and load PyTorch models efficiently.
    
    OPTIONS:
    - torch.save(): Save state dict
    - TorchScript: JIT compilation
    - ONNX: Cross-platform format
    """
    
    spark = SparkSession.builder \
        .appName("PyTorch Persistence") \
        .master("local[*]") \
        .getOrCreate()
    
    import torch
    import torch.nn as nn
    import tempfile
    import os
    
    # ==========================================================================
    # Define and Train Model
    # ==========================================================================
    
    class SimpleModel(nn.Module):
        def __init__(self):
            super(SimpleModel, self).__init__()
            self.fc = nn.Linear(5, 1)
        
        def forward(self, x):
            return self.fc(x)
    
    model = SimpleModel()
    print("Model Architecture:")
    print(model)
    
    # ==========================================================================
    # Save Model (State Dict)
    # ==========================================================================
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pth') as tmp:
        model_path = tmp.name
    
    torch.save(model.state_dict(), model_path)
    print(f"\n✅ Model saved to: {model_path}")
    
    # ==========================================================================
    # Load Model
    # ==========================================================================
    
    loaded_model = SimpleModel()
    loaded_model.load_state_dict(torch.load(model_path))
    loaded_model.eval()
    
    print("✅ Model loaded successfully!")
    
    # Test
    test_input = torch.randn(1, 5)
    with torch.no_grad():
        output = loaded_model(test_input)
    
    print(f"\nTest Output: {output.item():.4f}")
    
    # ==========================================================================
    # TorchScript (for production)
    # ==========================================================================
    
    print("\n" + "=" * 70)
    print("TORCHSCRIPT (Production Deployment):")
    print("=" * 70)
    
    # Trace model
    traced_model = torch.jit.trace(loaded_model, test_input)
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.pt') as tmp:
        script_path = tmp.name
    
    traced_model.save(script_path)
    print(f"✅ TorchScript saved to: {script_path}")
    
    # Load TorchScript
    loaded_script = torch.jit.load(script_path)
    
    print("✅ TorchScript loaded!")
    print("\nBenefits:")
    print("- Optimized for inference")
    print("- No Python dependency")
    print("- Can run in C++ environment")
    
    # Cleanup
    os.unlink(model_path)
    os.unlink(script_path)
    
    spark.stop()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("PYTORCH INTEGRATION WITH PYSPARK")
    print("=" * 70)
    
    print("\n1. PyTorch Basic Inference")
    print("-" * 70)
    pytorch_basic_inference()
    
    print("\n2. PyTorch Model Training")
    print("-" * 70)
    pytorch_model_training()
    
    print("\n3. Pre-trained Models (Transfer Learning)")
    print("-" * 70)
    pytorch_pretrained_models()
    
    print("\n4. GPU Acceleration")
    print("-" * 70)
    pytorch_gpu_inference()
    
    print("\n5. Model Persistence and Loading")
    print("-" * 70)
    pytorch_model_persistence()
    
    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS:")
    print("=" * 70)
    print("✅ Train PyTorch models on sampled data")
    print("✅ Use Pandas UDFs for distributed inference")
    print("✅ Leverage pre-trained models (ResNet, BERT)")
    print("✅ GPU acceleration for faster inference")
    print("✅ TorchScript for production deployment")
    print("=" * 70 + "\n")
