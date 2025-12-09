"""
Basic ETL Pipeline Example
Demonstrates a complete Extract-Transform-Load process
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from readers.data_reader import DataReader
from transformations.common_transforms import CommonTransforms
from utils.spark_session import create_spark_session, stop_spark_session
from writers.data_writer import DataWriter


class ETLPipeline:
    """
    Basic ETL Pipeline for processing data.
    
    This pipeline demonstrates:
    - Reading data from CSV
    - Applying transformations
    - Writing results to Parquet
    """
    
    def __init__(self, app_name: str = "Basic ETL Pipeline"):
        """Initialize the ETL pipeline."""
        self.spark = create_spark_session(app_name=app_name)
        self.reader = DataReader(self.spark)
        self.writer = DataWriter()
        self.transforms = CommonTransforms()
    
    def extract(self, input_path: str):
        """
        Extract data from source.
        
        Args:
            input_path: Path to input data
            
        Returns:
            DataFrame: Extracted data
        """
        print(f"Extracting data from: {input_path}")
        df = self.reader.read_csv(input_path)
        print(f"Extracted {df.count()} rows")
        return df
    
    def transform(self, df):
        """
        Apply transformations to data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        print("Applying transformations...")
        
        # Remove duplicates
        df = self.transforms.remove_duplicates(df)
        
        # Filter null values in critical columns
        # df = self.transforms.filter_nulls(df, ["id", "name"])
        
        # Add processing timestamp
        df = self.transforms.add_timestamp_column(df, "processed_at")
        
        # Example: Add calculated columns
        # df = self.transforms.add_derived_column(
        #     df, 
        #     "full_name", 
        #     F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
        # )
        
        print(f"Transformation complete. Result: {df.count()} rows")
        return df
    
    def load(self, df, output_path: str):
        """
        Load transformed data to destination.
        
        Args:
            df: Transformed DataFrame
            output_path: Path to write output
        """
        print(f"Loading data to: {output_path}")
        self.writer.write_parquet(df, output_path, mode="overwrite")
        print("Data loaded successfully")
    
    def run(self, input_path: str, output_path: str):
        """
        Execute the complete ETL pipeline.
        
        Args:
            input_path: Path to input data
            output_path: Path to write output
        """
        try:
            # Extract
            raw_df = self.extract(input_path)
            
            # Transform
            transformed_df = self.transform(raw_df)
            
            # Load
            self.load(transformed_df, output_path)
            
            print("ETL pipeline completed successfully!")
            
        except Exception as e:
            print(f"Error in ETL pipeline: {str(e)}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        print("Cleaning up resources...")
        stop_spark_session(self.spark)


def main():
    """Main entry point for the ETL pipeline."""
    # Example usage
    input_path = "data/raw/input.csv"
    output_path = "data/processed/output.parquet"
    
    pipeline = ETLPipeline()
    pipeline.run(input_path, output_path)


if __name__ == "__main__":
    main()
