"""
Data Reader Module
Provides utilities for reading data from various sources
"""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


class DataReader:
    """Class for reading data from various sources."""
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DataReader.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
    
    def read_csv(
        self,
        path: str,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        **options
    ) -> DataFrame:
        """
        Read CSV file(s) into a DataFrame.
        
        Args:
            path: Path to CSV file or directory
            header: Whether first row contains headers
            infer_schema: Whether to infer schema automatically
            delimiter: Field delimiter character
            **options: Additional read options
            
        Returns:
            DataFrame: Loaded data
        """
        return (
            self.spark.read
            .option("header", header)
            .option("inferSchema", infer_schema)
            .option("delimiter", delimiter)
            .options(**options)
            .csv(path)
        )
    
    def read_json(
        self,
        path: str,
        multiline: bool = False,
        **options
    ) -> DataFrame:
        """
        Read JSON file(s) into a DataFrame.
        
        Args:
            path: Path to JSON file or directory
            multiline: Whether JSON spans multiple lines
            **options: Additional read options
            
        Returns:
            DataFrame: Loaded data
        """
        return (
            self.spark.read
            .option("multiLine", multiline)
            .options(**options)
            .json(path)
        )
    
    def read_parquet(self, path: str, **options) -> DataFrame:
        """
        Read Parquet file(s) into a DataFrame.
        
        Args:
            path: Path to Parquet file or directory
            **options: Additional read options
            
        Returns:
            DataFrame: Loaded data
        """
        return self.spark.read.options(**options).parquet(path)
    
    def read_jdbc(
        self,
        url: str,
        table: str,
        properties: dict,
        **options
    ) -> DataFrame:
        """
        Read data from JDBC source.
        
        Args:
            url: JDBC connection URL
            table: Table name or query
            properties: Connection properties (user, password, etc.)
            **options: Additional read options
            
        Returns:
            DataFrame: Loaded data
        """
        return (
            self.spark.read
            .jdbc(url, table, properties=properties)
            .options(**options)
        )
    
    def read_delta(self, path: str, **options) -> DataFrame:
        """
        Read Delta table into a DataFrame.
        
        Args:
            path: Path to Delta table
            **options: Additional read options
            
        Returns:
            DataFrame: Loaded data
        """
        return self.spark.read.format("delta").options(**options).load(path)
