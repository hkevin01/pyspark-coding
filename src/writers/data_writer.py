"""
Data Writer Module
Provides utilities for writing data to various destinations
"""
from pyspark.sql import DataFrame


class DataWriter:
    """Class for writing data to various destinations."""
    
    @staticmethod
    def write_csv(
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        header: bool = True,
        **options
    ) -> None:
        """
        Write DataFrame to CSV file(s).
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, error, ignore)
            header: Whether to write headers
            **options: Additional write options
        """
        (
            df.write
            .mode(mode)
            .option("header", header)
            .options(**options)
            .csv(path)
        )
    
    @staticmethod
    def write_json(
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        **options
    ) -> None:
        """
        Write DataFrame to JSON file(s).
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, error, ignore)
            **options: Additional write options
        """
        (
            df.write
            .mode(mode)
            .options(**options)
            .json(path)
        )
    
    @staticmethod
    def write_parquet(
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: list = None,
        **options
    ) -> None:
        """
        Write DataFrame to Parquet file(s).
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, error, ignore)
            partition_by: Columns to partition by
            **options: Additional write options
        """
        writer = df.write.mode(mode).options(**options)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.parquet(path)
    
    @staticmethod
    def write_jdbc(
        df: DataFrame,
        url: str,
        table: str,
        mode: str = "overwrite",
        properties: dict = None,
        **options
    ) -> None:
        """
        Write DataFrame to JDBC destination.
        
        Args:
            df: DataFrame to write
            url: JDBC connection URL
            table: Target table name
            mode: Write mode (overwrite, append, error, ignore)
            properties: Connection properties (user, password, etc.)
            **options: Additional write options
        """
        props = properties or {}
        (
            df.write
            .mode(mode)
            .jdbc(url, table, properties=props)
            .options(**options)
        )
    
    @staticmethod
    def write_delta(
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: list = None,
        **options
    ) -> None:
        """
        Write DataFrame to Delta table.
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (overwrite, append, error, ignore)
            partition_by: Columns to partition by
            **options: Additional write options
        """
        writer = df.write.format("delta").mode(mode).options(**options)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(path)
