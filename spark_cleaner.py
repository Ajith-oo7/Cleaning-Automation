"""
Spark-based data cleaner for extremely large datasets.
This is an optional extension for datasets that are too large for Dask.
Requires Apache Spark to be installed.
"""

import os
import time
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, count, lit, lower, trim, percentile_approx

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class SparkDataCleaner:
    """PySpark-based data cleaner for extremely large datasets."""
    
    def __init__(self, app_name="DataCleaner", memory="4g"):
        """Initialize SparkDataCleaner with specified memory."""
        self.logger = logger
        self.app_name = app_name
        self.memory = memory
        self.spark = None
    
    def _create_spark_session(self):
        """Create and configure a Spark session."""
        self.spark = (SparkSession.builder
                     .appName(self.app_name)
                     .config("spark.driver.memory", self.memory)
                     .config("spark.executor.memory", self.memory)
                     .getOrCreate())
        
        self.logger.info(f"Created Spark session with {self.memory} memory")
    
    def clean_data(self, file_path):
        """Clean data using Spark."""
        try:
            start_time = time.time()
            file_path = Path(file_path)
            file_extension = file_path.suffix.lower()
            
            # Create Spark session
            if self.spark is None:
                self._create_spark_session()
            
            # Get file size
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            self.logger.info(f"Starting to process {file_path} ({file_size_mb:.2f} MB) with Spark")
            
            # Output path
            output_path = file_path.with_name(f"{file_path.stem}_cleaned{file_extension}")
            
            # Read the data
            df = self._read_file(file_path, file_extension)
            
            # Get initial row count
            initial_count = df.count()
            initial_cols = len(df.columns)
            self.logger.info(f"Initial data: {initial_count} rows, {initial_cols} columns")
            
            # Clean the data
            cleaned_df = self._process_data(df)
            
            # Get final row count
            final_count = cleaned_df.count()
            self.logger.info(f"Final data: {final_count} rows, {len(cleaned_df.columns)} columns")
            self.logger.info(f"Removed {initial_count - final_count} rows during cleaning")
            
            # Save the data
            self._save_file(cleaned_df, output_path, file_extension)
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"Processing completed in {elapsed_time:.2f} seconds")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error cleaning data with Spark: {str(e)}")
            return False
        
        finally:
            # Don't stop Spark session to allow reuse
            pass
    
    def _read_file(self, file_path, file_extension):
        """Read file with Spark based on extension."""
        if file_extension == '.csv':
            return self.spark.read.option("header", "true").option("inferSchema", "true").csv(str(file_path))
        elif file_extension == '.parquet':
            return self.spark.read.parquet(str(file_path))
        elif file_extension == '.json':
            return self.spark.read.json(str(file_path))
        else:
            raise ValueError(f"Unsupported file extension for Spark: {file_extension}")
    
    def _process_data(self, df):
        """Apply data cleaning operations with Spark."""
        # Log null values
        self.logger.info("Checking for null values:")
        null_counts = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
        null_counts.show()
        
        # 1. Remove duplicates
        df = df.dropDuplicates()
        
        # 2. Handle missing values
        numeric_cols = [f.name for f in df.schema.fields if 
                       f.dataType.typeName() in ('integer', 'long', 'double', 'float')]
        
        categorical_cols = [f.name for f in df.schema.fields if 
                           f.dataType.typeName() in ('string')]
        
        # Fill nulls in numeric columns with approximate median
        for col_name in numeric_cols:
            # Calculate approximate median using percentile_approx
            approx_median = df.select(
                percentile_approx(col(col_name), 0.5, 10000)
            ).collect()[0][0]
            
            if approx_median is not None:
                df = df.withColumn(
                    col_name, 
                    when(col(col_name).isNull() | isnan(col(col_name)), 
                         lit(approx_median)
                    ).otherwise(col(col_name))
                )
        
        # Fill nulls in categorical columns
        for col_name in categorical_cols:
            # Get most frequent value
            most_frequent = df.groupBy(col_name) \
                             .count() \
                             .orderBy("count", ascending=False) \
                             .filter(col(col_name).isNotNull()) \
                             .first()
            
            fill_value = "Unknown"
            if most_frequent is not None:
                fill_value = most_frequent[0]
            
            df = df.withColumn(
                col_name, 
                when(col(col_name).isNull(), lit(fill_value)).otherwise(col(col_name))
            )
        
        # 3. Remove outliers using IQR method for numeric columns
        for col_name in numeric_cols:
            # Calculate quartiles
            quantiles = df.select(
                percentile_approx(col(col_name), [0.25, 0.75], 10000)
            ).collect()[0][0]
            
            if quantiles and None not in quantiles:
                q1, q3 = quantiles
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                # Filter out outliers
                outlier_count = df.filter(
                    (col(col_name) < lower_bound) | 
                    (col(col_name) > upper_bound)
                ).count()
                
                df = df.filter(
                    (col(col_name) >= lower_bound) & 
                    (col(col_name) <= upper_bound)
                )
                
                self.logger.info(f"Removed {outlier_count} outliers from column {col_name}")
        
        # 4. Standardize text data
        for col_name in categorical_cols:
            df = df.withColumn(col_name, lower(trim(col(col_name))))
        
        return df
    
    def _save_file(self, df, output_path, file_extension):
        """Save file with Spark based on extension."""
        if file_extension == '.csv':
            df.write.option("header", "true").mode("overwrite").csv(str(output_path))
        elif file_extension == '.parquet':
            df.write.mode("overwrite").parquet(str(output_path))
        elif file_extension == '.json':
            df.write.mode("overwrite").json(str(output_path))
        else:
            raise ValueError(f"Unsupported file extension for saving: {file_extension}")
    
    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Stopped Spark session")
            self.spark = None


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python spark_cleaner.py <file_path> [memory]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    memory = sys.argv[2] if len(sys.argv) > 2 else "4g"
    
    cleaner = SparkDataCleaner(memory=memory)
    cleaner.clean_data(file_path)
    cleaner.stop()