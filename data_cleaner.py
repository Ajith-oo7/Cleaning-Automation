import os
import time
import logging
import sys
from pathlib import Path
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import dask.dataframe as dd
import pandas as pd
import numpy as np
from dotenv import load_dotenv

# Import the data analyzer (add this)
try:
    from data_analyzer import DataAnalyzer
    SWEETVIZ_AVAILABLE = True
except ImportError:
    SWEETVIZ_AVAILABLE = False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables if .env file exists
load_dotenv()

# Directory to monitor - default to current directory if not specified in .env
MONITOR_DIR = os.getenv('MONITOR_DIR', os.path.dirname(os.path.abspath(__file__)))
PROCESSED_MARKER = '_processed'
CLEANED_SUFFIX = '_cleaned'
SUPPORTED_EXTENSIONS = ['.csv', '.xlsx', '.parquet', '.json']
# Add this line to control whether to generate reports
GENERATE_REPORTS = os.getenv('GENERATE_REPORTS', 'True').lower() in ('true', '1', 't')
# Add this to control sample size for reports on large files
REPORT_SAMPLE_SIZE = int(os.getenv('REPORT_SAMPLE_SIZE', '10000'))

class DataCleaner:
    """Class to handle data cleaning operations for large datasets."""
    
    def __init__(self):
        self.logger = logger
        # Initialize the analyzer if Sweetviz is available
        if SWEETVIZ_AVAILABLE and GENERATE_REPORTS:
            self.analyzer = DataAnalyzer()
        else:
            self.analyzer = None
    
    def clean_data(self, file_path):
        """Clean data from the given file path and save to a new file."""
        try:
            start_time = time.time()
            file_path = Path(file_path)
            file_extension = file_path.suffix.lower()
            
            if file_extension not in SUPPORTED_EXTENSIONS:
                self.logger.warning(f"Unsupported file type: {file_extension}")
                return False
            
            # Get file size before processing
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            self.logger.info(f"Starting to process {file_path} ({file_size_mb:.2f} MB)")
            
            # Output path
            output_path = file_path.with_name(f"{file_path.stem}{CLEANED_SUFFIX}{file_extension}")
            
            # Generate report on original data if analyzer is available
            if self.analyzer:
                self.logger.info("Generating analysis report for original data...")
                self.analyzer.analyze_file(file_path, sample_size=REPORT_SAMPLE_SIZE)
            
            # Use dask for large files
            if file_size_mb > 100:  # Use dask for files larger than 100MB
                self.logger.info("Using Dask for large file processing")
                df = self._read_file_with_dask(file_path, file_extension)
                cleaned_df = self._process_with_dask(df)
                self._save_file_with_dask(cleaned_df, output_path, file_extension)
            else:
                self.logger.info("Using Pandas for processing")
                df = self._read_file_with_pandas(file_path, file_extension)
                cleaned_df = self._process_with_pandas(df)
                self._save_file_with_pandas(cleaned_df, output_path, file_extension)
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"Processing completed in {elapsed_time:.2f} seconds")
            self.logger.info(f"Cleaned data saved to {output_path}")
            
            # Generate comparison report if analyzer is available
            if self.analyzer and output_path.exists():
                self.logger.info("Generating comparison report between original and cleaned data...")
                report_path = self.analyzer.analyze_file(
                    file_path, 
                    cleaned_file_path=output_path,
                    sample_size=REPORT_SAMPLE_SIZE
                )
                if report_path:
                    self.logger.info(f"Data comparison report available at: {report_path}")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error cleaning data: {str(e)}")
            return False
    
    def _read_file_with_dask(self, file_path, file_extension):
        """Read file with Dask based on extension."""
        if file_extension == '.csv':
            return dd.read_csv(file_path)
        elif file_extension == '.parquet':
            return dd.read_parquet(file_path)
        elif file_extension == '.json':
            return dd.read_json(file_path)
        elif file_extension == '.xlsx':
            # Dask doesn't directly support Excel, read with pandas then convert
            return dd.from_pandas(pd.read_excel(file_path), npartitions=10)
    
    def _read_file_with_pandas(self, file_path, file_extension):
        """Read file with Pandas based on extension."""
        if file_extension == '.csv':
            return pd.read_csv(file_path)
        elif file_extension == '.parquet':
            return pd.read_parquet(file_path)
        elif file_extension == '.json':
            return pd.read_json(file_path)
        elif file_extension == '.xlsx':
            return pd.read_excel(file_path)
    
    def _process_with_dask(self, df):
        """Apply data cleaning operations with Dask."""
        # Remove duplicates (if possible with dask)
        try:
            df = df.drop_duplicates()
        except:
            self.logger.warning("Skipping duplicate removal with Dask")
        
        # Fill missing values with appropriate methods
        # For numerical columns
        numerical_cols = df.select_dtypes(include=['number']).columns
        for col in numerical_cols:
            # Fill with median for numerical data
            df[col] = df[col].fillna(df[col].quantile(0.5))
        
        # For other columns, fill with most common value
        categorical_cols = df.select_dtypes(exclude=['number']).columns
        for col in categorical_cols:
            # Fill with mode (most frequent)
            # Since mode is complex in Dask, we'll use a simpler approach
            df[col] = df[col].fillna('Unknown')
        
        # Remove outliers (example for numerical columns)
        # This is a simplified approach for demonstration
        for col in numerical_cols:
            # Compute Q1 and Q3
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Filter out outliers
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
        
        return df
    
    def _process_with_pandas(self, df):
        """Apply data cleaning operations with Pandas."""
        # Initial shape
        initial_rows, initial_cols = df.shape
        self.logger.info(f"Initial data shape: {initial_rows} rows, {initial_cols} columns")
        
        # Remove duplicates
        df = df.drop_duplicates()
        self.logger.info(f"After removing duplicates: {df.shape[0]} rows")
        
        # Handle missing values
        # For numerical columns
        numerical_cols = df.select_dtypes(include=['number']).columns
        for col in numerical_cols:
            # Fill with median for numerical data
            df[col] = df[col].fillna(df[col].median())
        
        # For categorical columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            # Fill with mode (most frequent)
            mode_val = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            df[col] = df[col].fillna(mode_val)
        
        # For datetime columns
        datetime_cols = df.select_dtypes(include=['datetime']).columns
        for col in datetime_cols:
            # Forward fill for time series data, then backward fill any remaining NaTs
            df[col] = df[col].fillna(method='ffill').fillna(method='bfill')
        
        # Remove outliers for numerical columns
        for col in numerical_cols:
            # Compute Q1, Q3 and IQR
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            
            # Define bounds
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Filter outliers
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)].shape[0]
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
            if outliers > 0:
                self.logger.info(f"Removed {outliers} outliers from column {col}")
        
        # Standardize text data (for categorical columns)
        for col in categorical_cols:
            # Convert to lowercase if it's a string column
            if df[col].dtype == 'object':
                df[col] = df[col].str.lower().str.strip()
        
        # Final shape
        final_rows, final_cols = df.shape
        self.logger.info(f"Final data shape: {final_rows} rows, {final_cols} columns")
        self.logger.info(f"Removed {initial_rows - final_rows} rows during cleaning")
        
        return df
    
    def _save_file_with_dask(self, df, output_path, file_extension):
        """Save file with Dask based on extension."""
        if file_extension == '.csv':
            df.to_csv(output_path, index=False, single_file=True)
        elif file_extension == '.parquet':
            df.to_parquet(output_path, engine='pyarrow')
        elif file_extension == '.json':
            df.to_json(output_path, orient='records', lines=True)
        elif file_extension == '.xlsx':
            # Convert to pandas and save
            df.compute().to_excel(output_path, index=False)
    
    def _save_file_with_pandas(self, df, output_path, file_extension):
        """Save file with Pandas based on extension."""
        if file_extension == '.csv':
            df.to_csv(output_path, index=False)
        elif file_extension == '.parquet':
            df.to_parquet(output_path, engine='pyarrow')
        elif file_extension == '.json':
            df.to_json(output_path, orient='records', lines=True)
        elif file_extension == '.xlsx':
            df.to_excel(output_path, index=False)


class DataFileHandler(FileSystemEventHandler):
    """Handler for file system events."""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.logger = logger
        self.processed_files = set()
    
    def on_created(self, event):
        """Handle file creation events."""
        if event.is_directory:
            return
        
        file_path = event.src_path
        if self._should_process_file(file_path):
            self.logger.info(f"New file detected: {file_path}")
            time.sleep(1)  # Wait a bit to ensure file is fully written
            self.process_file(file_path)
    
    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return
        
        file_path = event.src_path
        if self._should_process_file(file_path):
            self.logger.info(f"File modified: {file_path}")
            time.sleep(1)  # Wait a bit to ensure file is fully written
            self.process_file(file_path)
    
    def _should_process_file(self, file_path):
        """Check if file should be processed."""
        # Skip already processed files
        if file_path in self.processed_files:
            return False
        
        # Check file extension
        _, file_extension = os.path.splitext(file_path)
        if file_extension.lower() not in SUPPORTED_EXTENSIONS:
            return False
        
        # Skip files that are already cleaned
        if CLEANED_SUFFIX in file_path:
            return False
        
        # Skip temporary files
        if file_path.endswith('.tmp') or file_path.startswith('.'):
            return False
        
        return True
    
    def process_file(self, file_path):
        """Process a data file."""
        try:
            self.logger.info(f"Starting to process {file_path}")
            success = self.cleaner.clean_data(file_path)
            
            if success:
                self.processed_files.add(file_path)
                self.logger.info(f"Successfully processed {file_path}")
            else:
                self.logger.error(f"Failed to process {file_path}")
        
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")


def run_file_monitor():
    """Run the file monitoring service."""
    logger.info(f"Starting file monitoring in directory: {MONITOR_DIR}")
    logger.info(f"Supported file extensions: {', '.join(SUPPORTED_EXTENSIONS)}")
    
    event_handler = DataFileHandler()
    observer = Observer()
    observer.schedule(event_handler, MONITOR_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping file monitoring...")
        observer.stop()
    
    observer.join()


if __name__ == "__main__":
    logger.info("Data Cleaning Automation Starting...")
    run_file_monitor()