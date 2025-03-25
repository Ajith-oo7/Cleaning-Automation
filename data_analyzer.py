"""
Data analysis module using Sweetviz to generate visual reports.
This allows comparison of data before and after cleaning.
"""

import os
import sweetviz
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DataAnalyzer:
    """Generate Sweetviz reports for data analysis before and after cleaning."""
    
    def __init__(self, reports_dir="analysis_reports"):
        """Initialize the analyzer with a directory for reports."""
        self.reports_dir = reports_dir
        
        # Create reports directory if it doesn't exist
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)
            logger.info(f"Created reports directory: {reports_dir}")
    
    def analyze_file(self, file_path, cleaned_file_path=None, sample_size=None):
        """
        Analyze a data file and generate Sweetviz report.
        
        Args:
            file_path: Path to the original data file
            cleaned_file_path: Path to the cleaned version (if available)
            sample_size: Number of rows to sample (for very large files)
        
        Returns:
            Path to the generated HTML report
        """
        try:
            file_path = Path(file_path)
            file_name = file_path.stem
            file_extension = file_path.suffix.lower()
            
            # Load the original data
            logger.info(f"Loading data for analysis: {file_path}")
            original_df = self._read_data(file_path, sample_size)
            
            if cleaned_file_path:
                # If we have cleaned data, create a comparison report
                cleaned_file_path = Path(cleaned_file_path)
                cleaned_df = self._read_data(cleaned_file_path, sample_size)
                
                # Generate comparison report
                logger.info("Generating comparison report...")
                report = sweetviz.compare(
                    [original_df, "Original Data"], 
                    [cleaned_df, "Cleaned Data"],
                    pairwise_analysis="auto"
                )
                report_name = f"{file_name}_comparison_report"
            else:
                # Otherwise, just analyze the single file
                logger.info("Generating single data report...")
                report = sweetviz.analyze(original_df)
                report_name = f"{file_name}_report"
            
            # Save the report
            report_path = os.path.join(self.reports_dir, report_name)
            report.show_html(report_path + ".html", open_browser=False)
            logger.info(f"Report generated: {report_path}.html")
            
            return report_path + ".html"
            
        except Exception as e:
            logger.error(f"Error analyzing data: {str(e)}")
            return None
    
    def _read_data(self, file_path, sample_size=None):
        """Read data from file, with optional sampling for large files."""
        file_extension = file_path.suffix.lower()
        
        try:
            if file_extension == '.csv':
                if sample_size:
                    # Use chunking for large files
                    return pd.read_csv(file_path, nrows=sample_size)
                else:
                    return pd.read_csv(file_path)
                
            elif file_extension == '.parquet':
                df = pd.read_parquet(file_path)
                if sample_size and len(df) > sample_size:
                    return df.sample(sample_size)
                return df
                
            elif file_extension == '.json':
                df = pd.read_json(file_path)
                if sample_size and len(df) > sample_size:
                    return df.sample(sample_size)
                return df
                
            elif file_extension == '.xlsx':
                if sample_size:
                    return pd.read_excel(file_path, nrows=sample_size)
                else:
                    return pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file extension: {file_extension}")
                
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    import sys
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if len(sys.argv) < 2:
        print("Usage: python data_analyzer.py <file_path> [cleaned_file_path] [sample_size]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    cleaned_file_path = sys.argv[2] if len(sys.argv) > 2 else None
    sample_size = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    analyzer = DataAnalyzer()
    report_path = analyzer.analyze_file(file_path, cleaned_file_path, sample_size)
    
    if report_path:
        print(f"Analysis complete! Report saved to: {report_path}")
    else:
        print("Error generating report.")
        sys.exit(1)