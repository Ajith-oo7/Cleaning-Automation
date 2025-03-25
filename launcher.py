"""
Launcher script for the data cleaning system.
Provides an easy way to generate test data and run different cleaning processes.
"""

import os
import sys
import time
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Data Cleaning System Launcher")
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Generate test data command
    generate_parser = subparsers.add_parser("generate", help="Generate test data")
    generate_parser.add_argument("--rows", type=int, default=10000, help="Number of rows to generate")
    generate_parser.add_argument("--output", type=str, default="sample_data.csv", help="Output file path")
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Start folder monitoring")
    monitor_parser.add_argument("--dir", type=str, default=None, help="Directory to monitor (default: current directory)")
    monitor_parser.add_argument("--reports", action="store_true", help="Generate Sweetviz reports during cleaning")
    
    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean a specific file")
    clean_parser.add_argument("file", type=str, help="File to clean")
    clean_parser.add_argument("--engine", type=str, choices=["auto", "pandas", "dask", "spark"], default="auto", 
                             help="Processing engine to use")
    clean_parser.add_argument("--memory", type=str, default="4g", help="Memory allocation for Spark (if used)")
    clean_parser.add_argument("--reports", action="store_true", help="Generate Sweetviz reports")
    clean_parser.add_argument("--sample", type=int, default=10000, 
                            help="Sample size for reports with large files (default: 10000)")
    
    # Benchmark command
    benchmark_parser = subparsers.add_parser("benchmark", help="Run performance benchmark")
    benchmark_parser.add_argument("--sizes", type=str, default="10000,50000,100000,500000", 
                                 help="Comma-separated list of data sizes to benchmark")
    
    # Add analyze command for generating reports without cleaning
    analyze_parser = subparsers.add_parser("analyze", help="Generate data analysis report")
    analyze_parser.add_argument("file", type=str, help="File to analyze")
    analyze_parser.add_argument("--cleaned", type=str, default=None, 
                              help="Optional cleaned file to compare with original")
    analyze_parser.add_argument("--sample", type=int, default=10000, 
                              help="Sample size for large files (default: 10000)")
    
    args = parser.parse_args()
    
    if args.command == "generate":
        # Import here to avoid loading all modules for every command
        from generate_test_data import generate_test_data
        print(f"Generating {args.rows} rows of test data...")
        generate_test_data(rows=args.rows, output_file=args.output)
        
    elif args.command == "monitor":
        # Set environment variable if directory specified
        if args.dir:
            os.environ["MONITOR_DIR"] = args.dir
        
        # Set report generation flag
        if args.reports:
            os.environ["GENERATE_REPORTS"] = "True"
        else:
            os.environ["GENERATE_REPORTS"] = "False"
        
        # Import and run the file monitor
        from data_cleaner import run_file_monitor
        run_file_monitor()
        
    elif args.command == "clean":
        file_path = Path(args.file)
        
        if not file_path.exists():
            print(f"Error: File '{file_path}' does not exist")
            return 1
        
        # Set report generation flags
        if args.reports:
            os.environ["GENERATE_REPORTS"] = "True"
            os.environ["REPORT_SAMPLE_SIZE"] = str(args.sample)
        else:
            os.environ["GENERATE_REPORTS"] = "False"
        
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        engine = args.engine
        
        # Auto-select engine based on file size
        if engine == "auto":
            if file_size_mb < 100:
                engine = "pandas"
            elif file_size_mb < 1000:
                engine = "dask"
            else:
                engine = "spark"
            
            print(f"Auto-selected engine '{engine}' for {file_size_mb:.2f} MB file")
        
        # Use the selected engine
        if engine == "pandas" or engine == "dask":
            from data_cleaner import DataCleaner
            cleaner = DataCleaner()
            cleaner.clean_data(file_path)
            
        elif engine == "spark":
            try:
                from spark_cleaner import SparkDataCleaner
                cleaner = SparkDataCleaner(memory=args.memory)
                cleaner.clean_data(file_path)
                cleaner.stop()
            except ImportError:
                print("Error: PySpark is not installed. Please install it with:")
                print("pip install pyspark")
                return 1
        
    elif args.command == "benchmark":
        try:
            from benchmark import run_benchmark
            sizes = [int(s) for s in args.sizes.split(",")]
            run_benchmark(sizes=sizes)
        except ImportError as e:
            print(f"Error: {str(e)}")
            print("Make sure all benchmark dependencies are installed.")
            return 1
    
    elif args.command == "analyze":
        try:
            from data_analyzer import DataAnalyzer
            file_path = Path(args.file)
            
            if not file_path.exists():
                print(f"Error: File '{file_path}' does not exist")
                return 1
            
            cleaned_path = args.cleaned
            if cleaned_path and not Path(cleaned_path).exists():
                print(f"Error: Cleaned file '{cleaned_path}' does not exist")
                return 1
            
            analyzer = DataAnalyzer()
            report_path = analyzer.analyze_file(file_path, cleaned_path, args.sample)
            
            if report_path:
                print(f"Analysis complete! Report saved to: {report_path}")
            else:
                print("Error generating report.")
                return 1
                
        except ImportError:
            print("Error: Sweetviz is not installed. Please install it with:")
            print("pip install sweetviz")
            return 1
    
    else:
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())