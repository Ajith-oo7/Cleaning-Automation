import os
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from generate_test_data import generate_test_data
from data_cleaner import DataCleaner
import shutil

def run_benchmark(sizes=[10_000, 100_000, 500_000, 1_000_000]):
    """Run performance benchmark on different data sizes."""
    
    results = []
    
    print("Starting benchmark...")
    
    # Create a directory for benchmark results
    if not os.path.exists('benchmark_results'):
        os.makedirs('benchmark_results')
    
    # Create directory for benchmark data
    data_dir = 'benchmark_data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    cleaner = DataCleaner()
    
    for size in sizes:
        print(f"\nBenchmarking with {size:,} rows...")
        
        # Generate test data
        filename = f"{data_dir}/data_{size}.csv"
        df = generate_test_data(rows=size, output_file=filename)
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        
        # Measure cleaning time
        start_time = time.time()
        cleaner.clean_data(filename)
        processing_time = time.time() - start_time
        
        # Save results
        results.append({
            'rows': size,
            'file_size_mb': file_size_mb,
            'processing_time_sec': processing_time,
            'rows_per_sec': size / processing_time
        })
        
        print(f"Processed {size:,} rows in {processing_time:.2f} seconds")
        print(f"Speed: {size / processing_time:.2f} rows/second")
    
    # Create results DataFrame
    results_df = pd.DataFrame(results)
    results_df.to_csv('benchmark_results/performance_results.csv', index=False)
    
    # Plot results
    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 2, 1)
    plt.plot(results_df['rows'], results_df['processing_time_sec'], 'o-')
    plt.xlabel('Number of Rows')
    plt.ylabel('Processing Time (seconds)')
    plt.title('Processing Time vs. Data Size')
    plt.grid(True)
    
    plt.subplot(1, 2, 2)
    plt.plot(results_df['rows'], results_df['rows_per_sec'], 'o-')
    plt.xlabel('Number of Rows')
    plt.ylabel('Rows Processed per Second')
    plt.title('Processing Speed vs. Data Size')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('benchmark_results/performance_chart.png')
    
    print("\nBenchmark completed. Results saved to benchmark_results/")
    
    # Option to clean up
    cleanup = input("\nDo you want to clean up the benchmark data files? (y/n): ")
    if cleanup.lower() == 'y':
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
            print(f"Cleaned up {data_dir} directory")
    
    return results_df

if __name__ == "__main__":
    # Default benchmark sizes
    sizes = [10_000, 50_000, 100_000, 500_000]
    
    # Allow custom sizes from command line
    import sys
    if len(sys.argv) > 1:
        try:
            sizes = [int(s) for s in sys.argv[1:]]
        except ValueError:
            print("Error: All arguments must be integers representing data sizes")
            sys.exit(1)
    
    results = run_benchmark(sizes)