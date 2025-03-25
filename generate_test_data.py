import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

def generate_test_data(rows=10000, output_file='sample_data.csv'):
    """Generate sample data with various issues for testing the data cleaner."""
    
    print(f"Generating test data with {rows} rows...")
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Create a date range
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(rows)]
    
    # Create numeric data with outliers and missing values
    numeric_data1 = np.random.normal(100, 15, rows)
    numeric_data2 = np.random.normal(50, 10, rows)
    
    # Add outliers (5% of the data)
    outlier_indices = np.random.choice(rows, size=int(rows * 0.05), replace=False)
    numeric_data1[outlier_indices] = np.random.normal(200, 30, len(outlier_indices))
    numeric_data2[outlier_indices] = np.random.normal(150, 20, len(outlier_indices))
    
    # Create categorical data with inconsistencies and missing values
    categories = ['category_a', 'category_b', 'category_c', 'Category_A', 'CATEGORY_B', 'category_c  ']
    categorical_data = np.random.choice(categories, rows)
    
    # Create ID column with some duplicates
    ids = list(range(1, rows + 1))
    # Replace some IDs with duplicates (3% of data)
    duplicate_indices = np.random.choice(rows, size=int(rows * 0.03), replace=False)
    for idx in duplicate_indices:
        ids[idx] = np.random.choice(ids[:idx])
    
    # Create the DataFrame
    df = pd.DataFrame({
        'id': ids,
        'date': dates,
        'numeric_value1': numeric_data1,
        'numeric_value2': numeric_data2,
        'category': categorical_data
    })
    
    # Add missing values (10% of data)
    for col in df.columns:
        if col != 'id':  # Keep IDs intact
            missing_indices = np.random.choice(rows, size=int(rows * 0.1), replace=False)
            df.loc[missing_indices, col] = np.nan
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
    print(f"File size: {os.path.getsize(output_file) / (1024 * 1024):.2f} MB")
    
    # Display data statistics
    print("\nData Statistics:")
    print(f"Total rows: {len(df)}")
    print(f"Duplicate IDs: {len(df) - df['id'].nunique()}")
    print(f"Missing values: {df.isna().sum().sum()}")
    print(f"Outliers (approx): {len(outlier_indices)}")
    
    return df

if __name__ == "__main__":
    # Generate a small test file
    generate_test_data(rows=10000, output_file='sample_data.csv')
    
    # Uncomment to generate a larger test file
    # generate_test_data(rows=1000000, output_file='large_sample_data.csv')