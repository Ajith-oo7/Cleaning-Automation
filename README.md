# Automated Data Cleaning for Large Datasets

This project automates the process of cleaning large datasets (up to 500GB) by monitoring a folder for new files and automatically processing them when they arrive.

## Features

- **Automatic File Monitoring**: Watches a specified folder for new data files
- **Large Data Support**: Uses Dask for processing files over 100MB
- **Multiple File Formats**: Supports CSV, Excel, Parquet, and JSON files
- **Comprehensive Data Cleaning**: 
  - Removes duplicates
  - Handles missing values
  - Eliminates outliers
  - Standardizes text data
- **Automatic Output**: Creates cleaned files with "_cleaned" suffix
- **Scalable Processing**: Supports Pandas, Dask, and PySpark engines for different data sizes
- **Data Visualization Reports**: Uses Sweetviz to generate comprehensive visual reports