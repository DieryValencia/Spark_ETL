# Spark ETL for Online Retail Dataset

## Overview

This project is an Extract-Transform-Load (ETL) pipeline built with **PySpark** that processes the **Online Retail Dataset** to provide comprehensive business analytics and insights. The pipeline performs data cleaning, transformation, and analysis to answer 10 key business questions, exporting results to CSV files.

## Key Features

- **Data Cleaning**: Handles missing values, formats dates, and normalizes monetary values
- **Analytics Engine**: Answers 10 specific business questions with data-driven insights
- **Robust Export**: Generates clean, single-file CSV outputs with retry mechanisms
- **Windows Compatible**: Includes Hadoop configuration for Windows environments
- **Performance Optimized**: Uses caching, coalescing, and efficient Spark configurations

## Dataset
• **Nombre: Online Retail Dataset 
• **Fuente: UCI Machine Learning Repository 
• **URL: https://archive.ics.uci.edu/ml/datasets/Online+Retail 

## Project Structure

```
spark_etl_man/
├── pyspark_etl.py              # Main ETL script
├── data/                       # Input dataset
│   └── Online_Retail.csv      # Retail transaction data (541,909 rows)
├── resultados/                  # Output analysis files 
├── .hadoop/                    # Hadoop/Windows utilities directory
├── venv/                       # Python virtual environment
├── .gitignore                  # Version control exclusions
└── README.md                   # Project documentation
```

## Technical Specifications

- **Framework**: PySpark (Spark 3.5+)
- **Dataset**: Online Retail Dataset (441,909 transactions)
- **Languages**: Python
- **Output Format**: 12 CSV files with analytics results
- **Platform**: Windows 10/11 (Hadoop-compatible)

## Business Questions Answered

1. **Total Records & Unique Invoices**: 541,909 total records, 25,900 unique invoices
2. **Unique Customers**: Identifies distinct customer base
3. **Revenue Analysis**: Total net revenue ($636,498.51) with sales and return breakdowns
4. **Top Products**: Top 10 best-selling products by quantity (sold up to 80,995 units)
5. **Top Customers**: Top 10 customers by total purchase amount
6. **Geographic Analysis**: Top 5 countries (outside UK) by revenue
7. **Ticket Metrics**: Average, min, and max invoice amounts ($89.33 average)
8. **Sales Volume**: Units sold per invoice (min: 1, max: 30, avg: 12.35)
9. **Monthly Performance**: Best performing month by revenue (December)
10. **Return Rate**: Percentage of invoices with returns (3.47%)

## Performance Metrics

- **Processing Time**: ~2-5 minutes (depending on system)
- **Memory Usage**: ~2-4 GB RAM
- **Output Files**: 12 CSV files in `resultados/` directory
- **Data Volume**: 541,909 records processed

## Setup & Installation

### Prerequisites

- Python 3.14+
- Pip (Python package manager)

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd spark_etl_man
   ```

2. **Create and activate virtual environment**:
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install required dependencies**:
   ```bash
   pip install pyspark pandas
   ```

4. **Download Hadoop utilities for Windows**:
   - Download `winutils.exe` and `hadoop.dll` (Hadoop 3.3.x compatible)
   - Extract to `.hadoop/bin/` directory
   - Download from: https://github.com/kontext-tech/winutils

## Usage

### Run the ETL Pipeline

Execute the main script to process the dataset:

```bash
python pyspark_etl.py
```

### Monitor Progress

The script provides real-time progress output:

```
[1/5] Lectura del dataset...
[2/5] Limpieza y transformacion...
[3/5] Analisis...
[4/5] Exportacion...
[5/5] Finalizacion...
```

### Verify Results

Check output files in the `resultados/` directory:

```bash
ls -la resultados/
# 12 CSV files generated
```

Sample output file (`pregunta_01_total_facturas.csv`):
```csv
metrica,valor
total_registros,541909
total_facturas_unicas,25900
```

## Key Transformations

### Data Cleaning
- **UnitPrice**: Convert comma to dot (1,55 → 1.55)
- **Quantity**: Cast to integer
- **CustomerID**: Cast to integer
- **Date Parsing**: Extract day, month, year from `dd/MM/yyyy H:mm` format
- **TotalAmount**: Exact decimal arithmetic using `DecimalType(18,2)`

### Analytics Engine
- **Revenue Calculations**: Separate sales vs. returns
- **Customer Segmentation**: Purchase frequency and total spending
- **Product Analytics**: Sales volume and ranking
- **Geographic Analysis**: Regional performance metrics
- **Temporal Analysis**: Month-over-month performance

### Export Optimization
- **Single-file CSVs**: Eliminates Spark's part-file structure
- **Retry Mechanism**: Handles file locking issues (antivirus, cloud sync)
- **Atomic Operations**: Write to temporary directory first, then rename

## Configuration Options

### Spark Configuration

The script includes optimized Spark settings:

```python
spark = (
    SparkSession.builder
    .appName("ETL_OnlineRetail")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()
)
```

### Environment Variables

- `HADOOP_HOME`: Path to Hadoop installation
- `PATH`: Includes Hadoop binaries

## Troubleshooting

### Common Issues

1. **File Access Errors**:
   - **Cause**: Antivirus or cloud sync software locking files
   - **Solution**: Move project outside OneDrive/Dropbox, ensure write permissions

2. **Hadoop Utilities Missing**:
   - **Error**: "No se encontro winutils.exe"
   - **Solution**: Download from Kontext-Tech repository

3. **Dataset Encoding Issues**:
   - **Note**: Uses ISO-8859-1 encoding for compatibility

4. **Performance Issues**:
   - **Cause**: Insufficient memory or CPU cores
   - **Solution**: Increase Spark executor memory or cores

### Debugging Tips

- **Check dataset location**: Ensure `data/Online_Retail.csv` exists
- **Monitor memory**: Watch for OOM errors with large datasets
- **Verify permissions**: Ensure write access to `resultados/` directory
- **Check Hadoop setup**: Validate `winutils.exe` and `hadoop.dll` presence

## Contributing

### Code Quality

- **Python**: PEP 8 compliant
- **Spark**: Optimized for performance and readability
- **Error Handling**: Comprehensive retry and error recovery
- **Documentation**: Inline comments and detailed headers

### Best Practices Implemented

- **Modular Design**: Clear separation of ETL phases
- **Error Recovery**: Retry mechanism for file operations
- **Performance Optimization**: Caching and efficient Spark configuration
- **Windows Compatibility**: Special handling for file locking issues

## License

This project is licensed under the MIT License. See `LICENSE` file for details.

## Contact

For questions:

- **Email**: [dieryvale.01gmail.com]

## Acknowledgements

- **Dataset**: Online Retail Dataset from UCI Machine Learning Repository
- **Tools**: PySpark, Python, Hadoop (Windows)
- **Libraries**: Apache Spark, Python standard library
- **Special Thanks**: Kontext-Tech for winutils compatibility


