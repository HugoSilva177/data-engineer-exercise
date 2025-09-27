# Childcare Facilities Data Engineering Pipeline

A comprehensive ETL pipeline for processing childcare facility data from multiple CSV sources into a standardized DuckDB data warehouse with dbt transformations.

## Overview

This project processes childcare facility data with different schemas and formats, normalizing them into a unified data model. The pipeline handles column mapping, data cleaning, deduplication, and incremental updates.

## Architecture

```
CSV Files → Python ETL → DuckDB → dbt Models → Clean Data
    ↓           ↓           ↓          ↓           ↓
Source Data  Mapping &   Raw Data   Transform   Analytics
             Cleaning   Storage     & Test      Ready
```

## Features

- **Dynamic Column Mapping**: YAML-based configuration for flexible field mapping
- **Phone Normalization**: Standardizes phone numbers to digits-only format
- **Age Aggregation**: Combines multiple age-related columns into unified format
- **Schema Change Detection**: Automatically recreates tables when structure changes
- **Deduplication**: Prevents duplicate records using phone + address combination
- **Incremental Processing**: Only processes new data based on ETL timestamps
- **Data Quality Tests**: dbt tests ensure data integrity
- **Dockerized Environment**: Consistent deployment across environments

## Installation & Setup

### Prerequisites

- Docker
- Python 3.9+
- Git

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone git@github.com:HugoSilva177/data-engineer-exercise.git
   cd data-engineer-exercise
   ```

2. **Make the run script executable**
   ```bash
   chmod +x run.sh
   ```

3. **Complete workflow example**
   ```bash
   # Sync CSV files to DuckDB raw source
   ./run.sh sync_files
   
   # Run dbt transformations
   ./run.sh dbt run
   
   # Run dbt quality checks
   ./run.sh dbt test
   
   # Export table to CSV into duckdb/exports/ folder
   ./run.sh export_table cleaned_childcare_facilities
   ```

   **Full refresh for schema changes:**
   ```bash
   ./run.sh dbt run --full-refresh
   ```

## Configuration

### Column Mapping (src/config/sources.yaml)

The pipeline uses a YAML configuration file to map varying CSV column names to standardized fields.

### Database Configuration

- **Database**: DuckDB (local file-based)
- **Location**: `duckdb/warehouse.duckdb`
- **Tables**: 
  - `childcare_facilities` (raw data)
  - `cleaned_childcare_facilities` (dbt model and final output)

## Data Flow

1. **Ingestion**: CSV files are read from `src/files/` directory
2. **Mapping**: Columns are mapped according to `sources.yaml` configuration
3. **Transformation**: 
   - Phone numbers normalized (digits only)
   - Age columns aggregated into comma-separated values
   - ETL timestamps added
4. **Loading**: Data loaded into DuckDB with deduplication
5. **dbt Processing**: 
   - Incremental models process new data
   - Data quality tests validate results
   - Clean, analytics-ready tables created

## Project Structure

```
├── src/
│   ├── config/
│   │   └── sources.yaml          # Column mapping configuration
│   ├── files/                    # Source CSV files
│   │   ├── source_1.csv
│   │   ├── source_2.csv
│   │   └── source_3.csv
│   ├── main.py                   # Main ETL controller
│   ├── sync_files.py            # CSV processing logic
│   └── scripts/
│       └── dbt_run_commands.sh   # dbt execution script
├── dbt_project/
│   ├── models/
│   │   ├── _sources.yml          # dbt source definitions
│   │   └── cleaned_source/
│   │       ├── cleaned_childcare_facilities.sql
│   │       └── cleaned_childcare_facilities.yml        # dbt tests and documentation
│   ├── dbt_project.yml
│   └── profiles.yml
├── duckdb/
│   └── warehouse.duckdb          # DuckDB database file
├── requirements.txt
├── Dockerfile
├── README.md
└── run.sh
```

## Data Quality & Testing

### Python-level Quality Checks
- Phone number validation (digits only, minimum length)
- Address normalization and cleaning
- Deduplication logic prevents duplicate records
- Schema change detection ensures data integrity

### dbt Tests
- **not_null**: Ensures critical fields (phone, address1) are not null
- **unique**: Validates unique combinations of phone + address1
- **accepted_values**: Checks for valid capacity values
- **custom tests**: Expression-based tests for complex validations

## Tradeoffs & Design Decisions

### What Was Implemented
- ✅ **Column mapping flexibility**: YAML-based configuration allows easy adaptation to new data sources
- ✅ **Incremental processing**: Only processes new data based on timestamps
- ✅ **Data quality**: Multiple layers of validation and testing
- ✅ **Schema evolution**: Automatic table recreation when schemas change
- ✅ **Documentation**: Comprehensive docstrings and README

### Tradeoffs Made
- **File-based DuckDB**: Chose simplicity over distributed databases for this scope
- **Python pandas**: Used for familiarity, though could be slower for very large datasets
- **Basic deduplication**: Simple phone+address matching vs sophisticated fuzzy matching
- **Manual configuration**: YAML mapping requires manual updates for new sources

### What Was Left Out (Time Constraints)
- **Error handling**: More robust error recovery and retry mechanisms
- **Monitoring**: Observability, logging, and alerting systems
- **Data lineage**: Tracking data flow and transformation history
- **Performance optimization**: Query optimization, indexing strategies
- **Security**: Data encryption, access controls, PII handling
- **CI/CD pipeline**: Automated testing and deployment
- **Orchestration**: Airflow service to orchestrate the data pipeline.
- **Data validation**: More sophisticated data quality rules
- **Dimensional data modeling**: Create dimension and fact tables on dbt for analytics.

### What I'd Do Differently with More Time

1. **Enhanced Error Handling**
   - Implement retry logic with exponential backoff
   - Detailed error logging and notification systems
   - Graceful handling of schema mismatches

2. **Performance Optimization**
   - Batch processing for large datasets
   - Parallel processing of multiple CSV files
   - Database indexing strategies
   - Memory-efficient streaming for large files

3. **Advanced Data Quality**
   - Fuzzy matching for deduplication
   - Data profiling and anomaly detection
   - Statistical validation rules
   - Data freshness checks

4. **Scalability Improvements**
   - Support for cloud storage (S3, GCS)
   - Distributed processing
   - Auto-scaling capabilities

### Current Stack
- **Storage**: DuckDB (local file)
- **Processing**: Python/Pandas and dbt
- **Orchestration**: Manual execution
- **Testing**: dbt tests
- **Deployment**: Docker

---
**Built with ❤️ for efficient childcare data processing**