"""
CSV to DuckDB Data Processing Pipeline

This module provides functionality to process CSV files and load them into DuckDB
with column mapping, phone normalization, deduplication, and schema change detection.
"""

import pandas as pd
import duckdb
import yaml
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone


class CSVToDuckDBProcessor:
    """
    A processor class for loading and transforming CSV data into DuckDB tables.
    
    This class handles:
    - Column mapping based on YAML configuration
    - Phone number normalization
    - Age column aggregation
    - Schema change detection and table recreation
    - Deduplication based on phone and address1
    - ETL timestamp tracking
    """
    def __init__(self, config_path: str, db_path: str):
        """
        Initialize the CSV to DuckDB processor.
        
        Args:
            config_path (str): Path to the YAML configuration file containing column mappings
            db_path (str): Path to the DuckDB database file
        """
        self.config_path = config_path
        self.db_path = db_path
        self.config = self._load_config()
        self.conn = duckdb.connect(db_path)
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Configuration dictionary containing column mappings and dialect settings
        """
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def normalize_phone(self, phone: str) -> Optional[str]:
        """
        Normalize phone number by removing all non-digit characters.
        
        Args:
            phone (str): Raw phone number string
            
        Returns:
            Optional[str]: Normalized phone number with digits only, or None if invalid
        """
        if pd.isna(phone) or not phone:
            return None
        phone_str = str(phone)
        normalized = re.sub(r'[^\d]', '', phone_str)
        return normalized if normalized else None
    
    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map CSV columns to target schema based on configuration.
        
        This method applies column mapping rules from the configuration file,
        handles special cases like age aggregation, normalizes phone numbers,
        and adds ETL timestamps.
        
        Args:
            df (pd.DataFrame): Source DataFrame with original CSV columns
            
        Returns:
            pd.DataFrame: Mapped DataFrame with target column names and cleaned data
        """
        column_map = self.config.get('column_map', {})
        mapped_df = pd.DataFrame()
        
        for target_col, source_cols in column_map.items():
            if target_col == 'ages_accepted':
                mapped_df[target_col] = self._aggregate_ages_columns(df, source_cols)
            else:
                mapped_df[target_col] = None
                for source_col in source_cols:
                    if source_col in df.columns:
                        mapped_df[target_col] = df[source_col]
                        break
        
        if 'phone' in mapped_df.columns:
            mapped_df['phone'] = mapped_df['phone'].apply(self.normalize_phone)
        
        # Add ETL timestamp in UTC
        current_utc = datetime.now(timezone.utc).isoformat()
        mapped_df['etl_timestamp'] = current_utc
            
        return mapped_df
    
    def _aggregate_ages_columns(self, df: pd.DataFrame, source_cols: List[str]) -> pd.Series:
        """
        Aggregate multiple age-related columns into a single comma-separated field.
        
        This method looks for age-related columns in the source data and combines
        their values into a single field, filtering out negative responses.
        
        Args:
            df (pd.DataFrame): Source DataFrame
            source_cols (List[str]): List of potential age column names to aggregate
            
        Returns:
            pd.Series: Series with comma-separated age values for each row
        """
        result = []
        
        for _, row in df.iterrows():
            ages_found = []
            for col in source_cols:
                if col in df.columns and pd.notna(row[col]) and str(row[col]).strip():
                    value = str(row[col]).strip()
                    if value and value.upper() not in ['N', 'NO', 'FALSE', '0']:
                        ages_found.append(value)
            
            # Convert list to comma-separated string for storage
            result.append(','.join(ages_found) if ages_found else None)
        
        return pd.Series(result)
    
    def get_existing_columns(self, table_name: str) -> List[str]:
        """
        Get the list of existing columns for a given table.
        
        Args:
            table_name (str): Name of the table to inspect
            
        Returns:
            List[str]: List of column names, empty list if table doesn't exist
        """
        try:
            result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            return [row[0] for row in result]
        except Exception:
            return []
    
    def schema_changed(self, table_name: str, expected_columns: List[str]) -> bool:
        """
        Check if the table schema has changed compared to expected columns.
        
        Args:
            table_name (str): Name of the table to check
            expected_columns (List[str]): List of expected column names
            
        Returns:
            bool: True if schema has changed, False otherwise
        """
        existing_columns = self.get_existing_columns(table_name)
        return set(existing_columns) != set(expected_columns)
    
    def drop_table_if_exists(self, table_name: str):
        """
        Drop a table if it exists.
        
        Args:
            table_name (str): Name of the table to drop
        """
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped existing table {table_name} due to schema changes")
    
    def create_table_if_not_exists(self, table_name: str, df: pd.DataFrame):
        """
        Create a table if it doesn't exist, or recreate if schema has changed.
        
        This method compares the expected schema with the existing table schema
        and recreates the table if there are differences.
        
        Args:
            table_name (str): Name of the table to create
            df (pd.DataFrame): DataFrame containing the data and column structure
        """
        expected_columns = list(df.columns)
        
        if self.schema_changed(table_name, expected_columns):
            print(f"Schema change detected for table {table_name}")
            print(f"Expected columns: {expected_columns}")
            print(f"Existing columns: {self.get_existing_columns(table_name)}")
            self.drop_table_if_exists(table_name)
        
        columns_sql = []
        for col in df.columns:
            if col == 'etl_timestamp':
                columns_sql.append(f"{col} TIMESTAMP")
            else:
                columns_sql.append(f"{col} VARCHAR")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns_sql)}
        )
        """
        self.conn.execute(create_sql)
    
    def record_exists(self, table_name: str, phone: str, address1: str) -> bool:
        """
        Check if a record already exists based on phone and address1 combination.
        
        This method is used for deduplication to prevent inserting duplicate records.
        
        Args:
            table_name (str): Name of the table to check
            phone (str): Phone number to check
            address1 (str): Address to check
            
        Returns:
            bool: True if record exists, False otherwise
        """
        if not phone and not address1:
            return False
            
        where_conditions = []
        params = []
        
        if phone:
            where_conditions.append("phone = ?")
            params.append(phone)
        else:
            where_conditions.append("phone IS NULL")
            
        if address1:
            where_conditions.append("address1 = ?")
            params.append(address1)
        else:
            where_conditions.append("address1 IS NULL")
        
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join(where_conditions)}"
        result = self.conn.execute(query, params).fetchone()
        return result[0] > 0
    
    def insert_new_records(self, table_name: str, df: pd.DataFrame):
        """
        Insert only new records that don't already exist in the table.
        
        This method performs deduplication by checking each record against
        existing data using phone and address1 as unique identifiers.
        
        Args:
            table_name (str): Name of the target table
            df (pd.DataFrame): DataFrame containing records to insert
        """
        new_records = []
        
        for _, row in df.iterrows():
            phone = row.get('phone')
            address1 = row.get('address1')
            
            if not self.record_exists(table_name, phone, address1):
                new_records.append(row)
        
        if new_records:
            new_df = pd.DataFrame(new_records)
            self.conn.register('new_df', new_df)
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM new_df")
            print(f"Inserted {len(new_records)} new records into {table_name}")
        else:
            print(f"No new records to insert into {table_name}")
    
    def process_csv_file(self, csv_path: str, table_name: str = "childcare_facilities"):
        """
        Process a single CSV file and load it into the specified DuckDB table.
        
        This method handles the complete ETL process for a single file:
        - Reading CSV with configured dialect settings
        - Applying column mappings and transformations
        - Creating/updating table schema
        - Inserting new records with deduplication
        
        Args:
            csv_path (str): Path to the CSV file to process
            table_name (str): Name of the target table (default: "childcare_facilities")
        """
        print(f"Processing {csv_path}")
        
        dialect_config = self.config.get('dialect', {})
        delimiter = dialect_config.get('delimiter', ',')
        encoding = dialect_config.get('encoding', 'utf-8')
        
        df = pd.read_csv(csv_path, delimiter=delimiter, encoding=encoding)
        print(f"Loaded {len(df)} records from {csv_path}")
        
        mapped_df = self.map_columns(df)
        print(f"Mapped columns: {list(mapped_df.columns)}")
        
        self.create_table_if_not_exists(table_name, mapped_df)
        
        self.insert_new_records(table_name, mapped_df)
        
    def process_all_csv_files(self, csv_folder: str, table_name: str = "childcare_facilities"):
        """
        Process all CSV files in a given folder.
        
        This method discovers and processes all CSV files in the specified folder,
        loading them into the same target table.
        
        Args:
            csv_folder (str): Path to the folder containing CSV files
            table_name (str): Name of the target table (default: "childcare_facilities")
        """
        csv_files = Path(csv_folder).glob("*.csv")
        
        for csv_file in csv_files:
            self.process_csv_file(str(csv_file), table_name)
    
    def get_table_info(self, table_name: str):
        """
        Display information about a table including record count and schema.
        
        Args:
            table_name (str): Name of the table to inspect
        """
        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            print(f"Total records in {table_name}: {result[0]}")
            
            columns_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            print(f"Table schema for {table_name}:")
            for col in columns_result:
                print(f"  {col[0]}: {col[1]}")
        except Exception as e:
            print(f"Table {table_name} does not exist or error occurred: {e}")
    
    def close(self):
        """
        Close the database connection.
        
        This method should be called when finished processing to properly
        clean up database resources.
        """
        self.conn.close()


