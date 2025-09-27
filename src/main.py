"""
Main ETL Pipeline Controller

This module serves as the entry point for the data processing pipeline,
orchestrating CSV data ingestion, transformation, and dbt model execution.
"""

import sys
import duckdb
import subprocess
from sync_files import CSVToDuckDBProcessor


def run_export(table_name: str):
    """
    Export a DuckDB table to CSV format.
    
    This function connects to the DuckDB warehouse and exports the specified
    table to a CSV file in the exports directory.
    
    Args:
        table_name (str): Name of the table to export
    """
    print(f"Export table '{table_name}' from DuckDB to CSV...")
    with duckdb.connect("/app/duckdb/warehouse.duckdb") as duckdb_conn:
        duckdb_conn.execute(f"COPY {table_name} TO '/app/duckdb/exports/{table_name}.csv' (HEADER, DELIMITER ',')")   


def sync_data_files():
    """
    Synchronize CSV data files to DuckDB.
    
    This function processes all CSV files in the source directory,
    applies transformations, and loads them into the DuckDB warehouse.
    
    Raises:
        SystemExit: If an error occurs during processing
    """
    config_path = "/app/config/sources.yaml"
    db_path = "/app/duckdb/warehouse.duckdb"
    csv_folder = "/app/files"

    processor = CSVToDuckDBProcessor(config_path, db_path)
    try:
        processor.process_all_csv_files(csv_folder)
        processor.get_table_info("childcare_facilities")
    except Exception as e:
        print("Exception occurred during sync data files:", e)
        sys.exit(1)  # Exit with error
    finally:
        processor.close()


def main(args: list):
    """
    Main entry point for the ETL pipeline.
    
    This function dispatches to different pipeline stages based on the
    provided arguments. Supported stages:
    - 'export_table': Export a specific table to CSV
    - 'sync_files': Synchronize CSV files to DuckDB
    - 'dbt*': Execute dbt commands (any argument starting with 'dbt')
    
    Args:
        args (list): Command line arguments
                    For csv_export: ['csv_export', table_name]
                    For sync_files: ['sync_files']
                    For dbt commands: ['dbt', 'run'] or ['dbt', 'test'] etc.
    
    Raises:
        SystemExit: If an error occurs during dbt processing
        Exception: If invalid arguments are provided
    """
    data_stage = args[0]

    if 'export_table' in data_stage:
        run_export(data_stage.split(' ')[-1])

    elif data_stage == 'sync_files':
        sync_data_files()

    elif data_stage.startswith("dbt"):
        try:
            dbt_commands = args
            dbt_setup_call = ["bash", "/app/scripts/dbt_run_commands.sh"] + dbt_commands
            subprocess.run(dbt_setup_call)

        except Exception as e:
            print("Exception occurred during dbt data processing:", e)
        sys.exit(1)  # Exit with error

    else:
        raise Exception(f"Arguments '{args}' are invalid!")


if __name__ == "__main__":
    main(sys.argv[1:])
