import logging
import time
import pandas as pd
from .config_loader import load_config
from .db_connector import connect_oracle, connect_mysql
from .extractor import get_table_mappings, extract_data_from_oracle
from .transformer import get_column_mappings, transform_data
from .loader import get_target_table, load_data_to_mysql

# --- Basic Logging Setup ---
# In a larger application, you might use a more sophisticated setup (e.g., file logging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_etl_pipeline():
    """Orchestrates the entire ETL process from Oracle to MySQL."""
    start_time = time.time()
    logging.info("Starting ETL pipeline run...")

    oracle_conn = None
    mysql_conn = None
    total_success_rows = 0
    total_failed_rows = 0

    try:
        # 1. Load Configuration
        logging.info("Loading configuration...")
        config = load_config() # Assumes ../config/db_config.ini path is correct

        # 2. Establish Database Connections
        logging.info("Establishing database connections...")
        oracle_conn = connect_oracle(config)
        mysql_conn = connect_mysql(config)

        if not oracle_conn or not mysql_conn:
            logging.error("Database connection failed. Exiting pipeline.")
            return # Exit if connections are not established

        # 3. Get Table Mappings
        table_mappings = get_table_mappings(config)
        if not table_mappings:
            logging.warning("No table mappings found in the [migration] section of the config file. Nothing to migrate.")
            return

        logging.info(f"Found {len(table_mappings)} table(s) to migrate: {list(table_mappings.keys())}")

        # 4. Iterate Through Tables and Process Data
        for source_table, _ in table_mappings.items(): # We get target table inside loop using get_target_table
            table_start_time = time.time()
            logging.info(f"--- Processing table: {source_table} ---")

            target_table = get_target_table(config, source_table)
            if not target_table:
                logging.warning(f"No target table defined for source table '{source_table}'. Skipping.")
                continue

            logging.info(f"Target table: {target_table}")

            # 4a. Extract Data (Potentially Chunked)
            # Set chunk_size=None to load all at once, or e.g., 10000 for chunks
            extraction_chunk_size = config.getint('migration', 'chunk_size', fallback=10000) # Example: make chunksize configurable
            logging.info(f"Extracting data from {source_table} with chunk size: {extraction_chunk_size if extraction_chunk_size else 'All'}")
            data_source = extract_data_from_oracle(oracle_conn, source_table, chunk_size=extraction_chunk_size)

            if data_source is None:
                logging.error(f"Extraction failed for table {source_table}. Skipping.")
                continue

            # 4b. Get Column Mappings for this table
            column_mappings = get_column_mappings(config, source_table)
            logging.info(f"Column mappings for {source_table}: {column_mappings if column_mappings else 'None (using source names)'}")

            chunk_num = 0
            table_success_rows = 0
            table_failed_rows = 0

            # Process data source (either a single DataFrame or an iterator of DataFrames)
            data_iterator = [data_source] if isinstance(data_source, pd.DataFrame) else data_source

            for df_chunk in data_iterator:
                chunk_start_time = time.time()
                chunk_num += 1
                logging.info(f"Processing chunk {chunk_num} ({len(df_chunk)} rows) for {source_table} -> {target_table}")

                if df_chunk.empty:
                    logging.info(f"Chunk {chunk_num} is empty. Skipping.")
                    continue

                # 4c. Transform Data
                transformed_df = transform_data(df_chunk, column_mappings)

                # 4d. Load Data
                success, failures = load_data_to_mysql(mysql_conn, transformed_df, target_table)
                table_success_rows += success
                table_failed_rows += failures

                chunk_end_time = time.time()
                logging.info(f"Chunk {chunk_num} processing finished. Success: {success}, Failures: {failures}. Time: {chunk_end_time - chunk_start_time:.2f}s")

            table_end_time = time.time()
            logging.info(f"--- Finished processing table: {source_table} --- ")
            logging.info(f"Total rows for {source_table}: Success={table_success_rows}, Failures={table_failed_rows}")
            logging.info(f"Time taken for {source_table}: {table_end_time - table_start_time:.2f}s")

            total_success_rows += table_success_rows
            total_failed_rows += table_failed_rows

    except FileNotFoundError as e:
        logging.error(f"Configuration file error: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred during the ETL pipeline: {e}") # Use logging.exception to include traceback
    finally:
        # 5. Close Connections
        if oracle_conn:
            try:
                oracle_conn.close()
                logging.info("Oracle connection closed.")
            except Exception as e:
                logging.error(f"Error closing Oracle connection: {e}")
        if mysql_conn:
            try:
                mysql_conn.close()
                logging.info("MySQL connection closed.")
            except Exception as e:
                logging.error(f"Error closing MySQL connection: {e}")

        end_time = time.time()
        logging.info(f"ETL pipeline run finished.")
        logging.info(f"Overall Summary: Total Success Rows = {total_success_rows}, Total Failed Rows = {total_failed_rows}")
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == '__main__':
    run_etl_pipeline()
