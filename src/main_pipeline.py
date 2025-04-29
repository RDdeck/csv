import logging
import time
import pandas as pd
from .config_loader import load_config
from .db_connector import connect_oracle, connect_mysql
from .extractor import get_table_mappings, extract_data_from_oracle
from .transformer import get_column_mappings, transform_data
from .loader import get_target_table, load_data_to_mysql
from .state_manager import load_state, save_state # Added state manager imports

# --- Basic Logging Setup ---
# In a larger application, you might use a more sophisticated setup (e.g., file logging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_etl_pipeline():
    """Orchestrates the entire ETL process from Oracle to MySQL."""
    start_time = time.time()
    logging.info("Starting ETL pipeline run...")

    oracle_conn = None
    mysql_conn = None
    total_loaded_rows = 0 # Renamed for clarity with upsert
    total_failed_rows = 0
    state_file_path = 'pipeline_state.json' # Default, will be overwritten by config
    current_state = {}
    new_state = {}

    try:
        # 1. Load Configuration
        logging.info("Loading configuration...")
        config = load_config() # Assumes ../config/db_config.ini is relative to config_loader.py

        # 1b. Load Pipeline State
        state_file_path = config.get('migration', 'state_file_path', fallback='pipeline_state.json')
        logging.info(f"Loading pipeline state from: {state_file_path}")
        current_state = load_state(state_file_path)
        new_state = current_state.copy() # Initialize new state with current state
        logging.info(f"Loaded initial state: {current_state}")

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

            # --- Incremental Load Setup ---
            hwm = current_state.get(source_table) # Get High Water Mark for this table
            inc_col_key = f"{source_table}.incremental_column"
            inc_col = config.get('migration', inc_col_key, fallback=None)
            max_hwm_for_table = hwm # Initialize max HWM for this table run with the previous value

            if inc_col:
                logging.info(f"Incremental column for {source_table}: {inc_col}")
                if hwm is not None: # Check explicitly for None
                    logging.info(f"Found high-water mark for {source_table}: {hwm}")
                else:
                    logging.info(f"No high-water mark found for {source_table}. Will perform initial load (if incremental column is set).")
            else:
                 logging.info(f"No incremental column configured for {source_table}. Performing full load.")
            # --- End Incremental Load Setup ---

            target_table = get_target_table(config, source_table)
            if not target_table:
                logging.warning(f"No target table defined for source table '{source_table}'. Skipping.")
                continue

            logging.info(f"Target table: {target_table}")

            # 4a. Extract Data (Passing config and hwm)
            extraction_chunk_size = config.getint('migration', 'chunk_size', fallback=10000)
            logging.info(f"Extracting data from {source_table} (Incremental: {bool(inc_col)}, HWM: {hwm}) with chunk size: {extraction_chunk_size if extraction_chunk_size else 'All'}")
            data_source = extract_data_from_oracle(
                oracle_conn=oracle_conn,
                table_name=source_table,
                config=config, # Pass config object
                chunk_size=extraction_chunk_size,
                high_water_mark=hwm # Pass high water mark
            )

            if data_source is None:
                logging.error(f"Extraction failed for table {source_table}. Skipping table.")
                continue

            # 4b. Get Column Mappings for this table
            column_mappings = get_column_mappings(config, source_table)
            logging.info(f"Column mappings for {source_table}: {column_mappings if column_mappings else 'None (using source names)'}")

            chunk_num = 0
            table_loaded_rows = 0 # Renamed from table_success_rows
            table_failed_rows = 0
            processed_rows_in_table = 0 # Track total rows processed from source

            # Process data source (either a single DataFrame or an iterator of DataFrames)
            data_iterator = [data_source] if isinstance(data_source, pd.DataFrame) else data_source

            for df_chunk in data_iterator:
                chunk_start_time = time.time()
                chunk_num += 1
                rows_in_chunk = len(df_chunk)
                processed_rows_in_table += rows_in_chunk
                logging.info(f"Processing chunk {chunk_num} ({rows_in_chunk} rows) for {source_table} -> {target_table}")

                if df_chunk.empty:
                    logging.info(f"Chunk {chunk_num} is empty. Skipping processing.")
                    continue

                # --- Track Max HWM from Chunk ---
                if inc_col and inc_col in df_chunk.columns and not df_chunk[inc_col].isnull().all():
                    try:
                        # Make sure column is of a comparable type, drop NaNs before max()
                        comparable_series = pd.to_numeric(df_chunk[inc_col], errors='coerce').dropna()
                        if not comparable_series.empty:
                            chunk_max_hwm = comparable_series.max()

                            # Comparison logic: update max_hwm_for_table if chunk_max_hwm is greater
                            # Handle potential type mismatches (e.g., comparing number to string)
                            if max_hwm_for_table is None or chunk_max_hwm > type(chunk_max_hwm)(max_hwm_for_table):
                               logging.debug(f"Updating max HWM for {source_table} from {max_hwm_for_table} to {chunk_max_hwm}")
                               max_hwm_for_table = chunk_max_hwm
                        else:
                             logging.debug(f"Incremental column '{inc_col}' in chunk {chunk_num} for {source_table} contains only non-comparable or NaN values.")

                    except TypeError as te:
                        logging.warning(f"Could not compare HWM values for column '{inc_col}' in chunk {chunk_num} of {source_table}. Check data types. Error: {te}")
                    except Exception as e:
                        logging.error(f"Error finding max HWM in chunk {chunk_num} for {source_table}: {e}", exc_info=True)
                # --- End HWM Tracking ---

                # 4c. Transform Data
                transformed_df = transform_data(df_chunk, column_mappings)

                # 4d. Load Data
                affected_rows, failures = load_data_to_mysql(mysql_conn, transformed_df, target_table)
                table_loaded_rows += affected_rows # Accumulate affected rows (upserts)
                table_failed_rows += failures # Accumulate failures across chunks

                chunk_end_time = time.time()
                logging.info(f"Chunk {chunk_num} processing finished. Affected Rows: {affected_rows}, Failures: {failures}. Time: {chunk_end_time - chunk_start_time:.2f}s")

                # Optional: Add check here to break early if failures exceed a threshold

            # --- Update State After Processing All Chunks for Table ---
            table_end_time = time.time()
            logging.info(f"--- Finished processing table: {source_table} --- ")
            logging.info(f"Source rows processed for {source_table}: {processed_rows_in_table}")
            logging.info(f"Total rows for {source_table}: Affected in Target DB={table_loaded_rows}, Assumed Failures={table_failed_rows}")
            logging.info(f"Time taken for {source_table}: {table_end_time - table_start_time:.2f}s")

            if inc_col and table_failed_rows == 0 and processed_rows_in_table > 0:
                # Check if max_hwm_for_table has a new, valid value compared to the initial hwm
                if max_hwm_for_table is not None and max_hwm_for_table != hwm:
                    # Convert potential pandas Timestamp or numpy types to standard types for JSON
                    if isinstance(max_hwm_for_table, pd.Timestamp):
                        state_value = max_hwm_for_table.isoformat()
                    elif hasattr(max_hwm_for_table, 'item'): # Handle numpy types like int64, float64
                        state_value = max_hwm_for_table.item()
                    else:
                        state_value = max_hwm_for_table # Assumes numbers or existing strings are fine

                    logging.info(f"Updating state for {source_table} to new high-water mark: {state_value}")
                    new_state[source_table] = state_value
                elif max_hwm_for_table is not None and max_hwm_for_table == hwm:
                     logging.info(f"No new high-water mark found for {source_table} (max value {max_hwm_for_table} matches initial HWM {hwm}). State not updated.")
                else:
                     # This case might happen if the incremental column was all NULLs, empty chunks, or max HWM remained None
                     logging.info(f"No valid new high-water mark determined for {source_table} in this run. State not updated.")
            elif inc_col and table_failed_rows > 0:
                logging.warning(f"Failures occurred during processing for {source_table}. State will not be updated for this table to ensure data consistency.")
            elif inc_col and processed_rows_in_table == 0:
                logging.info(f"No rows were processed for {source_table} (potentially due to HWM). State remains unchanged.")
            # --- End State Update Logic ---

            total_loaded_rows += table_loaded_rows
            total_failed_rows += table_failed_rows

    except FileNotFoundError as e:
        logging.error(f"Configuration file error: {e}")
    except Exception as e:
        logging.exception(f"An unexpected error occurred during the ETL pipeline: {e}") # Use logging.exception to include traceback
    finally:
        # 5. Save Final State (Important: Do this before closing connections)
        # Save state regardless of errors in individual tables, to capture progress of successful ones.
        if new_state != current_state:
            logging.info(f"Saving updated pipeline state to: {state_file_path}")
            logging.debug(f"Final state to save: {new_state}") # Use debug for potentially large state dict
            try:
                save_state(state_file_path, new_state)
            except Exception as state_save_e:
                logging.error(f"Failed to save final pipeline state to {state_file_path}: {state_save_e}", exc_info=True)
        else:
            logging.info("No state changes detected during the run. State file not updated.")

        # 6. Close Connections
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
        logging.info(f"Overall Summary: Total Affected Rows (Upserts) = {total_loaded_rows}, Total Failed Rows = {total_failed_rows}")
        logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")


if __name__ == '__main__':
    run_etl_pipeline()
