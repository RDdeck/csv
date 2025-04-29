import pandas as pd
import logging
from .db_connector import connect_oracle # Relative import
# config_loader is needed only for the __main__ block example now
from .config_loader import load_config

# Configure logging (if not already configured globally)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Assuming logging is configured in main_pipeline.py or elsewhere
log = logging.getLogger(__name__)

def get_table_mappings(config):
    """Extracts table mappings from the configuration.

    Args:
        config (configparser.ConfigParser): The loaded configuration object.

    Returns:
        dict: A dictionary mapping source table names (Oracle) to
              target table names (MySQL). Returns an empty dict if
              'migration' section or table mappings are missing.
    """
    if 'migration' in config:
        # Filter out non-table mappings (like column mappings)
        return {k: v for k, v in config.items('migration') if '.' not in k}
    # Example: Check if 'migration' section exists before accessing
    if 'migration' not in config:
        log.warning("Missing [migration] section in config.")
        return {}
    # Filter out non-table mappings (like column mappings or state file path)
    table_mapping_items = {}
    for key, value in config.items('migration'):
        # Simple check: assume keys without '.' are table mappings
        # More robust checks might be needed depending on config complexity
        if '.' not in key and key not in ['state_file_path', 'chunk_size']: # Exclude known non-mapping keys
             table_mapping_items[key] = value
    return table_mapping_items
    # return {k: v for k, v in config.items('migration') if '.' not in k} # Original simpler version

def extract_data_from_oracle(oracle_conn, table_name, config, high_water_mark=None, chunk_size=10000):
    """Extracts data from a specific table in Oracle, supporting incremental loads.

    Args:
        oracle_conn (cx_Oracle.Connection): An active Oracle database connection.
        table_name (str): The name of the Oracle table to extract data from.
        config (configparser.ConfigParser): The loaded configuration object.
        high_water_mark (any, optional): The last value recorded for the
                                         incremental column. Defaults to None (full load).
        chunk_size (int, optional): The number of rows to fetch per chunk.
                                    If None or 0, fetches all data at once.
                                    Defaults to 10000.

    Returns:
        pandas.DataFrame or iterator: A DataFrame or an iterator yielding
                                      DataFrames. Returns None if extraction fails.
    """
    query_params = {}
    incremental_column = None
    load_type = "Full"

    # Check config for incremental column specific to this table
    incremental_column_key = f"{table_name}.incremental_column"
    if 'migration' in config and config.has_option('migration', incremental_column_key):
        incremental_column = config.get('migration', incremental_column_key)
        log.info(f"Found incremental column for {table_name}: {incremental_column}")
    else:
        log.info(f"No incremental column configured for {table_name}. Performing full load.")

    # Build the base query
    query = f"SELECT * FROM {table_name}"

    # Add WHERE clause for incremental load if applicable
    if incremental_column and high_water_mark is not None:
        load_type = "Incremental"
        query += f" WHERE {incremental_column} > :hwm"
        query_params['hwm'] = high_water_mark
        log.info(f"Incremental load for {table_name}: Filtering where {incremental_column} > {high_water_mark}")
    elif incremental_column and high_water_mark is None:
        log.info(f"Incremental column '{incremental_column}' configured for {table_name}, but no high_water_mark provided. Performing initial full load (ordered).")
    else:
        log.info(f"Performing full load for {table_name}.")


    # Add ORDER BY clause if incremental column is defined (ensures chronological processing)
    if incremental_column:
        query += f" ORDER BY {incremental_column} ASC"
        log.info(f"Ordering results by {incremental_column}.")

    log.info(f"Executing {load_type} extraction query on Oracle for {table_name}. Query structure: {query.split('WHERE')[0]}...") # Log query structure without params

    try:
        if chunk_size and chunk_size > 0:
            # Return an iterator for chunked reading, passing parameters
            log.info(f"Using chunk size: {chunk_size}")
            return pd.read_sql(query, oracle_conn, chunksize=chunk_size, params=query_params)
        else:
            # Read all data into a single DataFrame, passing parameters
            log.info("Fetching all data (no chunking).")
            df = pd.read_sql(query, oracle_conn, params=query_params)
            log.info(f"Extracted {len(df)} rows from {table_name} ({load_type} load).")
            return df
    except pd.io.sql.DatabaseError as e:
        log.error(f"Database error extracting data from Oracle table {table_name}: {e}", exc_info=True)
        # Could be table not found, permissions error, etc.
        return None
    except Exception as e:
        log.error(f"An unexpected error occurred during extraction from {table_name}: {e}", exc_info=True)
        return None

if __name__ == '__main__':
    # Example Usage: Load config, connect, and extract data from mapped tables
    # Note: This example doesn't fully simulate state/high_water_mark passing
    try:
        config = load_config() # Load config for the example
        table_mappings = get_table_mappings(config) # Use updated get_table_mappings

        if not table_mappings:
            log.info("No table mappings found in [migration] section of config.")
        else:
            oracle_conn = connect_oracle(config)
            if oracle_conn:
                log.info("\n--- Starting Extraction Process (Example Usage) ---")
                all_data = {} # Dictionary to hold dataframes or iterators

                # --- Mock State for Example ---
                # In real pipeline, this comes from state_manager
                mock_state = {'oracle_employees': '2023-01-15 00:00:00'}
                log.info(f"Using mock state for example: {mock_state}")
                # -----------------------------

                for source_table, target_table in table_mappings.items():
                    log.info(f"\nExtracting data for mapping: {source_table} -> {target_table}")

                    # --- Get High Water Mark for this table from mock state ---
                    current_hwm = mock_state.get(source_table)
                    log.info(f"High water mark for {source_table}: {current_hwm}")
                    # --------------------------------------------------------

                    # Example: Extract data, passing config and HWM
                    # Using chunk_size=None for simplicity in this example output
                    data = extract_data_from_oracle(
                        oracle_conn=oracle_conn,
                        table_name=source_table,
                        config=config, # Pass the loaded config
                        high_water_mark=current_hwm, # Pass the HWM
                        chunk_size=None # Example: Load all at once
                    )

                    if data is not None:
                        if isinstance(data, pd.DataFrame):
                             log.info(f"Successfully extracted {len(data)} rows from {source_table}.")
                             all_data[source_table] = data
                             # log.info(data.head()) # Optional: Print head
                        else: # It's an iterator (if chunk_size > 0)
                             log.info(f"Successfully created iterator for {source_table} (chunked extraction).")
                             # Example: Consume iterator to see results (not typical here)
                             # chunk_count = 0
                             # total_rows = 0
                             # for i, chunk in enumerate(data):
                             #     log.info(f"  Chunk {i+1}: {len(chunk)} rows")
                             #     total_rows += len(chunk)
                             #     chunk_count += 1
                             # log.info(f"Iterator yielded {chunk_count} chunks, {total_rows} total rows.")
                             all_data[source_table] = data # Store the iterator itself
                    else:
                        log.warning(f"Extraction failed for table {source_table}.")

                # Close Oracle connection when done
                oracle_conn.close()
                log.info("\nOracle connection closed.")
            else:
                log.error("Cannot proceed with extraction, Oracle connection failed.")

    except FileNotFoundError as e:
        log.error(f"Configuration file error: {e}", exc_info=True)
    except Exception as e:
        log.error(f"An error occurred during extraction testing: {e}", exc_info=True)
