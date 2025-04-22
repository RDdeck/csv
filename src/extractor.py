import pandas as pd
from .db_connector import connect_oracle # Relative import
from .config_loader import load_config # Relative import

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
    return {}

def extract_data_from_oracle(oracle_conn, table_name, chunk_size=10000):
    """Extracts data from a specific table in Oracle, potentially in chunks.

    Args:
        oracle_conn (cx_Oracle.Connection): An active Oracle database connection.
        table_name (str): The name of the Oracle table to extract data from.
        chunk_size (int, optional): The number of rows to fetch per chunk.
                                    If None or 0, fetches all data at once.
                                    Defaults to 10000.

    Returns:
        pandas.DataFrame or iterator: If chunk_size is None or 0, returns a
                                      single DataFrame. If chunk_size > 0,
                                      returns an iterator yielding DataFrames.
                                      Returns None if extraction fails.
    """
    query = f"SELECT * FROM {table_name}"
    print(f"Executing query on Oracle: {query}")
    try:
        if chunk_size and chunk_size > 0:
            # Return an iterator for chunked reading
            return pd.read_sql(query, oracle_conn, chunksize=chunk_size)
        else:
            # Read all data into a single DataFrame
            df = pd.read_sql(query, oracle_conn)
            print(f"Extracted {len(df)} rows from {table_name}.")
            return df
    except pd.io.sql.DatabaseError as e:
        print(f"Error extracting data from Oracle table {table_name}: {e}")
        # Could be table not found, permissions error, etc.
        return None
    except Exception as e:
        print(f"An unexpected error occurred during extraction from {table_name}: {e}")
        return None

if __name__ == '__main__':
    # Example Usage: Load config, connect, and extract data from mapped tables
    try:
        config = load_config()
        table_mappings = get_table_mappings(config)

        if not table_mappings:
            print("No table mappings found in [migration] section of config.")
        else:
            oracle_conn = connect_oracle(config)
            if oracle_conn:
                print("\n--- Starting Extraction Process ---")
                all_data = {} # Dictionary to hold dataframes or iterators

                for source_table, target_table in table_mappings.items():
                    print(f"\nExtracting data for mapping: {source_table} -> {target_table}")
                    # Example: Extract all data at once (adjust chunk_size if needed)
                    data = extract_data_from_oracle(oracle_conn, source_table, chunk_size=None)

                    if data is not None:
                        if isinstance(data, pd.DataFrame):
                             print(f"Successfully extracted {len(data)} rows from {source_table}.")
                             # In a real scenario, you'd pass this data to the transformer/loader
                             all_data[source_table] = data
                             # print(data.head()) # Optional: Print head for verification
                        else: # It's an iterator
                             print(f"Successfully created iterator for {source_table} (chunked extraction).")
                             # You would iterate over 'data' in the loading step
                             all_data[source_table] = data
                    else:
                        print(f"Extraction failed for table {source_table}.")

                # Close Oracle connection when done
                oracle_conn.close()
                print("\nOracle connection closed.")
            else:
                print("Cannot proceed with extraction, Oracle connection failed.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An error occurred during extraction testing: {e}")
