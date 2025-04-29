import pandas as pd
import logging
from mysql.connector import Error as MySQLError
from .db_connector import connect_mysql # Relative import
# Below imports only used in __main__ example block
from .config_loader import load_config
from .transformer import get_column_mappings, transform_data

# Assuming logging is configured elsewhere (e.g., main_pipeline.py)
log = logging.getLogger(__name__)

def get_target_table(config, source_table_name):
    """Gets the target MySQL table name for a given source Oracle table name."""
    if 'migration' in config:
        # Simple mapping: source_table = target_table
        return config.get('migration', source_table_name, fallback=None)
    return None

def load_data_to_mysql(mysql_conn, df, target_table_name):
    """Loads data from a pandas DataFrame into the specified MySQL table.

    Args:
        mysql_conn (mysql.connector.connection.MySQLConnection): Active MySQL connection.
        df (pd.DataFrame): The DataFrame containing data to load.
        target_table_name (str): The name of the target MySQL table.

    Returns:
        tuple: (affected_rows_count, failure_count) indicating rows affected by
               upsert (inserts + updates) and rows assumed failed in the batch.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        log.warning(f"DataFrame for {target_table_name} is empty or invalid. Skipping load.")
        return 0, 0
    if not list(df.columns):
        log.warning(f"DataFrame for {target_table_name} has no columns. Skipping load.")
        return 0, 0

    cursor = None
    affected_rows_count = 0 # Renamed from success_count
    failure_count = 0
    sql = "" # Initialize sql string

    try:
        cursor = mysql_conn.cursor()

        # --- Construct UPSERT statement ---
        # Assume the first column is the primary key (as per requirement)
        pk_col = df.columns[0]
        quoted_cols = [f"`{col}`" for col in df.columns]
        cols_sql = ', '.join(quoted_cols)
        placeholders = ', '.join(['%s'] * len(df.columns))

        # Build INSERT part
        sql = f"INSERT INTO `{target_table_name}` ({cols_sql}) VALUES ({placeholders})"

        # Build ON DUPLICATE KEY UPDATE part
        update_clauses = []
        # Iterate columns *excluding* the first (assumed PK)
        if len(quoted_cols) > 1:
             for col in quoted_cols[1:]:
                 update_clauses.append(f"{col} = VALUES({col})")

             if update_clauses: # Only add clause if there are non-PK columns
                 update_sql = ', '.join(update_clauses)
                 sql += f" ON DUPLICATE KEY UPDATE {update_sql}"
             else:
                 # This case (only PK column) is unlikely but noted.
                 # The INSERT will behave like INSERT IGNORE if PK exists.
                 log.warning(f"Table {target_table_name} seems to only have a PK column ('{pk_col}'). ON DUPLICATE KEY UPDATE clause skipped.")
        else:
             # Also unlikely, but handle case of only one column total
             log.warning(f"DataFrame for {target_table_name} only has one column ('{pk_col}'). ON DUPLICATE KEY UPDATE clause skipped.")


        log.info(f"Preparing to upsert {len(df)} rows into MySQL table: {target_table_name}")
        log.debug(f"Upsert statement structure: {sql}") # Use debug level for potentially long SQL

        # Convert DataFrame to list of tuples (handle NaN/NaT)
        data_tuples = [
            tuple(None if pd.isna(x) else x for x in row)
            for row in df.itertuples(index=False, name=None)
        ]

        # Execute using executemany
        cursor.executemany(sql, data_tuples)
        mysql_conn.commit() # Commit the transaction

        # --- Handle row count for UPSERT ---
        # cursor.rowcount for INSERT...ON DUPLICATE KEY UPDATE:
        # - Returns 1 for each new row inserted.
        # - Returns 2 for each row updated (if the data actually changed).
        # - Returns 0 if an existing row was not updated (values were the same).
        # We capture the total affected rows (inserts + updates).
        affected_rows_count = cursor.rowcount if cursor.rowcount >= 0 else len(data_tuples) # Fallback if rowcount is -1
        log.info(f"Successfully upserted data into {target_table_name}. Rows affected (inserted/updated): {affected_rows_count}.")
        if cursor.rowcount >= 0:
            log.debug(f"Raw cursor.rowcount from executemany (1=inserted, 2=updated): {cursor.rowcount}")
        else:
            log.warning("cursor.rowcount was negative, using DataFrame length as fallback for affected rows count.")


    except MySQLError as e:
        log.error(f"MySQL Error during upsert into {target_table_name}: {e}", exc_info=True)
        log.error(f"Failed SQL structure: {sql[:1000]}{'...' if len(sql) > 1000 else ''}") # Log truncated SQL
        # Log sample failing data (first few tuples) here for debugging if needed
        # log.error(f"Sample data tuples (first 3): {data_tuples[:3]}")
        failure_count = len(df) # Assume all rows in this batch failed on error
        affected_rows_count = 0
        try:
            log.warning("Attempting to rollback transaction...")
            mysql_conn.rollback() # Rollback on error
            log.info("Transaction rolled back.")
        except MySQLError as rb_err:
            log.error(f"Error during rollback: {rb_err}")
    except Exception as e:
        log.error(f"Unexpected error loading data into {target_table_name}: {e}", exc_info=True)
        failure_count = len(df)
        affected_rows_count = 0
        # Consider rollback here too, though may not be needed if connection died
        if mysql_conn and mysql_conn.is_connected():
             try:
                 log.warning("Attempting to rollback transaction due to unexpected error...")
                 mysql_conn.rollback()
                 log.info("Transaction rolled back.")
             except MySQLError as rb_err:
                 log.error(f"Error during rollback after unexpected error: {rb_err}")
    finally:
        if cursor:
            cursor.close()

    return affected_rows_count, failure_count


if __name__ == '__main__':
    # Example Usage: Load config, connect, create dummy data, transform, load
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__) # Re-get logger after basicConfig

    try:
        config = load_config()
        source_table_example = 'oracle_employees' # Must match a key in [migration]
        target_table_example = get_target_table(config, source_table_example)

        if not target_table_example:
            log.error(f"Error: No target table mapping found for '{source_table_example}' in config.")
        else:
            log.info(f"\n--- Loading Example for {source_table_example} -> {target_table_example} ---")

            # --- Create Dummy Data (same as transformer example) ---
            dummy_data = {
                'EMP_ID': [101, 102, 103], # Assumed PK
                'FIRST_NAME': ['Alice', 'Bob', 'Charlie'],
                'HIRE_DATE': ['2024-01-10', '2024-02-20', None], # Include None/NaN
                'SALARY': [70000, 80000, 75000]
            }
            source_df = pd.DataFrame(dummy_data)
            source_df['HIRE_DATE'] = pd.to_datetime(source_df['HIRE_DATE'], errors='coerce') # Convert to datetime/NaT

            # --- Transform Data ---
            col_mappings = get_column_mappings(config, source_table_example)
            transformed_df = transform_data(source_df.copy(), col_mappings)
            log.info("\nTransformed DataFrame to Load:")
            log.info(transformed_df)

            # --- Connect to MySQL ---
            mysql_conn = connect_mysql(config)

            if mysql_conn:
                log.info(f"\nAttempting to upsert data into MySQL table: {target_table_example}")
                # Assumes target table (e.g., mysql_staff) exists with appropriate columns and PK
                # Example: CREATE TABLE mysql_staff (
                #     staff_id INT PRIMARY KEY,  -- Matches first col 'EMP_ID' -> 'staff_id'
                #     first_name VARCHAR(50),   -- Matches 'FIRST_NAME' -> 'first_name'
                #     start_date DATE,         -- Matches 'HIRE_DATE' -> 'start_date'
                #     salary DECIMAL(10, 2)     -- Matches 'SALARY' -> 'salary'
                # );

                # --- First Load (Insert) ---
                log.info("\n--- First Load Attempt (should insert) ---")
                success1, failures1 = load_data_to_mysql(mysql_conn, transformed_df.copy(), target_table_example)
                log.info(f"Load 1 complete. Affected Rows: {success1}, Failures: {failures1}")

                # --- Modify data for second load (Update) ---
                log.info("\n--- Second Load Attempt (should update/insert) ---")
                # Change Alice's salary, add a new employee
                modified_data = {
                    'EMP_ID': [101, 104], # Update 101, Insert 104
                    'FIRST_NAME': ['Alice', 'David'],
                    'HIRE_DATE': ['2024-01-10', '2024-03-15'],
                    'SALARY': [72000, 90000] # Updated salary for Alice
                }
                modified_df_source = pd.DataFrame(modified_data)
                modified_df_source['HIRE_DATE'] = pd.to_datetime(modified_df_source['HIRE_DATE'], errors='coerce')
                modified_transformed_df = transform_data(modified_df_source.copy(), col_mappings)
                log.info("Modified Transformed DataFrame to Load:")
                log.info(modified_transformed_df)

                success2, failures2 = load_data_to_mysql(mysql_conn, modified_transformed_df, target_table_example)
                # Expected rowcount: 2 (for update of 101) + 1 (for insert of 104) = 3 (if data changed)
                log.info(f"Load 2 complete. Affected Rows: {success2}, Failures: {failures2}")
                log.info("Check the mysql_staff table to verify results.")

                # Close connection
                mysql_conn.close()
                log.info("MySQL connection closed.")
            else:
                log.error("Cannot proceed with loading, MySQL connection failed.")

    except FileNotFoundError as e:
        log.error(f"Configuration file error: {e}")
    except MySQLError as e:
         log.error(f"Database error during loading test: {e}", exc_info=True)
    except Exception as e:
        log.error(f"An error occurred during loading testing: {e}", exc_info=True)

    return success_count, failure_count


if __name__ == '__main__':
    # Example Usage: Load config, connect, create dummy data, transform, load
    try:
        config = load_config()
        source_table_example = 'oracle_employees' # Must match a key in [migration]
        target_table_example = get_target_table(config, source_table_example)

        if not target_table_example:
            print(f"Error: No target table mapping found for '{source_table_example}' in config.")
        else:
            print(f"\n--- Loading Example for {source_table_example} -> {target_table_example} ---")

            # --- Create Dummy Data (same as transformer example) ---
            dummy_data = {
                'EMP_ID': [101, 102, 103],
                'FIRST_NAME': ['Alice', 'Bob', 'Charlie'],
                'HIRE_DATE': ['2024-01-10', '2024-02-20', None], # Include None/NaN
                'SALARY': [70000, 80000, 75000]
            }
            source_df = pd.DataFrame(dummy_data)
            source_df['HIRE_DATE'] = pd.to_datetime(source_df['HIRE_DATE'], errors='coerce') # Convert to datetime/NaT

            # --- Transform Data ---
            col_mappings = get_column_mappings(config, source_table_example)
            transformed_df = transform_data(source_df.copy(), col_mappings)
            print("\nTransformed DataFrame to Load:")
            print(transformed_df)

            # --- Connect to MySQL ---
            mysql_conn = connect_mysql(config)

            if mysql_conn:
                print(f"\nAttempting to load data into MySQL table: {target_table_example}")
                # This requires the target table (e.g., mysql_staff) to exist in MySQL
                # with appropriate columns (e.g., staff_id, FIRST_NAME, start_date, SALARY)
                # You might need to manually create the table first for testing.

                # Example: CREATE TABLE mysql_staff (
                #     staff_id INT PRIMARY KEY,
                #     FIRST_NAME VARCHAR(50),
                #     start_date DATE,
                #     SALARY DECIMAL(10, 2)
                # );

                success, failures = load_data_to_mysql(mysql_conn, transformed_df, target_table_example)
                print(f"\nLoad complete. Success: {success}, Failures: {failures}")

                # Close connection
                mysql_conn.close()
                print("MySQL connection closed.")
            else:
                print("Cannot proceed with loading, MySQL connection failed.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except MySQLError as e:
         print(f"Database error during loading test: {e}")
    except Exception as e:
        print(f"An error occurred during loading testing: {e}")
