import pandas as pd
from mysql.connector import Error as MySQLError
from .db_connector import connect_mysql # Relative import
from .config_loader import load_config # Relative import
from .transformer import get_column_mappings, transform_data # Relative imports

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
        tuple: (success_count, failure_count) indicating how many rows
               were successfully inserted and how many failed.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        print(f"DataFrame for {target_table_name} is empty or invalid. Skipping load.")
        return 0, 0

    cursor = None
    success_count = 0
    failure_count = 0

    try:
        cursor = mysql_conn.cursor()

        # Prepare the INSERT statement
        cols = ', '.join([f"`{col}`" for col in df.columns]) # Use backticks for safety
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO `{target_table_name}` ({cols}) VALUES ({placeholders})"
        print(f"Preparing to load {len(df)} rows into MySQL table: {target_table_name}")
        # print(f"Sample INSERT statement structure: {sql}") # Uncomment for debugging

        # Convert DataFrame to list of tuples for executemany
        # Handle potential NaN/NaT values which MySQL might not like directly
        data_tuples = [
            tuple(None if pd.isna(x) else x for x in row)
            for row in df.itertuples(index=False, name=None)
        ]

        # Use executemany for bulk insertion
        # Note: executemany might not provide row-level error feedback easily across all connectors/versions.
        # For granular error handling, row-by-row insert in a loop with try-except is needed, but much slower.
        cursor.executemany(sql, data_tuples)
        mysql_conn.commit() # Commit the transaction

        success_count = cursor.rowcount if cursor.rowcount >= 0 else len(data_tuples) # rowcount can be -1 sometimes
        print(f"Successfully loaded {success_count} rows into {target_table_name}.")

    except MySQLError as e:
        print(f"MySQL Error loading data into {target_table_name}: {e}")
        print("Attempted SQL:", sql[:500] + "..." if len(sql) > 500 else sql) # Print truncated SQL
        # Consider logging sample failing data (first few tuples) here for debugging
        # print("Sample data tuples (first 5):", data_tuples[:5])
        failure_count = len(df) # Assume all rows in this batch failed on error
        success_count = 0
        try:
            mysql_conn.rollback() # Rollback on error
            print("Transaction rolled back.")
        except MySQLError as rb_err:
            print(f"Error during rollback: {rb_err}")
    except Exception as e:
        print(f"Unexpected error loading data into {target_table_name}: {e}")
        failure_count = len(df)
        success_count = 0
        # Attempt rollback here too? Depends on error type.
    finally:
        if cursor:
            cursor.close()

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
