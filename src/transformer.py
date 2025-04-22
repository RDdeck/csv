import pandas as pd
from .config_loader import load_config # Relative import

def get_column_mappings(config, source_table_name):
    """Extracts column mappings for a specific table from the configuration.

    Args:
        config (configparser.ConfigParser): The loaded configuration object.
        source_table_name (str): The name of the source (Oracle) table.

    Returns:
        dict: A dictionary mapping source column names to target column names
              for the given table. Returns an empty dict if no specific mappings
              are found for this table in the '[migration]' section.
    """
    mappings = {}
    if 'migration' in config:
        prefix = f"{source_table_name}."
        for key, value in config.items('migration'):
            if key.startswith(prefix):
                source_col = key[len(prefix):] # Get source column name
                # Value might be target_table.target_col, just extract target_col
                target_col = value.split('.')[-1]
                mappings[source_col] = target_col
    return mappings

def transform_data(df, column_mappings):
    """Applies transformations to the DataFrame.

    Currently implements column renaming based on mappings.
    Can be extended for data type conversion, cleaning, etc.

    Args:
        df (pd.DataFrame): The DataFrame extracted from the source database.
        column_mappings (dict): Dictionary mapping source columns to target columns.

    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    if not isinstance(df, pd.DataFrame):
        print("Warning: transform_data received non-DataFrame input. Skipping transformation.")
        return df # Or raise error?

    # 1. Rename columns based on mappings
    if column_mappings:
        # Select only the columns that are present in the DataFrame AND the mapping keys
        rename_map = {src: tgt for src, tgt in column_mappings.items() if src in df.columns}
        if rename_map:
             print(f"Renaming columns: {rename_map}")
             df = df.rename(columns=rename_map)
        # Optional: Select ONLY the columns that are in the target mapping
        # target_columns = list(rename_map.values())
        # df = df[target_columns]

    # 2. TODO: Implement Data Type Conversions (Example)
    #    This is highly specific to the source/target schemas.
    #    Example: Convert a column 'DATE_COLUMN' to datetime objects
    # if 'TARGET_DATE_COL' in df.columns:
    #     try:
    #         df['TARGET_DATE_COL'] = pd.to_datetime(df['TARGET_DATE_COL'], errors='coerce') # 'coerce' turns errors into NaT
    #     except Exception as e:
    #         print(f"Warning: Could not convert TARGET_DATE_COL to datetime: {e}")

    # 3. TODO: Implement Data Cleaning (Example)
    #    Example: Fill missing values in a specific column with a default
    # if 'TARGET_NUMERIC_COL' in df.columns:
    #     df['TARGET_NUMERIC_COL'] = df['TARGET_NUMERIC_COL'].fillna(0)

    # 4. TODO: Add more transformation rules as needed

    print(f"Transformation complete. DataFrame shape: {df.shape}")
    return df


if __name__ == '__main__':
    # Example Usage: Load config, create dummy data, apply transformations
    try:
        config = load_config()
        source_table = 'oracle_employees' # Example table name from config

        # --- Get Mappings ---
        col_mappings = get_column_mappings(config, source_table)
        print(f"\n--- Column Mappings for {source_table} ---")
        if col_mappings:
            print(col_mappings)
        else:
            print("No specific column mappings found.")


        # --- Create Dummy DataFrame ---
        print(f"\n--- Transforming Dummy Data for {source_table} ---")
        # Assume these are columns extracted from Oracle
        dummy_data = {
            'EMP_ID': [1, 2, 3],
            'FIRST_NAME': ['John', 'Jane', 'Peter'],
            'HIRE_DATE': ['2023-01-15', '2023-03-10', '2022-11-01'],
            'SALARY': [50000, 60000, 55000]
        }
        # Add extra column not in mappings to test robustness
        dummy_data['ORACLE_SPECIFIC_COL'] = ['A', 'B', 'C']

        source_df = pd.DataFrame(dummy_data)
        print("Original DataFrame:")
        print(source_df)
        print("\nApplying transformations...")


        # --- Apply Transformation ---
        transformed_df = transform_data(source_df.copy(), col_mappings) # Use copy to keep original
        print("\nTransformed DataFrame:")
        print(transformed_df)

        # Example: What if config had these mappings for oracle_employees?
        # [migration]
        # oracle_employees = mysql_staff
        # oracle_employees.emp_id = mysql_staff.staff_id
        # oracle_employees.hire_date = mysql_staff.start_date
        # (FIRST_NAME and SALARY would map implicitly if target table has same names)
        # Expected output columns (if using selection): staff_id, start_date
        # Expected output columns (if just renaming): staff_id, FIRST_NAME, start_date, SALARY, ORACLE_SPECIFIC_COL


    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An error occurred during transformation testing: {e}")
