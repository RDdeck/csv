# Python Oracle to MySQL ETL Pipeline

This project provides a Python-based ETL (Extract, Transform, Load) pipeline for migrating data from an Oracle database to a MySQL database.

## Features

*   Connects to Oracle and MySQL databases.
*   Extracts data from specified Oracle tables.
*   Supports basic data transformations (column renaming via config).
*   Loads data into corresponding MySQL tables.
*   Configurable database credentials, table mappings, and column mappings via an INI file.
*   Supports chunked data extraction for large tables.
*   Basic logging for monitoring the process.
*   Includes unit tests for core components.

## Project Structure

```
.
├── config/
│   ├── db_config.ini       # Configuration file for DB credentials and mappings (NEEDS EDITING)
│   └── (other_config.ini)
├── src/
│   ├── __init__.py
│   ├── config_loader.py    # Loads configuration from db_config.ini
│   ├── db_connector.py     # Handles Oracle and MySQL connections
│   ├── extractor.py        # Extracts data from Oracle
│   ├── transformer.py      # Performs data transformations
│   ├── loader.py           # Loads data into MySQL
│   └── main_pipeline.py    # Main script to run the ETL process
├── tests/
│   ├── __init__.py
│   ├── test_config_loader.py # Tests for config loading
│   ├── test_transformer.py   # Tests for data transformation
│   └── test_mappings.py      # Tests for mapping functions
├── requirements.txt        # Python dependencies
├── README.md               # This file
└── PYTHON CSV FILES READ.ipynb # Original notebook (for reference)
```

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create a virtual environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Oracle Client Libraries:**
    *   The `cx_Oracle` library might require Oracle Instant Client libraries to be installed and accessible on your system.
    *   Download the appropriate Instant Client package (e.g., Basic or Basic Light) for your operating system from the Oracle website.
    *   Unzip the package to a known location (e.g., `/opt/oracle/instantclient_21_3` or `C:\oracle\instantclient_21_3`).
    *   You might need to:
        *   Add the Instant Client directory to your system's `PATH` (Windows) or `LD_LIBRARY_PATH` (Linux/macOS).
        *   **OR** Uncomment and set the correct path in `src/db_connector.py` if using Thick mode explicitly:
            ```python
            # cx_Oracle.init_oracle_client(lib_dir=r"/path/to/your/instantclient_XX_Y")
            ```
    *   Refer to the `cx_Oracle` installation documentation for detailed instructions specific to your OS.

## Configuration (`config/db_config.ini`)

Before running the pipeline, you **must** edit the `config/db_config.ini` file with your actual database credentials and migration details. **Do not commit sensitive credentials to version control.**

```ini
[oracle]
user = YOUR_ORACLE_USER          # Replace with your Oracle username
password = YOUR_ORACLE_PASSWORD  # Replace with your Oracle password
# Example DSN format: host:port/service_name OR use TNS alias if configured
dsn = YOUR_ORACLE_HOST:PORT/SERVICE_NAME

[mysql]
user = YOUR_MYSQL_USER          # Replace with your MySQL username
password = YOUR_MYSQL_PASSWORD  # Replace with your MySQL password
host = YOUR_MYSQL_HOST          # Replace with your MySQL server hostname or IP
port = 3306                     # Replace with your MySQL port (default is 3306)
database = YOUR_MYSQL_DATABASE  # Replace with your target MySQL database name

[migration]
# --- Required: Table Mappings ---
# Define which Oracle tables map to which MySQL tables.
# Format: oracle_table_name = mysql_table_name
# Example:
# employees = staff
# departments = business_units
source_table1 = target_table1
source_table2 = target_table2

# --- Optional: Column Mappings ---
# Define if column names differ between source and target tables.
# Format: oracle_table_name.oracle_col_name = mysql_table_name.mysql_col_name
# If omitted for a column, it's assumed the name is the same.
# Example:
# employees.emp_id = staff.staff_id
# employees.hire_date = staff.start_date
# departments.dept_id = business_units.unit_code
source_table1.oracle_col_a = target_table1.mysql_col_x

# --- Optional: Extraction Chunk Size ---
# Number of rows to fetch from Oracle in each batch. Useful for large tables.
# Defaults to 10000 if not specified. Set to 0 or remove for no chunking.
chunk_size = 10000
```

**Important:** Ensure the target tables exist in the MySQL database *before* running the migration. This script does not create tables.

## Running the Pipeline

1.  Ensure your configuration in `config/db_config.ini` is correct.
2.  Make sure the target tables exist in your MySQL database.
3.  Run the main script from the root directory of the project:

    ```bash
    python src/main_pipeline.py
    ```

    The script will log its progress to the console, including connection attempts, table processing, row counts (success/failure), and timings.

## Running Tests

To run the unit tests, navigate to the root directory and use the `unittest` discovery feature:

```bash
python -m unittest discover tests/
```

This will find and execute all tests within the `tests` directory.

## TODO / Potential Improvements

*   Implement schema creation/validation in MySQL.
*   Add more sophisticated data type conversion logic in `transformer.py`.
*   Implement more robust error handling and retry mechanisms.
*   Support parallel processing for multiple tables.
*   Improve logging (e.g., log to files, configurable log levels).
*   Use environment variables or a secrets management system for credentials instead of the INI file for better security.
*   Add more comprehensive unit and integration tests (including database interactions using mocking or test databases).
```
