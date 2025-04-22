import cx_Oracle
import mysql.connector
from mysql.connector import Error as MySQLError
from .config_loader import load_config # Use relative import

def connect_oracle(config):
    """Establishes a connection to the Oracle database.

    Args:
        config (configparser.ConfigParser): The loaded configuration object.

    Returns:
        cx_Oracle.Connection: The Oracle database connection object, or None if connection fails.
    """
    try:
        dsn = config.get('oracle', 'dsn')
        user = config.get('oracle', 'user')
        password = config.get('oracle', 'password')

        # Ensure Thick mode is enabled if required (usually needed unless using specific Oracle Instant Client setups)
        # Comment out or remove if you are sure you need Thin mode
        # cx_Oracle.init_oracle_client(lib_dir=r"/path/to/your/instantclient_XX_Y") # Example path

        print(f"Attempting to connect to Oracle: DSN='{dsn}', User='{user}'") # Added logging
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        print("Oracle connection successful.")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Error connecting to Oracle: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during Oracle connection: {e}")
        return None


def connect_mysql(config):
    """Establishes a connection to the MySQL database.

    Args:
        config (configparser.ConfigParser): The loaded configuration object.

    Returns:
        mysql.connector.connection.MySQLConnection: The MySQL database connection object, or None if connection fails.
    """
    try:
        host = config.get('mysql', 'host')
        port = config.getint('mysql', 'port') # Ensure port is integer
        user = config.get('mysql', 'user')
        password = config.get('mysql', 'password')
        database = config.get('mysql', 'database')

        print(f"Attempting to connect to MySQL: Host='{host}', Port='{port}', User='{user}', Database='{database}'") # Added logging
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("MySQL connection successful.")
            return connection
        else:
            print("MySQL connection failed (is_connected() returned False).") # Added detail
            return None
    except MySQLError as e:
        print(f"Error connecting to MySQL: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during MySQL connection: {e}")
        return None


if __name__ == '__main__':
    # Example usage: Load config and attempt connections
    try:
        # We need to load the config differently when running directly
        # Assume config_loader.py is in the same directory for this example
        # Adjust the import based on actual project structure if needed
        from config_loader import load_config # Direct import for __main__ execution
        config = load_config() # Use the correct loader

        # --- Test Oracle Connection ---
        print("\n--- Testing Oracle Connection ---")
        oracle_conn = connect_oracle(config)
        if oracle_conn:
            print(f"Oracle DB Version: {oracle_conn.version}")
            # Remember to close the connection
            oracle_conn.close()
            print("Oracle connection closed.")
        else:
            print("Oracle connection could not be established.")
            print("Please check your Oracle configuration in config/db_config.ini")
            print("Ensure Oracle client libraries are correctly installed and configured (if using Thick mode).")


        # --- Test MySQL Connection ---
        print("\n--- Testing MySQL Connection ---")
        mysql_conn = connect_mysql(config)
        if mysql_conn:
            print("MySQL connection appears successful (check server logs if needed).")
            # Remember to close the connection
            mysql_conn.close()
            print("MySQL connection closed.")
        else:
            print("MySQL connection could not be established.")
            print("Please check your MySQL configuration in config/db_config.ini")


    except FileNotFoundError as e:
        print(f"Error: {e}")
    except ImportError:
        # This happens if config_loader cannot be imported directly
        print("Error: Could not import config_loader. Ensure it's in the same directory or adjust PYTHONPATH if running db_connector.py directly.")
    except Exception as e:
        print(f"An error occurred during testing: {e}")
