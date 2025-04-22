import configparser
import os

def load_config(config_path='../config/db_config.ini'):
    """Loads database configuration from the specified INI file.

    Args:
        config_path (str): The path to the configuration file.
                           Defaults to '../config/db_config.ini'.

    Returns:
        configparser.ConfigParser: A ConfigParser object with the loaded
                                   configuration.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
    """
    # Construct absolute path relative to this script file
    abs_config_path = os.path.join(os.path.dirname(__file__), config_path)

    if not os.path.exists(abs_config_path):
        raise FileNotFoundError(f"Configuration file not found at: {abs_config_path}")

    config = configparser.ConfigParser()
    config.read(abs_config_path)
    return config

if __name__ == '__main__':
    # Example usage: Load and print config sections
    try:
        config = load_config()
        print("Configuration loaded successfully.")
        for section in config.sections():
            print(f"[{section}]")
            for key, value in config.items(section):
                # Be careful printing sensitive info like passwords in production
                print(f"{key} = {value}")
            print()
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")
