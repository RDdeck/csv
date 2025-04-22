import unittest
import os
import configparser
from src.config_loader import load_config # Assumes tests run from root dir

# Helper to create a dummy config file for testing
def create_dummy_config(path='config/test_db_config.ini'):
    content = """
[oracle]
user = test_ora_user
password = test_ora_pass
dsn = test_dsn

[mysql]
user = test_mysql_user
password = test_mysql_pass
host = localhost
port = 3306
database = test_db

[migration]
chunk_size = 5000
table1 = target_table1
table2 = target_table2
table1.colA = target_table1.colX
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    return path

class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        # Create dummy config file before each test
        self.dummy_config_path = create_dummy_config('../config/test_db_config.ini') # Relative to src/config_loader.py

    def tearDown(self):
        # Clean up dummy config file after each test
        abs_path = os.path.join(os.path.dirname(__file__), '../config/test_db_config.ini')
        if os.path.exists(abs_path):
            os.remove(abs_path)
        # Clean up directory if empty? More complex, skip for now.

    def test_load_config_success(self):
        """Test loading a valid configuration file."""
        config = load_config(config_path='../config/test_db_config.ini') # Path relative to src/config_loader.py
        self.assertIsInstance(config, configparser.ConfigParser)
        self.assertEqual(config.get('oracle', 'user'), 'test_ora_user')
        self.assertEqual(config.getint('mysql', 'port'), 3306)
        self.assertEqual(config.getint('migration', 'chunk_size'), 5000)

    def test_load_config_file_not_found(self):
        """Test loading a non-existent configuration file."""
        with self.assertRaises(FileNotFoundError):
            load_config(config_path='../config/non_existent_config.ini')

if __name__ == '__main__':
    unittest.main()
