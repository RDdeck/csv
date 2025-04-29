import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from configparser import ConfigParser
from src.extractor import extract_data_from_oracle

# Mock the logger used in extractor to avoid configuring it
@patch('src.extractor.log', MagicMock())
class TestExtractor(unittest.TestCase):

    def setUp(self):
        """Set up mock Oracle connection and basic config."""
        self.mock_oracle_conn = MagicMock()
        self.config = ConfigParser()
        self.config.add_section('migration')
        self.table_name = 'test_table'

    @patch('pandas.read_sql')
    def test_extract_full_load_no_incremental_col(self, mock_read_sql):
        """Test full load when no incremental column is configured."""
        extract_data_from_oracle(self.mock_oracle_conn, self.table_name, self.config, chunk_size=None)

        # Assert read_sql was called
        mock_read_sql.assert_called_once()
        # Get the arguments passed to read_sql
        args, kwargs = mock_read_sql.call_args
        sql_query = args[0]
        params = kwargs.get('params')

        # Assert the query is a simple SELECT * without WHERE or ORDER BY
        self.assertEqual(f"SELECT * FROM {self.table_name}", sql_query.strip())
        self.assertEqual({}, params) # No parameters expected

    @patch('pandas.read_sql')
    def test_extract_full_load_with_hwm_but_no_config(self, mock_read_sql):
        """Test full load when HWM is provided but no incremental column in config."""
        extract_data_from_oracle(self.mock_oracle_conn, self.table_name, self.config, high_water_mark='2023-01-01', chunk_size=None)

        mock_read_sql.assert_called_once()
        args, kwargs = mock_read_sql.call_args
        sql_query = args[0]
        params = kwargs.get('params')

        # Still expect a full load query as incremental column is missing
        self.assertEqual(f"SELECT * FROM {self.table_name}", sql_query.strip())
        self.assertEqual({}, params)

    @patch('pandas.read_sql')
    def test_extract_initial_load_with_incremental_col(self, mock_read_sql):
        """Test initial load when incremental column is configured but no HWM is provided."""
        inc_col = 'LAST_UPDATED'
        self.config.set('migration', f'{self.table_name}.incremental_column', inc_col)

        extract_data_from_oracle(self.mock_oracle_conn, self.table_name, self.config, high_water_mark=None, chunk_size=None)

        mock_read_sql.assert_called_once()
        args, kwargs = mock_read_sql.call_args
        sql_query = args[0]
        params = kwargs.get('params')

        # Expect SELECT * with ORDER BY but no WHERE clause
        expected_query = f"SELECT * FROM {self.table_name} ORDER BY {inc_col} ASC"
        self.assertEqual(expected_query, sql_query.strip())
        self.assertEqual({}, params) # No HWM parameter needed

    @patch('pandas.read_sql')
    def test_extract_incremental_load_with_config_and_hwm(self, mock_read_sql):
        """Test incremental load with incremental column configured and HWM provided."""
        inc_col = 'MODIFICATION_TS'
        hwm_value = '2024-03-10T12:00:00Z'
        self.config.set('migration', f'{self.table_name}.incremental_column', inc_col)

        extract_data_from_oracle(self.mock_oracle_conn, self.table_name, self.config, high_water_mark=hwm_value, chunk_size=None)

        mock_read_sql.assert_called_once()
        args, kwargs = mock_read_sql.call_args
        sql_query = args[0]
        params = kwargs.get('params')

        # Expect SELECT * with WHERE and ORDER BY clauses
        expected_query = f"SELECT * FROM {self.table_name} WHERE {inc_col} > :hwm ORDER BY {inc_col} ASC"
        self.assertEqual(expected_query, sql_query.strip())
        self.assertEqual({'hwm': hwm_value}, params) # Expect HWM parameter

    @patch('pandas.read_sql')
    def test_extract_with_chunking(self, mock_read_sql):
        """Test that chunksize parameter is passed to read_sql."""
        chunk_size = 5000
        extract_data_from_oracle(self.mock_oracle_conn, self.table_name, self.config, chunk_size=chunk_size)

        mock_read_sql.assert_called_once()
        args, kwargs = mock_read_sql.call_args
        # Assert chunksize was passed correctly
        self.assertEqual(chunk_size, kwargs.get('chunksize'))


if __name__ == '__main__':
    unittest.main()
