import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from src.loader import load_data_to_mysql

# Mock the logger used in loader to avoid configuring it
@patch('src.loader.log', MagicMock())
class TestLoader(unittest.TestCase):

    def setUp(self):
        """Set up mock MySQL connection and cursor."""
        self.mock_mysql_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_mysql_conn.cursor.return_value = self.mock_cursor
        # Simulate rowcount behaviour for upsert (1 for insert, 2 for update)
        self.mock_cursor.rowcount = 1 # Default assumption
        self.target_table = 'target_test_table'

    def test_load_empty_dataframe(self):
        """Test that loading an empty DataFrame does nothing and returns (0, 0)."""
        empty_df = pd.DataFrame()

        affected_rows, failures = load_data_to_mysql(self.mock_mysql_conn, empty_df, self.target_table)

        # Assert cursor and commit were NOT called
        self.mock_mysql_conn.cursor.assert_not_called()
        self.mock_mysql_conn.commit.assert_not_called()
        self.assertEqual(affected_rows, 0)
        self.assertEqual(failures, 0)

    def test_load_dataframe_with_no_columns(self):
        """Test that loading a DataFrame with no columns does nothing."""
        no_cols_df = pd.DataFrame([]) # Creates DF with 0 columns

        affected_rows, failures = load_data_to_mysql(self.mock_mysql_conn, no_cols_df, self.target_table)

        self.mock_mysql_conn.cursor.assert_not_called()
        self.mock_mysql_conn.commit.assert_not_called()
        self.assertEqual(affected_rows, 0)
        self.assertEqual(failures, 0)


    def test_load_upsert_sql_generation_multi_column(self):
        """Verify the generated SQL for INSERT...ON DUPLICATE KEY UPDATE."""
        data = {'id': [1, 2], 'name': ['Alice', 'Bob'], 'value': [10, 20]}
        df = pd.DataFrame(data)
        # Set rowcount to simulate 1 insert + 1 update = 3? No, it's per execution.
        # executemany returns total affected rows (1+2=3 if one inserted, one updated)
        self.mock_cursor.rowcount = 3 # Simulate 1 insert, 1 update

        affected_rows, failures = load_data_to_mysql(self.mock_mysql_conn, df, self.target_table)

        # Assert that cursor.executemany was called
        self.mock_cursor.executemany.assert_called_once()

        # Get the arguments passed to executemany
        args, _ = self.mock_cursor.executemany.call_args
        sql_query = args[0]
        data_tuples = args[1]

        # Construct the expected SQL
        expected_sql = (
            f"INSERT INTO `{self.target_table}` (`id`, `name`, `value`) VALUES (%s, %s, %s) "
            f"ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `value` = VALUES(`value`)"
        )

        # Normalize whitespace just in case
        self.assertEqual(' '.join(expected_sql.split()), ' '.join(sql_query.split()))

        # Verify data tuples are correct
        expected_tuples = [(1, 'Alice', 10), (2, 'Bob', 20)]
        self.assertEqual(expected_tuples, data_tuples)

        # Verify commit was called
        self.mock_mysql_conn.commit.assert_called_once()

        # Verify returned counts
        self.assertEqual(affected_rows, self.mock_cursor.rowcount)
        self.assertEqual(failures, 0)

    def test_load_upsert_sql_generation_single_column_pk_only(self):
        """Verify SQL when only a primary key column exists (no UPDATE clause)."""
        data = {'pk_col': [101, 102]}
        df = pd.DataFrame(data)
        self.mock_cursor.rowcount = 2 # Simulate 2 inserts

        affected_rows, failures = load_data_to_mysql(self.mock_mysql_conn, df, self.target_table)

        self.mock_cursor.executemany.assert_called_once()
        args, _ = self.mock_cursor.executemany.call_args
        sql_query = args[0]
        data_tuples = args[1]

        # Construct the expected SQL (no ON DUPLICATE KEY UPDATE)
        expected_sql = f"INSERT INTO `{self.target_table}` (`pk_col`) VALUES (%s)"

        self.assertEqual(' '.join(expected_sql.split()), ' '.join(sql_query.split()))
        expected_tuples = [(101,), (102,)]
        self.assertEqual(expected_tuples, data_tuples)
        self.mock_mysql_conn.commit.assert_called_once()
        self.assertEqual(affected_rows, self.mock_cursor.rowcount)
        self.assertEqual(failures, 0)

    def test_load_data_handles_nan_values(self):
        """Verify NaN values are converted to None in data tuples."""
        data = {'id': [1, 2], 'value': [10.5, pd.NA]} # Use pd.NA or np.nan
        df = pd.DataFrame(data)
        self.mock_cursor.rowcount = 2

        load_data_to_mysql(self.mock_mysql_conn, df, self.target_table)

        self.mock_cursor.executemany.assert_called_once()
        args, _ = self.mock_cursor.executemany.call_args
        data_tuples = args[1]

        # Verify data tuples have None instead of NaN/NA
        expected_tuples = [(1, 10.5), (2, None)]
        self.assertEqual(expected_tuples, data_tuples)


if __name__ == '__main__':
    unittest.main()
