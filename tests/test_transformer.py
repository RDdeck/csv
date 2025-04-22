import unittest
import pandas as pd
from src.transformer import transform_data # Assumes tests run from root dir

class TestTransformer(unittest.TestCase):

    def test_column_renaming(self):
        """Test basic column renaming based on mappings."""
        data = {'col_a': [1, 2], 'col_b': ['x', 'y'], 'extra_col': [True, False]}
        df = pd.DataFrame(data)
        mappings = {'col_a': 'new_a', 'col_b': 'new_b'} # 'extra_col' not in mapping
        transformed_df = transform_data(df.copy(), mappings)

        self.assertListEqual(list(transformed_df.columns), ['new_a', 'new_b', 'extra_col'])
        self.assertEqual(transformed_df.shape[0], 2)

    def test_no_renaming_if_no_mappings(self):
        """Test that DataFrame remains unchanged if no mappings are provided."""
        data = {'col_a': [1, 2], 'col_b': ['x', 'y']}
        df = pd.DataFrame(data)
        mappings = {}
        transformed_df = transform_data(df.copy(), mappings)

        self.assertListEqual(list(transformed_df.columns), ['col_a', 'col_b'])

    def test_renaming_with_missing_source_cols(self):
        """Test renaming when some mapped source columns aren't in the DataFrame."""
        data = {'col_a': [1, 2]} # col_b is missing
        df = pd.DataFrame(data)
        mappings = {'col_a': 'new_a', 'col_b': 'new_b'}
        transformed_df = transform_data(df.copy(), mappings)

        self.assertListEqual(list(transformed_df.columns), ['new_a']) # Only col_a gets renamed

    # Add placeholder tests for future functionality
    @unittest.skip("Data type conversion not implemented yet")
    def test_data_type_conversion(self):
        # TODO: Implement test when functionality exists
        pass

    @unittest.skip("Data cleaning not implemented yet")
    def test_data_cleaning(self):
        # TODO: Implement test when functionality exists
        pass

if __name__ == '__main__':
    unittest.main()
