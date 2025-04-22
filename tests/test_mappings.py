import unittest
import configparser
from src.extractor import get_table_mappings
from src.transformer import get_column_mappings
from src.loader import get_target_table

# Helper to create a ConfigParser object for testing
def create_test_config():
    config = configparser.ConfigParser()
    config.add_section('oracle')
    config.set('oracle', 'user', 'test')
    config.add_section('mysql')
    config.set('mysql', 'database', 'test')
    config.add_section('migration')
    config.set('migration', 'ora_emp', 'mysql_staff')
    config.set('migration', 'ora_dept', 'mysql_departments')
    config.set('migration', 'ora_emp.emp_id', 'mysql_staff.staff_id')
    config.set('migration', 'ora_emp.hire_date', 'mysql_staff.start_date')
    config.set('migration', 'ora_dept.dept_no', 'mysql_departments.id')
    config.set('migration', 'chunk_size', '1000') # Non-mapping item
    return config

class TestMappingFunctions(unittest.TestCase):

    def setUp(self):
        self.config = create_test_config()

    def test_get_table_mappings(self):
        """Test extracting table-to-table mappings."""
        expected_tables = {
            'ora_emp': 'mysql_staff',
            'ora_dept': 'mysql_departments'
            # chunk_size and column mappings should be excluded
        }
        actual_tables = get_table_mappings(self.config)
        self.assertDictEqual(actual_tables, expected_tables)

    def test_get_column_mappings_found(self):
        """Test extracting column mappings for a specific table."""
        expected_cols = {
            'emp_id': 'staff_id',
            'hire_date': 'start_date'
        }
        actual_cols = get_column_mappings(self.config, 'ora_emp')
        self.assertDictEqual(actual_cols, expected_cols)

    def test_get_column_mappings_not_found(self):
        """Test getting column mappings for a table with no specific rules."""
        # Table 'ora_locations' has no specific column rules in the dummy config
        expected_cols = {}
        actual_cols = get_column_mappings(self.config, 'ora_locations')
        self.assertDictEqual(actual_cols, expected_cols)

    def test_get_target_table_found(self):
        """Test getting the target table name for a mapped source table."""
        self.assertEqual(get_target_table(self.config, 'ora_emp'), 'mysql_staff')
        self.assertEqual(get_target_table(self.config, 'ora_dept'), 'mysql_departments')

    def test_get_target_table_not_found(self):
        """Test getting the target table name for an unmapped source table."""
        self.assertIsNone(get_target_table(self.config, 'unmapped_table'))

if __name__ == '__main__':
    unittest.main()
