import unittest
import os
import json
import logging
from src.state_manager import load_state, save_state

# Disable logging for tests to keep output clean
logging.disable(logging.CRITICAL)

class TestStateManager(unittest.TestCase):

    def setUp(self):
        """Set up test environment; create a dummy state file path."""
        self.test_state_file = 'test_state_manager_state.json'
        # Ensure no leftover file from previous failed test run
        if os.path.exists(self.test_state_file):
            os.remove(self.test_state_file)

    def tearDown(self):
        """Clean up test environment; remove the dummy state file."""
        if os.path.exists(self.test_state_file):
            os.remove(self.test_state_file)

    def test_load_state_file_not_found(self):
        """Test loading state when the file does not exist."""
        state = load_state('non_existent_file.json')
        self.assertEqual(state, {})

    def test_load_state_empty_file(self):
        """Test loading state from an empty file."""
        with open(self.test_state_file, 'w') as f:
            pass # Create empty file
        state = load_state(self.test_state_file)
        self.assertEqual(state, {})

    def test_load_state_invalid_json(self):
        """Test loading state from a file with invalid JSON."""
        with open(self.test_state_file, 'w') as f:
            f.write("{invalid json format")
        state = load_state(self.test_state_file)
        self.assertEqual(state, {})

    def test_load_state_valid_json(self):
        """Test loading state from a file with valid JSON."""
        expected_state = {'table1': '2024-01-01T10:00:00', 'table2': 12345}
        with open(self.test_state_file, 'w') as f:
            json.dump(expected_state, f)
        state = load_state(self.test_state_file)
        self.assertEqual(state, expected_state)

    def test_save_state(self):
        """Test saving a state dictionary to a file."""
        state_to_save = {'tableA': 'valueA', 'tableB': 987}
        save_state(self.test_state_file, state_to_save)

        # Verify by reading the file content directly
        self.assertTrue(os.path.exists(self.test_state_file))
        with open(self.test_state_file, 'r') as f:
            loaded_state = json.load(f)
        self.assertEqual(loaded_state, state_to_save)

    def test_save_and_load_cycle(self):
        """Test a full save -> load -> modify -> save -> load cycle."""
        # Initial save
        initial_state = {'tableX': 'initial_value', 'tableY': 100}
        save_state(self.test_state_file, initial_state)

        # Load 1
        loaded_state_1 = load_state(self.test_state_file)
        self.assertEqual(loaded_state_1, initial_state)

        # Modify and Save 2
        modified_state = loaded_state_1
        modified_state['tableX'] = 'updated_value'
        modified_state['tableZ'] = 200
        save_state(self.test_state_file, modified_state)

        # Load 2
        loaded_state_2 = load_state(self.test_state_file)
        self.assertEqual(loaded_state_2, modified_state)
        self.assertEqual(loaded_state_2['tableX'], 'updated_value')
        self.assertEqual(loaded_state_2['tableZ'], 200)

if __name__ == '__main__':
    unittest.main()
