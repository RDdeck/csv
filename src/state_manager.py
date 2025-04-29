import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_state(state_file_path: str) -> dict:
    """Loads the pipeline state from a JSON file.

    Args:
        state_file_path (str): The path to the state file.

    Returns:
        dict: The loaded state dictionary, or an empty dictionary if the file
              doesn't exist, is empty, or contains invalid JSON.
    """
    state = {}
    if not os.path.exists(state_file_path):
        logging.info(f"State file '{state_file_path}' not found. Starting with empty state.")
        return state
    if os.path.getsize(state_file_path) == 0:
        logging.info(f"State file '{state_file_path}' is empty. Starting with empty state.")
        return state

    try:
        with open(state_file_path, 'r') as f:
            state = json.load(f)
        logging.info(f"Successfully loaded state from '{state_file_path}'.")
    except json.JSONDecodeError:
        logging.warning(f"Error decoding JSON from '{state_file_path}'. Returning empty state.", exc_info=True)
        # Return empty dict if JSON is invalid
        return {}
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading state from '{state_file_path}'. Returning empty state.", exc_info=True)
        # Return empty dict for any other file reading errors
        return {}
    return state

def save_state(state_file_path: str, state_data: dict):
    """Saves the pipeline state to a JSON file.

    Args:
        state_file_path (str): The path to the state file.
        state_data (dict): The state dictionary to save.
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
        with open(state_file_path, 'w') as f:
            json.dump(state_data, f, indent=4) # Use indent for readability
        logging.info(f"Successfully saved state to '{state_file_path}'.")
    except IOError as e:
        logging.error(f"Error writing state to file '{state_file_path}'.", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving state to '{state_file_path}'.", exc_info=True)

if __name__ == '__main__':
    # Example Usage (for testing purposes)
    test_state_file = 'test_pipeline_state.json'
    initial_state = {'table1': '2023-01-01 00:00:00', 'table2': 12345}

    # Clean up previous test file if exists
    if os.path.exists(test_state_file):
        os.remove(test_state_file)
        print(f"Removed existing test file: {test_state_file}")

    # Test saving state
    print(f"\nAttempting to save state: {initial_state}")
    save_state(test_state_file, initial_state)

    # Test loading state
    print("\nAttempting to load state...")
    loaded_state = load_state(test_state_file)
    print(f"Loaded state: {loaded_state}")
    assert loaded_state == initial_state, "Loaded state does not match saved state!"
    print("State load/save test successful.")

    # Test loading from non-existent file
    non_existent_file = 'non_existent_state.json'
    print(f"\nAttempting to load state from non-existent file: {non_existent_file}")
    state_from_missing = load_state(non_existent_file)
    print(f"Loaded state: {state_from_missing}")
    assert state_from_missing == {}, "Loading from non-existent file should return empty dict."
    print("Non-existent file test successful.")

    # Test loading from empty file
    empty_file = 'empty_state.json'
    with open(empty_file, 'w') as f:
        pass # Create an empty file
    print(f"\nAttempting to load state from empty file: {empty_file}")
    state_from_empty = load_state(empty_file)
    print(f"Loaded state: {state_from_empty}")
    assert state_from_empty == {}, "Loading from empty file should return empty dict."
    print("Empty file test successful.")
    os.remove(empty_file) # Clean up

    # Test loading from invalid JSON file
    invalid_json_file = 'invalid_json_state.json'
    with open(invalid_json_file, 'w') as f:
        f.write("{invalid json")
    print(f"\nAttempting to load state from invalid JSON file: {invalid_json_file}")
    state_from_invalid = load_state(invalid_json_file)
    print(f"Loaded state: {state_from_invalid}")
    assert state_from_invalid == {}, "Loading from invalid JSON file should return empty dict."
    print("Invalid JSON file test successful.")
    os.remove(invalid_json_file) # Clean up

    # Clean up test state file
    if os.path.exists(test_state_file):
        os.remove(test_state_file)
        print(f"\nRemoved test file: {test_state_file}")
