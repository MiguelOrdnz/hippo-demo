import os
import json
import pandas as pd

def read_csv_records(csv_file: str) -> pd.DataFrame:
    """
    Reads records from a CSV file and returns a pandas DataFrame.

    Parameters:
        csv_file (str): Path to the CSV file.

    Returns:
        pd.DataFrame: Data from the CSV file.
    """
    try:
        df = pd.read_csv(csv_file)
        return df
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

def read_json_records(json_file: str) -> pd.DataFrame:
    """
    Reads records from a JSON file and returns a pandas DataFrame.

    Parameters:
        json_file (str): Path to the JSON file.

    Returns:
        pd.DataFrame: Data from the JSON file.
    """
    with open(json_file, 'r') as f:
        json_data = json.load(f)

    # Convert JSON data to DataFrame
    df = pd.DataFrame(json_data)
    return df

def get_file_paths(file_path: str):
    """
    Retrieves all file names from the specified directory.

    This function lists all entries in the given directory and filters out
    only the files, excluding subdirectories or other non-file entries.

    Parameters:
        file_path (str): The path to the directory to scan.

    Returns:
        list: A list of file names in the directory.
    """
    # List all entries in the directory
    entries = os.listdir(file_path)

    # Filter to get only files
    files = [os.path.join(file_path, f) for f in entries if os.path.isfile(os.path.join(file_path, f))]
    
    return files
