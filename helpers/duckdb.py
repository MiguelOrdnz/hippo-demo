import json
import duckdb
import pandas as pd

def upsert_data(db_file: str, df: pd.DataFrame, table_name: str, primary_key: str):
    """
    Upserts data from a DataFrame into a DuckDB table.

    Parameters:
        db_file (str): Path to the DuckDB database file.
        df (pd.DataFrame): Data to be inserted.
        table_name (str): Name of the DuckDB table.
        primary_key (str): Column name to be used as the primary key for upsert.
    """
    # Connect to the DuckDB database
    conn = duckdb.connect(db_file)

    try:
        # Check if the table exists
        table_exists = conn.execute(
            f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        ).fetchone()[0] > 0

        if not table_exists:
            # If table doesn't exist, create it
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            print(f"Table '{table_name}' created and data inserted.")
        else:
            # Perform upsert operation
            # Create a temporary table
            conn.execute("CREATE TEMP TABLE temp_table AS SELECT * FROM df")

            # Delete existing rows with matching primary key
            conn.execute(f"""
                DELETE FROM {table_name}
                WHERE {primary_key} IN (SELECT {primary_key} FROM temp_table)
            """)

            # Insert new records from the temporary table
            conn.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_table
            """)
            print(f"Data upserted into table '{table_name}'.")
    
    except Exception as e:
        print("An error occurred:", e)
    finally:
        # Close the connection
        conn.close()

def export_query_to_json(db_file: str, query: str, output_file: str):
    """
    Executes a DuckDB query and exports the results to a JSON file.

    Parameters:
        db_file (str): Path to the DuckDB database file.
        query (str): SQL query to execute.
        output_file (str): Path to the JSON file where results will be saved.

    Returns:
        None
    """
    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_file)

        # Execute the query and fetch results into a pandas DataFrame
        df = conn.execute(query).fetchdf()

        # Convert DataFrame to JSON and save it
        df.to_json(output_file, orient='records', lines=False)
           
        print(f"Query results successfully exported to {output_file}.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        conn.close()
