import yaml
from helpers import duckdb, extract

def load_config(config_file: str):
    """
    Loads a YAML configuration file.

    Parameters:
        config_file (str): Path to the YAML configuration file.

    Returns:
        dict: Parsed content of the YAML file.
    """
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"An error occurred while loading the config: {e}")
        return None

if __name__ == "__main__":
    
    config = load_config('config.yml')
    db=config['duckdb']['path']
    
    ## Extract and load into Duckdb
    for stream in config['streams']:
        table_name = stream['name']
        primary_key = stream['primary_key']

        for file in extract.get_file_paths(stream['path']):
            if stream['format'] == 'json':
                data = extract.read_json_records(file)
            elif stream['format'] == 'csv':
                data = extract.read_csv_records(file)
            else:
                raise Exception(f"Unsupported format {stream['format']}")

            # Perform upsert operation
            duckdb.upsert_data(
                db_file=db,
                df=data,
                table_name=table_name,
                primary_key=primary_key
            )

    ## Run Business Models
    for model in config['models']:
        duckdb.export_query_to_json(
            db_file=db,
            query=open(model['query']).read(),
            output_file=f'output/{model['name']}.json'
        )
