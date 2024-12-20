# Hippo-Demo

A simple ETL (Extract, Transform, Load) pipeline for Hippo that leverages Python and DuckDB to process and analyze data. This project demonstrates how to build an efficient ETL workflow while generating models in a structured and organized output folder.

## Features
	•	ETL Pipeline: Extract data from multiple sources, transform it, and load it into DuckDB for analysis.
	•	Python & DuckDB Integration: Uses DuckDB for in-memory database operations and Python for orchestration.
	•	Config-Driven: Easily configurable via a YAML file to define streams and models.
	•	Output Models: Query results are saved as JSON files in the output folder for further processing or sharing.

## Prerequisites
	1.	Docker (for development container)
	2.	Python 3.8+
	3.	pip (Python package manager)

## Installation

1. Clone the Repository

git clone [https://github.com/MiguelOrdnz/hippo-demo.git](https://github.com/MiguelOrdnz/hippo-demo.git)
cd hippo-demo

2. Set Up the Development Environment

This project is configured to work with a development container (devcontainer). If you use Visual Studio Code:
	1.	Install the Remote - Containers extension.
	2.	Open the repository folder in VS Code.
	3.	Reopen in the container: Ctrl+Shift+P > Remote-Containers: Reopen in Container.

## Usage

1. Configure the Pipeline

Modify the config.yml file to define:
	•	Streams: Input data sources (pharmacies, claims, reverts).
	•	Models: Queries for generating output models.

Example `config.yml`:

```yaml
duckdb:
  path: hippo.db

streams:
  - name: pharmacies
    path: data/pharmacies
    primary_key: npi
    format: csv
  - name: reverts
    path: data/reverts
    primary_key: id
    format: json
  - name: claims
    path: data/claims
    primary_key: id
    format: json

models:
  - name: metrics
    query: sql/metrics.sql
  - name: recommendations
    query: sql/recommendations.sql
  - name: most_prescribed
    query: sql/most_prescribed.sql
```

2. Run the ETL Pipeline

```
python main.py
```

This will:
	1.	Extract data from the specified sources in config.yml.
	2.	Transform and load the data into DuckDB.
	3.	Generate output models as JSON files in the output folder.

##  Output

Generated models are saved in the output directory as JSON files. Each file corresponds to a model defined in the config.yml.

Example `output/metrics.json`:

```json
[
    {
        "npi": "4567890123",
        "ndc": "00078017705",
        "fills": 162,
        "reverted": 3,
        "avg_price": 40131.899382716,
        "total_price": 6501367.6999999983
    }
]
```

Example `output/most_prescribed.json`:

```json
[
    {
        "ndc": "55154445200",
        "most_prescribed_quantity": [
            1.0,
            10.0,
            15.0,
            30.0,
            45.0
        ]
    }
]
```

Example `output/recommendations.json`:

```json
[
    {
        "ndc": "55154445200",
        "chain": [
            {
            "name": "doctor",
            "avg_price": 0.0011304544
            },
            {
            "name": "health",
            "avg_price": 0.0011304544
            },
            ...
        ]
    }
]
```

## License

This project is licensed under the MIT License.

Author

[Miguel Ordonez](miguel.ordonez.rosales@gmail.com)
Feel free to reach out for questions or suggestions!
