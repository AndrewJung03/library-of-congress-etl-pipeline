
<!-- Languages -->
![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-PostgreSQL-blue?logo=postgresql)

<!-- Frameworks / Libraries -->
![Pandas](https://img.shields.io/badge/Pandas-Data%20Processing-orange?logo=pandas)
![Matplotlib](https://img.shields.io/badge/Matplotlib-Visualization-yellow)
![psycopg2](https://img.shields.io/badge/psycopg2-Postgres%20Adapter-lightgrey)
![Gemini](https://img.shields.io/badge/Google%20Gemini-API-purple?logo=google)
![dotenv](https://img.shields.io/badge/python--dotenv-Env%20Management-green)
![Pytest](https://img.shields.io/badge/Pytest-Testing-blue?logo=pytest)
![Library of Congress](https://img.shields.io/badge/Library%20of%20Congress-API-blue)

# Library of Congress Newspaper ETL Pipeline
This project is a fully automated ETL (Extract, Transform, Load) pipeline that downloads historical newspaper data from the Library of Congress API. It then cleans and normalizes the data, creates SQL tables in a PostgreseSQL database, loads the data into the tables, generates analytic charts, and then supports AI insights powered by Google Gemini.

## Project Overview

### Extract
- Fetches JSON data from the Library on Congress Newspaper API
- Handles pagination
- Saves combined raw JSON into data/raw/

### Transform
- Converts JSON --> CSV 
- Cleans invalid rows, enforces schema rules, standardizes text fields
- produces
    - data/cleaned/newspapers_cleaned.csv
    - data/cleaned/newspapers_rejected.csv

### Load
- Creates normalized relational schema in PostgreSQL
- Inserts cleaned data into tables

### Analyze 
- Creates visual charts
    - Issues per year
    - Language frequency
    - Pages per issue
- Uses AI to create insights
    - loads cleaned CSV
    - Lets user chat with AI assistant (Google Gemini) about the dataset
    - Provides insights, statistics, patterns, and explanations

## Folder Structure 
```
etl-pipeline-project/
│
├── data/
│   ├── raw/                 
│   │    └── newspapers_raw.json         # Raw JSON downloaded from LOC API
│   │
│   ├── processed/           
│   │    └── newspapers.csv              # CSV created from raw JSON
│   │
│   └── cleaned/             
│        ├── newspapers_cleaned.csv      # Cleaned dataset (final)
│        └── newspapers_rejected.csv     # Rows removed during validation
│
├── images/                  
│   ├── issues_per_year.png
│   ├── language_frequency.png
│   └── pages_per_issue.png
│
├── src/
│   └── etl/
│        ├── fetch_from_api.py           # Fetch raw JSON from the API
│        ├── transform_to_csv.py         # Convert JSON → CSV
│        ├── clean_csv.py                # Clean data & validate records
│        ├── create_tables.py            # Build PostgreSQL schema
│        ├── input_data_into_db.py       # Load cleaned data into PostgreSQL
│        ├── make_charts.py              # Generate analytics charts
│        ├── ai_client.py                # AI insights using Gemini
│        ├── logger.py                   # Centralized ETL logging
│        └── run_pipeline.py             # Orchestrates the full ETL pipeline
│
├── tests/                               # Unit tests for every ETL stage
│
└── README.md

```

