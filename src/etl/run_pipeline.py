import time

from src.etl.fetch_from_api import fetch_from_api
from src.etl.transform_to_csv import json_to_csv
from src.etl.clean_csv import clean_newspapers_csv
from src.etl.create_tables import create_tables
from src.etl.input_data_into_db import input_into_db
from src.etl.make_charts import (
    issues_per_year,
    language_frequency,
    issues_per_state,
    pages_per_issue
)

def run_pipeline():
    print("\n --- ETL PIPELINE STARTED ---")
    print("\n")
    print("\n")
    print("\n--- FETCHING DATA FROM API ---")
    fetch_from_api(collection="newspapers", max_pages=25)
    print("\n--- TRANSFORMING JSON → CSV...")
    json_to_csv("data/raw/newspapers_raw.json")
    time.sleep(1)
    print("\n--- CLEANING CSV DATA...")
    clean_newspapers_csv()
    time.sleep(1)
    print("\n--- CREATING DATABASE TABLES...") 
    create_tables()
    time.sleep(1)
    print("\n--- INPUTTING DATA INTO DATABASE...")
    input_into_db()
    time.sleep(1)
    print("\n--- GENERATING CHARTS...")
    issues_per_year()
    issues_per_state()
    language_frequency()
    pages_per_issue()
    print("\n--- ETL PIPELINE COMPLETED ---")

if __name__ == "__main__":
    run_pipeline()