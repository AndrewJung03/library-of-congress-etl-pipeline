import csv
import json
import os
from .logger import get_logger 

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

logger = get_logger("json_to_csv")

def safe_get(obj, key, default=""): 
    if key not in obj:
        return default

    value = obj[key]

    if isinstance(value, list):
        if len(value) == 0:
            return ""

        string_list = [str(item) for item in value]

        return ", ".join(string_list)

    return value

def safe_get_nested(obj, parent, key, default=""):
    nested = obj.get(parent, {})
    if not isinstance(nested, dict):
        return default

    val = nested.get(key, default)

    if isinstance(val, list):
        if not val:
            return ""
        return ", ".join(str(v) for v in val)
    return val

def json_to_csv(input_json_path):
    logger.info(f"Starting JSON → CSV conversion: {input_json_path}")

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    logger.info(f"Ensured processed directory exists: {PROCESSED_DIR}")

    try:
        with open(input_json_path, "r") as f:
            data = json.load(f)
        logger.info(f"Loaded JSON file successfully: {input_json_path}")
    except Exception as e:
        logger.error(f"Failed to load JSON file {input_json_path}: {e}")
        raise

    if not isinstance(data, list):
        logger.error("Input JSON root is not a list of objects.")
        raise ValueError("Input JSON should be a list of objects")

    # Define columns for the CSV
    fields = [
        "id",
        "title",
        "date",
        "description",
        "digitized",
        "language",
        "subject",
        "location_city",
        "location_state",
        "location_country",
        "image_url",
        "url",
        # nested fields (inside 'item')
        "item_date_issued",
        "item_created_published",
        "item_medium",
        "item_language",
        "item_newspaper_title",
        "item_lccn",
        "item_place_of_publication",
    ]

    csv_name = input_json_path.replace(RAW_DIR + "/", "").replace("_raw.json", ".csv")
    csv_path = os.path.join(PROCESSED_DIR, csv_name)

    logger.info(f"Preparing to write CSV to: {csv_path}")

    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()

            for item in data:
                row = {
                    "id": safe_get(item, "id"),
                    "title": safe_get(item, "title"),
                    "date": safe_get(item, "date"),
                    "description": safe_get(item, "description"),
                    "digitized": safe_get(item, "digitized"),
                    "language": safe_get(item, "language"),
                    "subject": safe_get(item, "subject"),
                    "location_city": safe_get(item, "location_city"),
                    "location_state": safe_get(item, "location_state"),
                    "location_country": safe_get(item, "location_country"),
                    "image_url": safe_get(item, "image_url").split(", ")[0]
                    if safe_get(item, "image_url")
                    else "",
                    "url": safe_get(item, "url"),
                    "item_date_issued": safe_get_nested(item, "item", "date_issued"),
                    "item_created_published": safe_get_nested(item, "item", "created_published"),
                    "item_medium": safe_get_nested(item, "item", "medium"),
                    "item_language": safe_get_nested(item, "item", "language"),
                    "item_newspaper_title": safe_get_nested(item, "item", "newspaper_title"),
                    "item_lccn": safe_get_nested(item, "item", "library_of_congress_control_number"),
                    "item_place_of_publication": safe_get_nested(item, "item", "place_of_publication"),
                }

                writer.writerow(row)

        logger.info(f"Successfully wrote CSV file: {csv_path}")

    except Exception as e:
        logger.error(f"Failed writing CSV {csv_path}: {e}")
        raise

    print(f"CSV created at: {csv_path}")
    logger.info(f"CSV created at: {csv_path}")

    return csv_path
