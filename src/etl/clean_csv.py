import pandas as pd
from pathlib import Path
from dateutil.parser import parse
from .logger import get_logger  

RAW_CSV = "data/processed/newspapers.csv"      
OUTPUT_DIR = Path("data/cleaned")
CLEANED_FILE = OUTPUT_DIR / "newspapers_cleaned.csv"
REJECTED_FILE = OUTPUT_DIR / "newspapers_rejected.csv"

logger = get_logger("clean_csv") 

def is_valid_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == "":
        return False
    try:
        parse(date_str)
        return True
    except Exception:
        return False
    
def clean_newspapers_csv():
    logger.info("Starting cleaning process for RAW CSV.")
    print("\n--- CLEANING NEWSPAPERS CSV ---\n")

    # Load data
    try:
        df = pd.read_csv(RAW_CSV)
        logger.info(f"Loaded raw CSV '{RAW_CSV}' with {len(df)} rows.")
    except Exception as e:
        logger.error(f"Failed to load raw CSV: {e}")
        raise

    original_count = len(df)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Track stats
    stats = {
        "invalid_date_rows": 0,
        "missing_critical_rows": 0,
        "missing_location_rows": 0,
        "placeholder_fills": {},
        "lowercase_ops": {}
    }

    critical_fields = [
        "id", "title", "item_lccn", "item_date_issued", "item_newspaper_title"
    ]

    location_fields = [
        "location_city", "location_state", "location_country"
    ]

    non_critical_fill_unknown = [
        "description", "language", "subject", "image_url",
        "item_medium", "item_created_published",
        "item_place_of_publication", "item_language"
    ]

    lowercase_fields = [
        "title", "description", "language", "subject",
        "location_city", "location_state", "location_country",
        "item_medium", "item_language",
        "item_created_published", "item_newspaper_title",
        "item_place_of_publication"
    ]

    print(f"Rows before cleaning: {original_count}")
    logger.info(f"Rows before cleaning: {original_count}")

    # 1. Remove duplicate IDs
    before = len(df)
    df = df.drop_duplicates(subset=["id"])
    removed_duplicates = before - len(df)
    logger.info(f"Removed {removed_duplicates} duplicate ID rows.")

    # 2. Reject missing CRITICAL fields
    rejected_rows = pd.DataFrame(columns=df.columns)

    for field in critical_fields:
        missing_mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
        if missing_mask.any():
            reject_chunk = df[missing_mask]
            rejected_rows = pd.concat([rejected_rows, reject_chunk], ignore_index=True)
            count = len(reject_chunk)
            stats["missing_critical_rows"] += count
            df = df[~missing_mask]
            logger.info(f"Rejected {count} rows missing critical field '{field}'.")

    # 3. Reject invalid dates
    invalid_date_mask = ~df["item_date_issued"].apply(is_valid_date)
    if invalid_date_mask.any():
        reject_chunk = df[invalid_date_mask]
        rejected_rows = pd.concat([rejected_rows, reject_chunk], ignore_index=True)
        count = len(reject_chunk)
        stats["invalid_date_rows"] += count
        df = df[~invalid_date_mask]
        logger.info(f"Rejected {count} rows with invalid date formats.")

    # 4. Reject rows missing location fields
    for loc_field in location_fields:
        missing_mask = df[loc_field].isna() | (df[loc_field].astype(str).str.strip() == "")
        if missing_mask.any():
            reject_chunk = df[missing_mask]
            rejected_rows = pd.concat([rejected_rows, reject_chunk], ignore_index=True)
            count = len(reject_chunk)
            stats["missing_location_rows"] += count
            df = df[~missing_mask]
            logger.info(f"Rejected {count} rows missing location field '{loc_field}'.")

    # 5. Fill non-critical missing fields with "unknown"
    for field in non_critical_fill_unknown:
        mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
        fill_count = mask.sum()
        if fill_count > 0:
            stats["placeholder_fills"][field] = fill_count
            df.loc[mask, field] = "unknown"
            logger.info(f"Filled {fill_count} missing '{field}' fields with 'unknown'.")

    # 6. Lowercase fields
    for field in lowercase_fields:
        mask = df[field].notna()
        df.loc[mask, field] = df.loc[mask, field].astype(str).str.lower()
        stats["lowercase_ops"][field] = mask.sum()
        logger.info(f"Applied lowercase operation on {mask.sum()} values for '{field}'.")

    # 7. Save outputs
    try:
        df.to_csv(CLEANED_FILE, index=False)
        rejected_rows.to_csv(REJECTED_FILE, index=False)
        logger.info(f"Saved cleaned CSV → {CLEANED_FILE}")
        logger.info(f"Saved rejected CSV → {REJECTED_FILE}")
    except Exception as e:
        logger.error(f"Failed to save cleaned or rejected CSVs: {e}")
        raise

    final_count = len(df)

    # 8. Print summary
    print("\n--- CLEANING SUMMARY ---")
    print(f"Rows before cleaning: {original_count}")
    print(f"Duplicates removed:  {removed_duplicates}")
    print(f"Rows rejected:       {len(rejected_rows)}")
    print(f"Rows after cleaning: {final_count}\n")

    print("Reasons for rejection:")
    print(f"  Missing critical fields: {stats['missing_critical_rows']}")
    print(f"  Invalid date formats:    {stats['invalid_date_rows']}")
    print(f"  Missing location fields: {stats['missing_location_rows']}\n")

    print("Placeholder fills:")
    for field, count in stats["placeholder_fills"].items():
        print(f"  {field}: {count}")

    print("\nLowercase operations:")
    for field, count in stats["lowercase_ops"].items():
        print(f"  {field}: {count}")

    print(f"\nCleaned CSV saved to:  {CLEANED_FILE}")
    print(f"Rejected CSV saved to: {REJECTED_FILE}")

    logger.info("Cleaning pipeline completed successfully.")
