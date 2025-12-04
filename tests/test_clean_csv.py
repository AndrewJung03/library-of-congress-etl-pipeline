import pandas as pd
from pathlib import Path
from etl.clean_csv import clean_newspapers_csv, CLEANED_FILE, REJECTED_FILE, RAW_CSV, is_valid_date


def test_is_valid_date():
    assert is_valid_date("1900-01-01") == True
    assert is_valid_date("January 1, 1900") == True
    assert is_valid_date("") == False
    assert is_valid_date(None) == False
    assert is_valid_date("not-a-date") == False


def test_clean_newspapers_csv(tmp_path, monkeypatch):
    """
    Test that clean_newspapers_csv:
    - Reads a test CSV (not the real one)
    - Writes cleaned + rejected CSVs to temporary paths
    - Does not crash on missing columns because we include them all
    """
    # Create a FULL CSV with all columns
    input_csv = tmp_path / "input.csv"

    input_csv.write_text(
        "id,title,item_lccn,item_date_issued,item_newspaper_title,"
        "location_city,location_state,location_country,"
        "description,language,subject,image_url,"
        "item_medium,item_created_published,item_place_of_publication,item_language\n"
        "1,Hello,sn123,1910-01-01,Daily News,"
        "Juneau,Alaska,United States,"
        "desc,en,news,http://example.com,"
        "paper,1910,usa,en\n"
        ",MissingID,sn999,1910-01-02,Daily News,"
        "Juneau,Alaska,United States,"
        "desc,en,news,http://example.com,"
        "paper,1910,usa,en\n"
    )

    # Redirect module paths to tmp_path

    cleaned_csv = tmp_path / "cleaned.csv"
    rejected_csv = tmp_path / "rejected.csv"

    monkeypatch.setattr("etl.clean_csv.RAW_CSV", str(input_csv))
    monkeypatch.setattr("etl.clean_csv.CLEANED_FILE", cleaned_csv)
    monkeypatch.setattr("etl.clean_csv.REJECTED_FILE", rejected_csv)
    monkeypatch.setattr("etl.clean_csv.OUTPUT_DIR", tmp_path)

    # Run cleaning function
    clean_newspapers_csv()

    # Assertions
    assert cleaned_csv.exists(), "Cleaned CSV was not created"
    assert rejected_csv.exists(), "Rejected CSV was not created"

    df_clean = pd.read_csv(cleaned_csv)
    df_reject = pd.read_csv(rejected_csv)

    assert len(df_clean) == 1       
    assert len(df_reject) == 1    
    assert df_clean.iloc[0]["id"] == 1