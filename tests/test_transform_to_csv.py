from etl.transform_to_csv import safe_get
from etl.transform_to_csv import safe_get_nested
import json
import csv
from etl.transform_to_csv import json_to_csv
from pathlib import Path

def test_safe_get_simple_value():
    obj = {"title": "Newspaper"}
    assert safe_get(obj, "title") == "Newspaper"

def test_safe_get_missing_key():
    obj = {}
    assert safe_get(obj, "title") == ""

def test_safe_get_list():
    obj = {"subject": ["news", "alaska"]}
    assert safe_get(obj, "subject") == "news, alaska"

def test_safe_get_empty_list():
    obj = {"subject": []}
    assert safe_get(obj, "subject") == ""


def test_safe_get_nested_simple():
    obj = {"item": {"language": "eng"}}
    assert safe_get_nested(obj, "item", "language") == "eng"

def test_safe_get_nested_list():
    obj = {"item": {"language": ["eng", "spa"]}}
    assert safe_get_nested(obj, "item", "language") == "eng, spa"

def test_safe_get_nested_missing_key():
    obj = {"item": {}}
    assert safe_get_nested(obj, "item", "language") == ""

def test_safe_get_nested_missing_parent():
    obj = {}
    assert safe_get_nested(obj, "item", "language") == ""

def test_json_to_csv_creates_valid_csv(tmp_path, monkeypatch):
    fake_data = [
        {
            "id": "123",
            "title": "Test Title",
            "date": "1900-01-01",
            "description": ["desc"],
            "digitized": True,
            "language": ["english"],
            "subject": ["news"],
            "location_city": ["juneau"],
            "location_state": ["alaska"],
            "location_country": ["usa"],
            "image_url": ["http://image.com/img.jpg"],
            "url": "http://loc/id/123",

            "item": {
                "date_issued": "1900-01-01",
                "created_published": ["Juneau"],
                "medium": "4 pages",
                "language": ["eng"],
                "newspaper_title": ["Daily News"],
                "library_of_congress_control_number": "sn12345",
                "place_of_publication": "Juneau, Alaska"
            }
        }
    ]
    json_file = tmp_path / "sample_raw.json"
    with open(json_file, "w") as f:
        json.dump(fake_data, f)

    # 3. Redirect PROCESSED_DIR to tmp_path
    monkeypatch.setattr("etl.transform_to_csv.PROCESSED_DIR", tmp_path)

    csv_path = Path(json_to_csv(str(json_file)))
    assert csv_path.exists()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1
    row = rows[0]

    assert row["id"] == "123"
    assert row["title"] == "Test Title"
    assert row["language"] == "english"
    assert row["item_language"] == "eng"
    assert row["item_newspaper_title"] == "Daily News"


