import json
from pathlib import Path
from etl.fetch_from_api import fetch_from_api


def test_fetch_creates_output_file(tmp_path):
    """
    Test that an output file is created even when max_pages=0.
    No API calls are made in this case.
    """
    # Use temporary directory instead of real data/raw
    fake_raw_dir = tmp_path / "raw"
    fake_raw_dir.mkdir()

    # Patch module variable directly
    import etl.fetch_from_api as mod
    mod.raw_data_saved_dir = str(fake_raw_dir)

    output_path = fetch_from_api(collection="test_collection", max_pages=0)

    assert Path(output_path).exists()


def test_output_is_valid_json(tmp_path):
    """
    The output file should contain valid JSON list.
    """
    fake_raw_dir = tmp_path / "raw"
    fake_raw_dir.mkdir()

    import etl.fetch_from_api as mod
    mod.raw_data_saved_dir = str(fake_raw_dir)

    output_path = fetch_from_api(collection="sample", max_pages=0)

    with open(output_path, "r") as f:
        data = json.load(f)

    assert isinstance(data, list)


def test_filename_correct(tmp_path):
    """
    The filename should be <collection>_raw.json.
    """
    fake_raw_dir = tmp_path / "raw"
    fake_raw_dir.mkdir()

    import etl.fetch_from_api as mod
    mod.raw_data_saved_dir = str(fake_raw_dir)

    output_path = fetch_from_api(collection="abc123", max_pages=0)

    assert output_path.endswith("abc123_raw.json")
