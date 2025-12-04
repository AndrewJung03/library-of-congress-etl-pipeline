import builtins
import pandas as pd
from unittest.mock import MagicMock, patch

from etl.input_data_into_db import input_into_db


def test_input_into_db_runs_insertions():

    # --- Create a small mock DataFrame to replace reading the real CSV ---
    mock_df = pd.DataFrame({
        "item_lccn": ["sn123"],
        "item_newspaper_title": ["Daily News"],
        "location_city": ["Juneau"],
        "location_state": ["Alaska"],
        "location_country": ["United States"],
        "id": ["abc123"],
        "item_date_issued": ["1910-01-01"],
        "title": ["Sample Title"],
        "item_medium": ["paper"],
        "image_url": ["http://example.com"],
        "url": ["http://example.com"],
        "item_language": ["en"],
        "subject": ["news"]
    })

    # --- Patch pandas.read_csv to return our mock dataframe ---
    with patch("pandas.read_csv", return_value=mock_df):

        # --- Mock psycopg2 connection + cursor ---
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Patch connect() so it returns our fake connection
        with patch("etl.input_data_into_db.connect", return_value=mock_conn):

            # Run the function
            input_into_db()

            # Assertions: Verify INSERT statements actually ran
            assert mock_cursor.execute.call_count > 0, "No SQL inserts ran"
            mock_conn.commit.assert_called()
            mock_cursor.close.assert_called()
            mock_conn.close.assert_called()
