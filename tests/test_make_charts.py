import pytest
from pathlib import Path
from etl.make_charts import (
    connect,
    query_to_df,
    issues_per_year,
    issues_per_state,
    language_frequency,
    pages_per_issue,
    IMAGES_DIR,
)

def test_connect_returns_connection():
    conn = connect()
    assert conn is not None
    conn.close()


def test_query_to_df_runs():
    sql = "SELECT 1 AS test_col;"
    df = query_to_df(sql)
    assert len(df) == 1
    assert "test_col" in df.columns


def test_issues_per_year_creates_file():
    issues_per_year()
    out_path = IMAGES_DIR / "issues_per_year.png"
    assert out_path.exists()


def test_issues_per_state_creates_file():
    issues_per_state()
    out_path = IMAGES_DIR / "issues_per_state.png"
    assert out_path.exists()


def test_language_frequency_creates_file():
    language_frequency()
    out_path = IMAGES_DIR / "language_frequency.png"
    assert out_path.exists()


def test_pages_per_issue_creates_file():
    pages_per_issue()
    out_path = IMAGES_DIR / "pages_per_issue.png"
    assert out_path.exists()
