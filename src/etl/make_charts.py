import psycopg2
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

DB_NAME = "newspapers"
DB_USER = "etl_user"
DB_PASSWORD = "9660"
DB_HOST = "localhost"
DB_PORT = "5432"

IMAGES_DIR = Path("images")
IMAGES_DIR.mkdir(exist_ok=True)


def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


def query_to_df(sql):
    conn = connect()
    df = pd.read_sql_query(sql, conn)
    conn.close()
    return df

def issues_per_year():
    sql = """
    SELECT
        EXTRACT(YEAR FROM date_issued)::INT AS year,
        COUNT(*) AS issue_count
    FROM issues
    GROUP BY year
    ORDER BY year;
    """

    df = query_to_df(sql)

    if df.empty:
        print("No data returned from database! Chart will be blank.")
        return

    # Convert to int (EXTRACT returns float)
    df["year"] = df["year"].astype(int)

    plt.figure(figsize=(10,6))
    plt.plot(df['year'], df['issue_count'], marker='o')
    plt.title('Number of Issues Published Per Year')
    plt.xlabel('Year')
    plt.ylabel('Number of Issues')
    plt.grid(True)
    plt.savefig(IMAGES_DIR / 'issues_per_year.png')
    plt.close()


def issues_per_state():
    sql = """
        SELECT
            l.state,
            COUNT(i.issue_id) AS issue_count
        FROM issues i
        JOIN locations l ON i.location_id = l.location_id
        GROUP BY l.state
        ORDER BY issue_count DESC;
    """
    df = query_to_df(sql)
    if df.empty:
        print("No data for issues per state")
        return
    plt.figure(figsize=(12,6))
    plt.bar(df['state'], df['issue_count'])
    plt.xticks(rotation=90)
    plt.title('Number of Issues Published Per State')
    plt.xlabel('State')
    plt.ylabel('Number of Issues')
    plt.tight_layout()
    plt.savefig(IMAGES_DIR / 'issues_per_state.png')
    plt.close()

def language_frequency():
    sql = """
        SELECT
            l.name AS language,
            COUNT(il.issue_id) AS issue_count
        FROM languages l
        JOIN issue_languages il ON l.language_id = il.language_id
        GROUP BY l.name
        ORDER BY issue_count DESC;
    """

    df = query_to_df(sql)
    if df.empty:
        print("No data for language frequency")
        return
    plt.figure(figsize=(12,6))
    plt.bar(df['language'], df['issue_count'])
    plt.xticks(rotation=90)
    plt.title('Number of Issues by Language')
    plt.xlabel('Language')
    plt.ylabel('Number of Issues')
    plt.tight_layout()
    plt.savefig(IMAGES_DIR / 'language_frequency.png')
    plt.close()

def pages_per_issue():
    sql = """
        SELECT
            REGEXP_REPLACE(medium, '[^0-9]', '', 'g')::INT AS page_count
        FROM issues
        WHERE medium IS NOT NULL
          AND REGEXP_REPLACE(medium, '[^0-9]', '', 'g') ~ '^[0-9]+$';
    """

    df = query_to_df(sql)

    if df.empty:
        print("No page-count data found.")
        return
    count_df = df["page_count"].value_counts().sort_index()

    plt.figure(figsize=(10, 6))
    plt.bar(count_df.index, count_df.values)

    plt.title("Distribution of Page Counts per Issue")
    plt.xlabel("Page Count")
    plt.ylabel("Number of Issues")
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    plt.savefig(IMAGES_DIR / "pages_per_issue.png")
    plt.close()




