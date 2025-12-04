import psycopg2

from etl.create_tables import create_tables, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


def test_create_tables():
    """
    Tests that create_tables() successfully creates all required tables.
    """

    required_tables = {
        "newspapers",
        "locations",
        "issues",
        "languages",
        "issue_languages",
        "subjects",
        "issue_subjects"
    }

    create_tables()

    conn = connect()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public';
    """)

    existing_tables = {row[0] for row in cur.fetchall()}

    # Ensure required tables all exist
    for table in required_tables:
        assert table in existing_tables, f"Table '{table}' was not created."

    cur.close()
    conn.close()
