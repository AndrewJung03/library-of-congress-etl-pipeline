import psycopg2

DB_NAME = "newspapers"
DB_USER = "etl_user"
DB_PASSWORD = "9660"
DB_HOST = "localhost"
DB_PORT = "5432"


def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )


def create_tables():
    drop_commands = [
        "DROP TABLE IF EXISTS issue_subjects CASCADE;",
        "DROP TABLE IF EXISTS subjects CASCADE;",
        "DROP TABLE IF EXISTS issue_languages CASCADE;",
        "DROP TABLE IF EXISTS languages CASCADE;",
        "DROP TABLE IF EXISTS issues CASCADE;",
        "DROP TABLE IF EXISTS locations CASCADE;",
        "DROP TABLE IF EXISTS newspapers CASCADE;"
    ]

    create_commands = [

        # newspapers
        """
        CREATE TABLE IF NOT EXISTS newspapers (
            newspaper_id SERIAL PRIMARY KEY,
            lccn TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL
        );
        """,

        # locations
        """
        CREATE TABLE IF NOT EXISTS locations (
            location_id SERIAL PRIMARY KEY,
            city TEXT NOT NULL,
            state TEXT NOT NULL,
            country TEXT NOT NULL
        );
        """,

        # issues
        """
        CREATE TABLE IF NOT EXISTS issues (
            issue_id SERIAL PRIMARY KEY,
            issue_loc_id TEXT UNIQUE NOT NULL,
            date_issued DATE NOT NULL,
            title TEXT NOT NULL,
            medium TEXT,
            image_url TEXT,
            url TEXT,
            newspaper_id INTEGER NOT NULL REFERENCES newspapers(newspaper_id) ON DELETE CASCADE,
            location_id INTEGER NOT NULL REFERENCES locations(location_id) ON DELETE CASCADE
        );
        """,

        # languages
        """
        CREATE TABLE IF NOT EXISTS languages (
            language_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """,

        # issue_languages
        """
        CREATE TABLE IF NOT EXISTS issue_languages (
            issue_id INTEGER NOT NULL REFERENCES issues(issue_id) ON DELETE CASCADE,
            language_id INTEGER NOT NULL REFERENCES languages(language_id) ON DELETE CASCADE,
            PRIMARY KEY (issue_id, language_id)
        );
        """,

        # subjects
        """
        CREATE TABLE IF NOT EXISTS subjects (
            subject_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        """,

        # issue_subjects
        """
        CREATE TABLE IF NOT EXISTS issue_subjects (
            issue_id INTEGER NOT NULL REFERENCES issues(issue_id) ON DELETE CASCADE,
            subject_id INTEGER NOT NULL REFERENCES subjects(subject_id) ON DELETE CASCADE,
            PRIMARY KEY (issue_id, subject_id)
        );
        """
    ]

    conn = connect()
    cur = conn.cursor()

    # Drop all existing tables
    for command in drop_commands:
        cur.execute(command)

    # Recreate tables fresh
    for command in create_commands:
        cur.execute(command)

    conn.commit()
    cur.close()
    conn.close()

    print("All tables dropped and recreated successfully")

