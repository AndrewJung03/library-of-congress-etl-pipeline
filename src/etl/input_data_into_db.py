import psycopg2
import pandas as pd

DB_NAME = "newspapers"
DB_USER = "etl_user"
DB_PASSWORD = "9660"
DB_HOST = "localhost"
DB_PORT = "5432"

CLEAN_CSV = "data/cleaned/newspapers_cleaned.csv"

def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def input_into_db():
    print("\n=== LOADING CLEANED DATA INTO POSTGRES ===")

    df = pd.read_csv(CLEAN_CSV)
    print(f"Loaded {len(df)} cleaned rows.")
    conn = connect()
    cur = conn.cursor()
    
    # Populate newspapers Table  
    newspaper_rows = df[["item_lccn", "item_newspaper_title"]].drop_duplicates()
    for _, row in newspaper_rows.iterrows():

        cur.execute(
            """
            INSERT INTO newspapers (lccn, title)
            VALUES (%s, %s)
            ON CONFLICT (lccn) DO NOTHING;
            """,
            (row["item_lccn"], row["item_newspaper_title"])
        )
    conn.commit()
    print('"newspapers" table populated')

    # populate locations table
    location_rows = df[["location_city", "location_state", "location_country"]].drop_duplicates()
    for _, row in location_rows.iterrows():
        cur.execute(
            """
            INSERT INTO locations (city, state, country)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (row["location_city"], row["location_state"], row["location_country"])
        )
    conn.commit()
    print('"locations" table populated')

    #populate fact table (issues)
    for _, row in df.iterrows():

        # get newspaper_id
        cur.execute(
            "SELECT newspaper_id FROM newspapers WHERE lccn=%s",
            (row["item_lccn"],)
        )
        newspaper_id = cur.fetchone()[0]

        # get location_id
        cur.execute(
            """
            SELECT location_id FROM locations
            WHERE city=%s AND state=%s AND country=%s
            """,
            (row["location_city"], row["location_state"], row["location_country"])
        )
        location_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO issues (
                issue_loc_id,
                date_issued,
                title,
                medium,
                image_url,
                url,
                newspaper_id,
                location_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (issue_loc_id) DO NOTHING;
            """,
            (
                row["id"],
                row["item_date_issued"],
                row["title"],
                row["item_medium"],
                row["image_url"],
                row["url"],
                newspaper_id,
                location_id
            )
        )

    conn.commit()
    print('"issues" table populated')

    # languages + junction table

    for _, row in df.iterrows():

        # get issue_id
        cur.execute("SELECT issue_id FROM issues WHERE issue_loc_id=%s", (row["id"],))
        issue_id = cur.fetchone()[0]

        languages = [lang.strip() for lang in str(row["item_language"]).split(",")]

        for lang in languages:
            # insert language
            cur.execute(
                """
                INSERT INTO languages (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING;
                """,
                (lang,)
            )

            # get language_id
            cur.execute("SELECT language_id FROM languages WHERE name=%s", (lang,))
            language_id = cur.fetchone()[0]

            # insert junction
            cur.execute(
                """
                INSERT INTO issue_languages (issue_id, language_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (issue_id, language_id)
            )

    conn.commit()
    print('"language" table populataed')

    for _, row in df.iterrows():

        # get issue_id
        cur.execute("SELECT issue_id FROM issues WHERE issue_loc_id=%s", (row["id"],))
        issue_id = cur.fetchone()[0]

        subjects = [sub.strip() for sub in str(row["subject"]).split(",")]

        for sub in subjects:
            # insert subject
            cur.execute(
                """
                INSERT INTO subjects (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING;
                """,
                (sub,)
            )

            # get subject_id
            cur.execute("SELECT subject_id FROM subjects WHERE name=%s", (sub,))
            subject_id = cur.fetchone()[0]

            # insert junction
            cur.execute(
                """
                INSERT INTO issue_subjects (issue_id, subject_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (issue_id, subject_id)
            )

    conn.commit()
    cur.close()
    conn.close()

    print("\n=== FINISHED INSERTING DATA ===")



if __name__ == "__main__":
    input_into_db()