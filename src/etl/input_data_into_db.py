import psycopg2
import pandas as pd
from .logger import get_logger   # <-- added

DB_NAME = "newspapers"
DB_USER = "etl_user"
DB_PASSWORD = "9660"
DB_HOST = "localhost"
DB_PORT = "5432"

CLEAN_CSV = "data/cleaned/newspapers_cleaned.csv"

# create logger
logger = get_logger("load_into_db")

def connect():
    logger.info("Attempting database connection for load step...")
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logger.info("Database connection successful.")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def input_into_db():
    logger.info("Starting LOAD step from cleaned CSV → Postgres.")
    print("\n--- LOADING CLEANED DATA INTO POSTGRES ---")

    try:
        df = pd.read_csv(CLEAN_CSV)
        logger.info(f"Loaded cleaned CSV: {CLEAN_CSV} ({len(df)} rows)")
    except Exception as e:
        logger.error(f"Failed to read cleaned CSV file: {e}")
        raise

    print(f"Loaded {len(df)} cleaned rows.")

    try:
        conn = connect()
        cur = conn.cursor()
    except Exception:
        logger.error("Could not acquire DB cursor/connection.")
        raise

    # Populate newspapers Table  
    logger.info("Populating newspapers table...")
    newspaper_rows = df[["item_lccn", "item_newspaper_title"]].drop_duplicates()
    for _, row in newspaper_rows.iterrows():
        try:
            cur.execute(
                """
                INSERT INTO newspapers (lccn, title)
                VALUES (%s, %s)
                ON CONFLICT (lccn) DO NOTHING;
                """,
                (row["item_lccn"], row["item_newspaper_title"])
            )
        except Exception as e:
            logger.error(f"Failed inserting newspaper row {row}: {e}")
            raise

    conn.commit()
    logger.info('"newspapers" table populated.')
    print('"newspapers" table populated')

    # populate locations table
    logger.info("Populating locations table...")
    location_rows = df[["location_city", "location_state", "location_country"]].drop_duplicates()
    for _, row in location_rows.iterrows():
        try:
            cur.execute(
                """
                INSERT INTO locations (city, state, country)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (row["location_city"], row["location_state"], row["location_country"])
            )
        except Exception as e:
            logger.error(f"Failed inserting location row {row}: {e}")
            raise

    conn.commit()
    logger.info('"locations" table populated.')
    print('"locations" table populated')

    # populate fact table (issues)
    logger.info("Populating issues table...")
    for _, row in df.iterrows():

        try:
            cur.execute(
                "SELECT newspaper_id FROM newspapers WHERE lccn=%s",
                (row["item_lccn"],)
            )
            newspaper_id = cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to fetch newspaper_id for row {row['id']}: {e}")
            raise

        try:
            cur.execute(
                """
                SELECT location_id FROM locations
                WHERE city=%s AND state=%s AND country=%s
                """,
                (row["location_city"], row["location_state"], row["location_country"])
            )
            location_id = cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Failed to fetch location_id for row {row['id']}: {e}")
            raise

        try:
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
        except Exception as e:
            logger.error(f"Failed inserting issue row {row['id']}: {e}")
            raise

    conn.commit()
    logger.info('"issues" table populated.')
    print('"issues" table populated')

    # languages + junction
    logger.info("Populating languages + issue_languages tables...")
    for _, row in df.iterrows():

        cur.execute("SELECT issue_id FROM issues WHERE issue_loc_id=%s", (row["id"],))
        issue_id = cur.fetchone()[0]

        languages = [lang.strip() for lang in str(row["item_language"]).split(",")]

        for lang in languages:
            try:
                cur.execute(
                    """
                    INSERT INTO languages (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING;
                    """,
                    (lang,)
                )

                cur.execute("SELECT language_id FROM languages WHERE name=%s", (lang,))
                language_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO issue_languages (issue_id, language_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (issue_id, language_id)
                )
            except Exception as e:
                logger.error(f"Failed inserting language '{lang}' for issue {issue_id}: {e}")
                raise

    conn.commit()
    logger.info('"language" table populated.')
    print('"language" table populataed')

    # subjects + junction
    logger.info("Populating subjects + issue_subjects tables...")
    for _, row in df.iterrows():

        cur.execute("SELECT issue_id FROM issues WHERE issue_loc_id=%s", (row["id"],))
        issue_id = cur.fetchone()[0]

        subjects = [sub.strip() for sub in str(row["subject"]).split(",")]

        for sub in subjects:
            try:
                cur.execute(
                    """
                    INSERT INTO subjects (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO NOTHING;
                    """,
                    (sub,)
                )

                cur.execute("SELECT subject_id FROM subjects WHERE name=%s", (sub,))
                subject_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO issue_subjects (issue_id, subject_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (issue_id, subject_id)
                )
            except Exception as e:
                logger.error(f"Failed inserting subject '{sub}' for issue {issue_id}: {e}")
                raise

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Finished inserting all cleaned data into Postgres.")
    print("\n--- FINISHED INSERTING DATA ---")
