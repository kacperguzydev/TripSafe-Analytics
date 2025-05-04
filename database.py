import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("database")

def create_database():
    logger.info("Checking for 'tripsafe' database…")
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'tripsafe'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE tripsafe")
        logger.info("Database 'tripsafe' created.")
    else:
        logger.info("Database 'tripsafe' already exists.")
    cur.close()
    conn.close()

def create_tables():
    logger.info("Creating tables in 'tripsafe'…")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cmds = [
        """
        CREATE TABLE IF NOT EXISTS trip_events (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50),
            trip_id TEXT,
            rider_id TEXT,
            driver_id TEXT,
            city VARCHAR(100),
            timestamp TIMESTAMPTZ,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trip_metrics (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            event_date DATE,
            total_trips INT,
            avg_trip_duration FLOAT,
            cancellations INT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trip_alerts (
            id SERIAL PRIMARY KEY,
            trip_id TEXT,
            driver_id TEXT,
            alert_type VARCHAR(100),
            description TEXT,
            timestamp TIMESTAMPTZ
        )
        """
    ]
    for sql in cmds:
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Tables ready.")

if __name__ == "__main__":
    create_database()
    create_tables()
