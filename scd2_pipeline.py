import logging
import os
import time

import psycopg

from config import setup_logger

logger = logging.getLogger(__name__)


class SCD2Pipeline:
    """
    SCD2Pipeline handles the incremental loading and management of
    a slowly changing dimension type 2 table `scd2_dimensional_users`
    from the raw source `raw_users`.
    """
    
    def __init__(self, 
                 dbname: str,
                 user: str,
                 password: str,
                 host: str,
                 port: str
                ):
        self.connection_args = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        
        logger.info("Initializing SCD2Pipeline")
    
    def check_if_table_is_empty(self, conn: psycopg.Connection) -> bool:
        """Check if the dimension table has any rows."""
        
        with conn.cursor() as curr:
            curr.execute("SELECT COUNT(*) FROM scd2_dimensional_users;")
            rows_count = curr.fetchone()[0]
            logger.info("Dimension table has %d rows.", rows_count)
            return rows_count == 0
    
    def create_last_records_stg(self, conn: psycopg.Connection) -> None:
        """
        Create a temporary staging table with the latest records per user.
        Only records newer than the last ts_db in the dimension table are selected.
        """
        
        with conn.cursor() as curr:
            curr.execute(
                """
                DROP TABLE IF EXISTS stg_latest_users;
                CREATE TEMP TABLE stg_latest_users AS
                WITH last_users AS (
                    SELECT user_id, first_name, last_name, birthday,
                           email, created_at, updated_at, ts_db,
                           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts_db DESC) as rn
                    FROM raw_users
                    WHERE ts_db > COALESCE(
                        (SELECT MAX(ts_db) FROM scd2_dimensional_users),
                        '1900-01-01'::timestamp
                    )
                )
                SELECT *
                FROM last_users
                WHERE rn = 1;
                """
            )
        logger.info("Temporary staging table 'stg_latest_users' created.")
    
    def full_load(self, conn: psycopg.Connection) -> None:
        """Insert all records from staging to the dimension table for full load."""
        
        with conn.cursor() as curr:
            curr.execute(
                """
                INSERT INTO scd2_dimensional_users (
                    user_id, first_name, last_name, birthday, email,
                    created_at, updated_at, ts_db, actual_from, actual_to, is_actual
                )
                SELECT user_id, first_name, last_name, birthday,
                       email, created_at, updated_at, ts_db,
                       ts_db, NULL, TRUE
                FROM stg_latest_users;
                """
            )
        logger.info("Full load completed: all staging records inserted into dimension table.")

    
    def update_existing(self, conn: psycopg.Connection) -> None:
        """Update existing dimension records as non-current if changes are detected."""
        
        with conn.cursor() as curr:
            curr.execute(
                """
                UPDATE scd2_dimensional_users AS sdu
                SET is_actual = FALSE,
                    actual_to = slu.ts_db
                FROM stg_latest_users AS slu
                WHERE sdu.user_id = slu.user_id
                AND sdu.is_actual = TRUE
                AND (sdu.first_name != slu.first_name
                     OR sdu.last_name != slu.last_name
                     OR sdu.email != slu.email);
                """
            )
        logger.info("Existing records updated (is_actual set to FALSE where changes detected).")

    
    def insert_changed(self, conn: psycopg.Connection) -> None:
        """Insert changed records into dimension table as new current versions."""
        
        with conn.cursor() as curr:
            curr.execute(
                """
                INSERT INTO scd2_dimensional_users (
                    user_id, first_name, last_name, birthday,
                    email, created_at, updated_at, ts_db,
                    actual_from, actual_to, is_actual
                )
                SELECT slu.user_id, slu.first_name, slu.last_name, slu.birthday,
                       slu.email, slu.created_at, slu.updated_at, slu.ts_db,
                       slu.ts_db, NULL, TRUE
                FROM stg_latest_users AS slu
                WHERE EXISTS (
                    SELECT 1
                    FROM scd2_dimensional_users AS sdu
                    WHERE sdu.user_id = slu.user_id
                    AND sdu.actual_to = slu.ts_db
                );
                """
            )
        logger.info("Changed records inserted as new current versions.")
    
    def insert_new(self, conn: psycopg.Connection) -> None:
        """Insert new records into dimension table if they do not exist yet."""
        
        with conn.cursor() as curr:
            curr.execute(
                """
                INSERT INTO scd2_dimensional_users (
                    user_id, first_name, last_name, birthday,
                    email, created_at, updated_at, ts_db,
                    actual_from, actual_to, is_actual
                )
                SELECT slu.user_id, slu.first_name, slu.last_name, slu.birthday,
                       slu.email, slu.created_at, slu.updated_at, slu.ts_db,
                       slu.ts_db, NULL, TRUE
                FROM stg_latest_users AS slu
                LEFT JOIN scd2_dimensional_users AS sdu
                       ON sdu.user_id = slu.user_id
                WHERE sdu.user_id IS NULL;
                """
            )
        logger.info("New records inserted into dimension table.")
    
    def apply_scd2_changes(self, conn: psycopg.Connection) -> None:
        """
        Apply the full SCD2 logic in one transaction:
            1. Update existing records as non-current
            2. Insert changed records as new current
            3. Insert entirely new records
        """
        
        self.update_existing(conn)
        self.insert_changed(conn)
        self.insert_new(conn)
        logger.info("SCD2 changes applied successfully.")

    def run_pipeline(self, sleep_seconds: int = 120) -> None:
        """
        Run the SCD2 pipeline continuously.

        Args:
            sleep_seconds (int): Sleep interval between batches in seconds.
        """
        
        logger.info("Starting SCD2 pipeline.")
        try:
            while True:
                with psycopg.connect(**self.connection_args) as conn:
                    logger.info("New pipeline iteration started.")

                    # Create staging table
                    self.create_last_records_stg(conn)

                    # Check if staging has any records
                    with conn.cursor() as curr:
                        curr.execute("SELECT COUNT(*) FROM stg_latest_users;")
                        new_rows = curr.fetchone()[0]

                    if new_rows == 0:
                        logger.info("No new raw records found. Skipping this batch.")
                    else:
                        logger.info(f"{new_rows} new/updated raw records found.")
                        if self.check_if_table_is_empty(conn):
                            logger.info("Dimension table empty. Performing full load.")
                            self.full_load(conn)
                        else:
                            logger.info("Performing incremental SCD2 load.")
                            self.apply_scd2_changes(conn)

                    logger.info(f"Iteration completed. Sleeping for {sleep_seconds} seconds.")

                time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Pipeline stopped gracefully.")


def main() -> None:
    
    db_config = {
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }
    
    pipeline = SCD2Pipeline(**db_config)
    
    # Run pipeline
    pipeline.run_pipeline(
        sleep_seconds=int(os.getenv("SLEEP_SECONDS", "40"))
    )


if __name__ == '__main__':
    main()
    