import datetime
import logging
import os
import random
import time

import psycopg
from faker import Faker
from psycopg.rows import dict_row

from config import setup_logger
from models.person import Person

logger = logging.getLogger(__name__)


class FakeIngestData:
    """
    Class for generating fake users and working with the raw_users table in PostgreSQL.

    Methods:
        create_table()        - Creates the table if it does not exist
        generate_data()       - Generates a list of fake users
        add_new_data()        - Inserts generated users into the table
        update_data()         - Updates a random row by inserting a new version with modified fields
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
        
        logger.info("Initializing FakeIngestData connection")
        self.create_table()
    
    def create_table(self):
        """
        Creates the raw_users table if it does not exist.
        """
        
        logger.info('Checking if table exists...')
        
        table_name = 'raw_users'
                
        with psycopg.connect(**self.connection_args) as conn:
            with conn.cursor() as curr:
                
                # Check if table exists
                curr.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_name = '{}'
                    );
                    """.format(table_name)
                )
                
                status = curr.fetchone()[0]
                logger.debug(f"Table exists status: {status}")
            
                # Create table if not exists
                curr.execute(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        id SERIAL,
                        user_id UUID,
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL,
                        birthday DATE,
                        email TEXT NOT NULL,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        ts_db TIMESTAMP
                    );
                    """.format(table_name)
                )
        
        if not status:
            logger.info(f"Table '{table_name}' has been created.")
        else:
            logger.info(f"Table '{table_name}' already exists.")
    
    def generate_data(self, batch_size: int = 10000) -> list[Person]:
        """
        Generates a list of fake users.

        Args:
            batch_size (int): Number of users to generate
        """
        
        logger.info(f'Starting data generation for {batch_size} users...')
        person_list = [Person.generate_person() for _ in range(batch_size)]
        logger.info(f'Data generation completed: {len(person_list)} persons created')
        return person_list
    
    def add_new_data(self, person_list: list[Person]) -> None:
        """
        Inserts generated users into the raw_users table.
        """
        
        logger.info('Adding new data to the database...')
        
        with psycopg.connect(**self.connection_args) as conn:
            with conn.cursor() as curr:
                rows_to_insert = [list(person.to_dict().values()) for person in person_list]
                logger.debug(f'Prepared {len(rows_to_insert)} rows for insertion')
                
                curr.executemany(
                    """
                    INSERT INTO raw_users (first_name, last_name, birthday,
                                           email, created_at, updated_at, 
                                           user_id, ts_db)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, rows_to_insert
                )
        
                logger.info(f'Successfully inserted {len(rows_to_insert)} rows')
        
    def update_data(self) -> None:
        """
        Selects a random row and inserts an updated version with new values for some fields.
        """
        
        logger.info('Updating a random row in the database...')
        faker = Faker()
        fields_to_update = ['first_name', 'last_name', 'email']
        
        with psycopg.connect(**self.connection_args, row_factory=dict_row) as conn:
            with conn.cursor() as curr:
                logger.debug('Fetching a random row to update...')
                res = curr.execute(
                    """
                    SELECT user_id, first_name, last_name, birthday, email, created_at, 
                           updated_at, ts_db
                    FROM raw_users 
                    ORDER BY random() LIMIT 1
                    """
                    ).fetchone()
                
                if not res:
                    logger.warning("No rows found to update")
                    return

                # Randomly choose fields to update
                num_fields_to_update = random.randint(1, len(fields_to_update))
                selected_fields = random.sample(fields_to_update, num_fields_to_update)
                logger.debug(f'Fields selected for update: {selected_fields}')
                            
                for field in selected_fields:
                    res[field] = getattr(faker, field)()
                
                update_date = datetime.datetime.now()
                res['updated_at'] = update_date
                res['ts_db'] = update_date
                        
                # Insert the updated row
                curr.execute(
                    """
                    INSERT INTO raw_users (
                        user_id, first_name, last_name, birthday, email,
                        created_at, updated_at, ts_db
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    list(res.values())
                )
                
                logger.info(f"Inserted updated row for user_id {res['user_id']}")


def main() -> None:
    db_config = {
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }

    batch_size = int(os.getenv("BATCH_SIZE", "1000"))
    sleep_seconds = int(os.getenv("SLEEP_SECONDS", "30"))

    ingestion = FakeIngestData(**db_config)
    ingestion.update_data()
    while True:
        logger.info("Starting new ingestion cycle...")

        # Generate and insert new data
        persons = ingestion.generate_data(batch_size=batch_size)
        ingestion.add_new_data(persons)

        # Simulate updates
        updates_count = max(1, batch_size // 10)
        logger.info(f"Simulating {updates_count} updates...")
        
        for _ in range(updates_count):
            ingestion.update_data()

        logger.info(f"Cycle completed. Sleeping {sleep_seconds} seconds...")
        time.sleep(sleep_seconds)


if __name__ == '__main__':
    main()
        