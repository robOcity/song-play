import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Creates the sparify database, if does not exist, and re-creates all tables. 

    All data already in the database will be lost when this script is run.
    """

    # TODO - drop and re-create database getting role and permissions from .env lookup
    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """Remove all tables and the data they hold from the database."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables readying them for the addition of new data."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
