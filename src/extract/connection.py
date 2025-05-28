from pg8000.native import Connection
import dotenv
import os

# Copied from de-theme-parks-seeded

# Create your create_conn function to return a database connection object    #


def create_conn():
    dotenv.load_dotenv()

    user = os.environ["DBUSER"]
    database = os.environ["DBNAME"]
    dbhost=os.environ["HOST"]
    dbport=os.environ["PORT"]
    password = os.environ["DBPASSWORD"]

    return Connection(
        database=database,
        user=user,
        password=password,
        host = dbhost,
        port = dbport
    )


# Create a close_db function that closes a passed database connection object #


def close_db(conn):
    conn.close()
