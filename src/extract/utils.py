from pg8000.native import Connection
import dotenv
import os
from pg8000.native import literal
from datetime import datetime


def create_conn():
    dotenv.load_dotenv()

    user = os.environ["DBUSER"]
    database = os.environ["DBNAME"]
    dbhost = os.environ["HOST"]
    dbport = os.environ["PORT"]
    password = os.environ["DBPASSWORD"]

    return Connection(
        database=database, user=user, password=password, host=dbhost, port=dbport
    )


def get_updated_data(db, table, last_extract):
    tab_data = db.run(
        f"""SELECT * FROM {literal(table)}
                   WHERE last_updated > {literal(last_extract)};"""
    )
    keys = [column["name"] for column in db.columns]
    new_dict = {key: tab_data[key] for key in keys}

    return new_dict


def serialise_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serialisable")
