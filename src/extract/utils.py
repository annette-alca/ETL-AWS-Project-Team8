from pg8000.native import Connection
import dotenv
import os
from datetime import datetime
from decimal import Decimal

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


def serialise_object(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Type not serialisable")


