import pg8000
from src.extract.connection import create_conn

def get_parks_data(db):
    parks = db.run("""SELECT * FROM parks;""")
    keys = ["park_id", "park_name", "year_opened", "annual_attendance"]

    park_dicts = [{keys[i]: park[i] for i in range(4)} for park in parks]

    return park_dicts


if __name__ == "__main__":
        db = create_conn()
        print(get_parks_data(db))