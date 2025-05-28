import pg8000
# from pg8000.native import literal
from src.extract.connection import create_conn
from pprint import pprint

def get_all_data(db, table):
    tab_data = db.run(f"SELECT * FROM {table} LIMIT 10;")
    keys = [column["name"] for column in db.columns]
    pprint(tab_data)
    new_dict_list = [{keys[i]:single_data[i] for i in range(len(keys))} for single_data in tab_data]
    return new_dict_list


if __name__ == "__main__":
    db = create_conn()
    for table in ['department','staff','counterparty','transaction']:
        pprint(get_all_data(db, table))
