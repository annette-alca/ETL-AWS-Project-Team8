import pg8000
import json
import boto3
import os
# from pg8000.native import literal
from src.extract.utils import *
from datetime import datetime
from pprint import pprint

def get_all_data(db, table_name):
    tab_data = db.run(f"SELECT * FROM {table_name}")
    extract_time = datetime.now()
    keys = [column["name"] for column in db.columns]
    new_dict_list = [{keys[i]:single_data[i] for i in range(len(keys))} for single_data in tab_data]
    return new_dict_list, extract_time

def get_updated_data(db, table_name, last_extract):
    tab_data = db.run(f"""SELECT * FROM {table_name}
                   WHERE last_updated > {last_extract};""")
    extract_time = datetime.now()
    keys = [column["name"] for column in db.columns]
    new_dict_list = [{keys[i]:single_data[i] for i in range(len(keys))} for single_data in tab_data]
    return new_dict_list, extract_time

def save_to_tempdata(dict_list, extract_time, table_name):
    date, time = str(extract_time).split()
    path = f"tempdata/{table_name}/{date}/{table_name}_{time}.json"
    with open(path,"w",encoding="utf-8") as f:
        json.dump(dict_list,f, default= serialise_datetime, indent=2)


if __name__ == "__main__":
    db = create_conn()
    for table_name in ['department','staff','counterparty','transaction']:
        try:
            os.mkdir(f'tempdata/{table_name}')
            os.mkdir(f'tempdata/{table_name}/2025-05-28')
        except:
            pass
        tab_data, extract_time  = get_all_data(db,table_name)
        save_to_tempdata(tab_data, extract_time, table_name)
   