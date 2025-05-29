import pg8000
import json
# import boto3
import os

# from pg8000.native import literal
from src.extract.utils import *
from datetime import datetime
from pprint import pprint


def lambda_extract(events, context):
    db = create_conn()
    table_names = ["address", "counterparty", "currency", "department", 
                   "design", "payment", "payment_type", "purchase_order", 
                   "staff", "transaction"]
    last_timestamp_dict = events
    new_timestamp_dict = {}
    for table_name in table_names:
        if not events:
            last_extract = None
        else:
            last_extract = last_timestamp_dict[table_name]
        new_dict_list, extract_time = get_data(db, table_name, last_extract)
        new_timestamp_dict[table_name]= extract_time
        try:
            os.mkdir(f"tempdata/{table_name}")
            os.mkdir(f"tempdata/{table_name}/2025-05-28")
        except:
            pass
        save_to_tempdata(new_dict_list, extract_time, table_name)
    db.close()
    return new_timestamp_dict

def get_data(db, table_name, last_extract=None):
    query = f"SELECT * FROM {table_name}"
    if last_extract:
        query+= f" WHERE last_updated > '{last_extract}'"     
    tab_data = db.run(query)
    extract_time = str(datetime.now())
    keys = [column["name"] for column in db.columns]
    new_dict_list = [
        {keys[i]: single_data[i] for i in range(len(keys))} for single_data in tab_data
    ]
    return new_dict_list, extract_time

def save_to_tempdata(dict_list, extract_time, table_name):
    date, time = extract_time.split()
    path = f"tempdata/{table_name}/{date}/{table_name}_{time}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dict_list, f, default=serialise_object, indent=2)

if __name__ == "__main__":
    # db = create_conn()
    # pprint(get_data(db, "transaction", "2025-05-27"))
   new_timestamp_dict=lambda_extract(None, None)
   new_timestamp_dict2=lambda_extract(new_timestamp_dict, None)



