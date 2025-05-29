import pg8000
import json
import boto3
import os
import time

from src.extract.utils import *
from datetime import datetime
# from pprint import pprint


def lambda_extract(events, context):
    db = create_conn()
    table_names = ["address", "counterparty", "currency", "department", 
                   "design", "payment", "payment_type", "purchase_order", 
                   "staff", "transaction"]
    try:
        with open("tempdata/timestamps.json","r", encoding="utf-8") as last_extraction:
            last_timestamp_dict = json.load(last_extraction)
    except FileNotFoundError:
        last_timestamp_dict = {}
    new_timestamp_dict = {}
    extract_client = boto3.client('s3')
    bucket_name = get_bucket_name() 
    for table_name in table_names:
        last_extract = last_timestamp_dict.get(table_name, None)
        new_dict_list, extract_time = get_data(db, table_name, last_extract)
        new_timestamp_dict[table_name]= extract_time
        save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time)

    db.close()
    with open("tempdata/timestamps.json", "w", encoding="utf-8") as f:
        json.dump(new_timestamp_dict, f, default=serialise_object, indent=2)
    return {'message':'completed ingestion', 'timestamp':new_timestamp_dict['transaction']}
    

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

def save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time):
    if len(new_dict_list)==0:
        return
    date, time = extract_time.split()
    key = f"dev/{table_name}/{date}/{table_name}_{time}.json"    
    extract_client.put_object(Bucket=bucket_name, Body=json.dumps(new_dict_list, default=serialise_object, indent=2), 
            Key=key)

def save_to_tempdata(dict_list, extract_time, table_name):
    if len(dict_list)==0:
        return
    date, time = extract_time.split()
    path = f"tempdata/{table_name}/{date}/{table_name}_{time}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dict_list, f, default=serialise_object, indent=2)

if __name__ == "__main__":
   print(lambda_extract(None, None))
   time.sleep(15)
   print(lambda_extract(None, None))



