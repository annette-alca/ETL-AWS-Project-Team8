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
    extract_client = boto3.client('s3')
    bucket_name = get_bucket_name() 
    table_names = ["address", "counterparty", "currency", "department", 
                   "design", "payment", "payment_type", "purchase_order", 
                   "staff", "transaction"]
    last_timestamp_dict, timestamp_key = get_last_timestamps(extract_client, bucket_name)
    new_timestamp_dict = {}
    for table_name in table_names:
        last_extract = last_timestamp_dict.get(table_name, None)
        new_dict_list, extract_time = get_data(db, table_name, last_extract)
        new_timestamp_dict[table_name]= extract_time
        save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time)

    db.close()
    extract_client.put_object(Bucket=bucket_name, Body=json.dumps(new_timestamp_dict, default=serialise_object, indent=2), 
            Key=timestamp_key)
    return {'message':'completed ingestion', 'timestamp':new_timestamp_dict['transaction']}
    
def get_last_timestamps(extract_client, bucket_name):
    key = "dev/extraction_times/timestamps.json"
    try:   
        body = extract_client.get_object(Bucket=bucket_name, Key=key)
        last_timestamp_dict = json.loads(body["Body"].read().decode("utf-8"))
    except extract_client.exceptions.NoSuchKey:
        last_timestamp_dict = {}
    return last_timestamp_dict, key

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


if __name__ == "__main__":
   print(lambda_extract(None, None))
   time.sleep(15)
   print(lambda_extract(None, None))



