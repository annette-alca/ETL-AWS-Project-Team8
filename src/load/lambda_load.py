import pandas as pd
import awswrangler as wr
from pg8000.native import Connection
from datetime import datetime,UTC
import boto3
import os
import json
from pprint import pprint # for local viewing
import dotenv #local implementation

def lambda_load(events, context):
    if events["total_new_files"]==0:
        return {"message": "completed loading",
                "timestamp": events["timestamp"],
                "total_tables_updated":0,
                "items_inserted_into_db": [] }

    processed_bucket = os.environ["PROCESSED_S3"]
    s3_client = boto3.client('s3')
    items_inserted_into_db = []
    db = create_conn(s3_client)
    for file_key in events["new_keys"]:
        df = parquet_to_df(file_key, processed_bucket)
        table_name = file_key.split('/')[1]
        updated_table_dict = insert_df_into_warehouse(db, df, table_name)
        items_inserted_into_db.append(updated_table_dict)
    return {"message": "completed loading",
        "timestamp": datetime.now(UTC).isoformat()[:-6],
        "total_tables_updated":len(items_inserted_into_db),
        "items_inserted_into_db": items_inserted_into_db }
    
def create_conn(extract_client):
    """
    Utility function, creates a database connection based on the environmental variables.

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket

    Returns:
        Connection (Object): pg8000.native object with environment credentials
    """

    dotenv.load_dotenv()
    user = os.environ["DBUSER"]
    database = os.environ["DBNAME_WH"]
    dbhost = os.environ["HOST_WH"]
    dbport = os.environ["PORT"]
    password = get_db_password(extract_client)
    return Connection(
        database=database, user=user, password=password, host=dbhost, port=dbport
    )

def get_db_password(extract_client):
    """
    Utility function, collects password credentials from S3 backend bucket

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket

    Returns:
        pw_dict['totesys'] (str): value from dict object
    """

    key = 'secrets/secrets.json'
    bucket = os.environ['BACKEND_S3']
    pw_file = extract_client.get_object(Bucket=bucket, Key=key)
    pw_dict = json.loads(pw_file["Body"].read().decode("utf-8"))
    return pw_dict['warehouse']

def parquet_to_df(file_key, processed_bucket):
    df = wr.s3.read_parquet(path=f's3://{processed_bucket}/{file_key}')
    return df

def insert_df_into_warehouse(db, df, table_name):
    # print(df)
    query = f"INSERT INTO {table_name}"
    column_string = ', '.join(df.columns)
    query += f"({column_string}) VALUES"
    for row in range(len(df)):
        query += '('
        for item in df.loc[row,:]:
            if type(item) in [float,int]:
                query += str({item}) + ','
            elif type(item) == str:
                item = item.replace("'","''") #some addresses have '
                query += f"'{item}'" +','
            else: #for datetime
                query += f"'{item}'" + ','
        query = query[:-1] + '),'
    query = query[:-1] + ';'
    # print(query)
    db.run(query)
    return {table_name:len(df)}
    