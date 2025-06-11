import pandas as pd
import awswrangler as wr
from pg8000.native import Connection
from pg8000.exceptions import DatabaseError
from datetime import datetime,UTC
import boto3
import os
import json
from pprint import pprint # for local viewing
import dotenv #local implementation

def lambda_load(events, context):
    """
    AWS Lambda entry point for uploading stored parquet files to Data Warehouse

    Args:
        events (dict): 
            - 'message': Status message
            - 'timestamp': Completion time
            - 'total_new_files': Number of transformed files
            - 'new_keys': List of new Parquet file keys

        context (None): AWS Lambda context Object

    Returns:
        dict: 
            - 'message': Status message
            - 'timestamp': Completion time 
            - 'total_tables_updated': Number of updated tables
             - 'items_inserted_into_db': dict of tables(keys) and count of rows updated (value)
    """

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
    
def create_conn(s3_client):
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
    password = get_db_password(s3_client)
    return Connection(
        database=database, user=user, password=password, host=dbhost, port=dbport
    )

def get_db_password(s3_client):
    """
    Utility function, collects password credentials from S3 backend bucket

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket

    Returns:
        pw_dict['totesys'] (str): value from dict object
    """

    key = 'secrets/secrets.json'
    bucket = os.environ['BACKEND_S3']
    pw_file = s3_client.get_object(Bucket=bucket, Key=key)
    pw_dict = json.loads(pw_file["Body"].read().decode("utf-8"))
    return pw_dict['warehouse']

def parquet_to_df(file_key, processed_bucket):
    """
    Utility function that converts a parquet file stored in an S3 bucket
    to a Dataframe

    Args:
        file_key (str): Name of the file to load
        processed_bucket (str): S3 bucket name

    Returns:
        DataFrame Object: Pandas DataFrame object containing converted data
    """
    df = wr.s3.read_parquet(path=f's3://{processed_bucket}/{file_key}')
    # print(df.head(10))
    return df

def insert_df_into_warehouse(db, df, table_name):
<<<<<<< HEAD
    # print(df.head(10))
=======
    """
    Utility function to insert DataFrame rows into a relational database

    Args:
        db (pg8000 Object): pg8000.Native.Connection object
        df (DataFrame Object): Pandas DataFrame object containing data to be inserted
        table_name (str): DB table name to be queried

    Returns:
        _type_: _description_
    """
>>>>>>> main
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
    try:
        db.run(query)
    except DatabaseError as e:
        print(e)
        print("problem in",table_name)
        print(query[:300])
        return {table_name:e}

    return {table_name:len(df)}
    


# if __name__ == "__main__":
#    events = {
#         "message": "completed transformation",
#         "timestamp": "2025-06-10T14:34:05.599526",
#         "total_new_files": 7,
#         "new_keys": [
#             "dev/dim_location/2025-06-10/dim_location_14:33:43.791256.parquet",
#             "dev/dim_counterparty/2025-06-10/dim_counterparty_14:33:43.791256.parquet",
#             "dev/dim_currency/2025-06-10/dim_currency_14:33:43.791256.parquet",
#             "dev/dim_design/2025-06-10/dim_design_14:33:43.791256.parquet",
#             "dev/dim_date/2025-06-10/dim_date_14:33:43.791256.parquet",
#             "dev/dim_staff/2025-06-10/dim_staff_14:33:43.791256.parquet",
#             "dev/fact_sales_order/2025-06-10/fact_sales_order_14:33:43.791256.parquet"
#         ]
#         }
#    print(lambda_load(events,None))