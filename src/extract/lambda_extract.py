import json
import boto3
from datetime import datetime, UTC
from pg8000.native import Connection
import os
from decimal import Decimal
# import dotenv # for local implementation
from pg8000.exceptions import DatabaseError

def lambda_extract(events, context):
    """ 
    Function that calls utility functions to extract data from DB.

    Args:
        events (None): Optional argument passed by AWS to trigger function - not used in this implementation
        context (None): Optional argument passed by AWS containing information about invocation,
                        function configuration or execution environment - not used in this implementation

    Returns:
        dict:   'message': <status message> ,
                'timestamp': <timestamp of last updated table>,
                'total_new_files': <count of recently updated tables>,
                'new_keys': <list containing keys of recently updated tables>

    """

    extract_client = boto3.client('s3')
    db = create_conn(extract_client)
    bucket_name = os.environ['INGESTION_S3']
    table_names = ["address", "counterparty", "currency", "department", 
                   "design", "payment", "payment_type", "purchase_order", 
                   "staff", "transaction"]
    last_timestamp_dict, timestamp_key = get_last_timestamps(extract_client, bucket_name)
    new_timestamp_dict = {}
    new_keys = []
    for table_name in table_names:
        last_extract = last_timestamp_dict.get(table_name, None)
        new_dict_list, extract_time = get_data(db, table_name, last_extract)
        new_timestamp_dict[table_name]= extract_time
        any_key = save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time)
        if any_key:
            new_keys.append(any_key)
    db.close()
    extract_client.put_object(Bucket=bucket_name, Body=json.dumps(new_timestamp_dict, default=serialise_object, indent=2), 
            Key=timestamp_key)
    return {'message':'completed ingestion', 'timestamp':new_timestamp_dict['transaction'],
            'total_new_files':len(new_keys), 'new_keys':new_keys}
    

def get_last_timestamps(extract_client, bucket_name):
    """
    Utility function, queries S3 bucket and returns dictionary containing keys and timestamps of latest DB updates

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket
        bucket_name (Object): an S3 bucket name where the JSON object of timestamps is stored

    Returns:
        last_timestamp_dict (dict): dictionary containing key-value pairs of tablenames (k) and timestamp updated (v)
        key (str): S3 object key reference
    """

    key = "dev/extraction_times/timestamps.json"
    try:   
        body = extract_client.get_object(Bucket=bucket_name, Key=key)
        last_timestamp_dict = json.loads(body["Body"].read().decode("utf-8"))
    except extract_client.exceptions.NoSuchKey:
        last_timestamp_dict = {}
    return last_timestamp_dict, key


def get_data(db, table_name, last_extract=None):
    """
    Utility function, queries DB tables and returns JSON of latest rows

    Args:
        db (pg8000 Object): pg8000.Native.Connection object
        table_name (str): DB table name to be queried
        last_extract (str, optional): Timestamp of last 'table_name' update check. Defaults to None.

    Returns:
        list: List of dictionaries containing table row data
        extract_time (str) : UTC timestamp of query
    """

    query = f"SELECT * FROM {table_name}"
    if last_extract:
        query+= f" WHERE last_updated > '{last_extract}'"  
    try:
        tab_data = db.run(query)
        extract_time = datetime.now(UTC).isoformat()
        extract_time = extract_time.replace('+00:00','')
        keys = [column["name"] for column in db.columns]
        new_dict_list = [
            {keys[i]: single_data[i] for i in range(len(keys))} for single_data in tab_data
        ]
        return new_dict_list, extract_time
    except DatabaseError:
        return [], last_extract
    

def save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time):

    date, time = extract_time.split('T')
    key = f"dev/{table_name}/{date}/{table_name}_{time}.json"
    print(key)    

    """
    Utility function, stores a JSON object in an S3 bucket.  Object key is derived from table_name and extract_time arguments

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket
        bucket_name (Object): an S3 bucket name where the JSON object is stored_
        new_dict_list (_type_): _description_
        table_name (str): DB table name to be queried
        extract_time (str) : UTC timestamp of query

    Returns:
        key (str): S3 Object key derived from table_name and extract_time
    """

    if len(new_dict_list)==0:
        return
    extract_client.put_object(Bucket=bucket_name, Body=json.dumps(new_dict_list, default=serialise_object, indent=2), 
            Key=key)
    return key

def create_conn(extract_client):

    # dotenv.load_dotenv()  # for local implementation

    """
    Utility function, creates a database connection based on the environmental variables.

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket

    Returns:
        Connection (Object): pg8000.native object with environment credentials
    """

    # dotenv.load_dotenv()
    user = os.environ["DBUSER"]
    database = os.environ["DBNAME"]
    dbhost = os.environ["HOST"]
    dbport = os.environ["PORT"]
    # password = os.environ["DBPASSWORD"]
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
    bucket = os.environ['BACKEND_S3'] # 'bucket-to-hold-tf-state-for-terraform'
    pw_file = extract_client.get_object(Bucket=bucket, Key=key)
    pw_dict = json.loads(pw_file["Body"].read().decode("utf-8"))
    return pw_dict['totesys']


def serialise_object(obj):
    """
    Utility function, specifies alternate serialisation methods or passes TypeErrors back to the base class

    Args:
        obj (datetime | Decimal): 

    Raises:
        TypeError: error raised if object Type is not string or decimal

    Returns:
        obj (timestamp | float) : type dependent on Arg type
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    raise TypeError("Type not serialisable")


# if __name__ == "__main__":
#    print(lambda_extract(None, None))
#    time.sleep(15)
#    print(lambda_extract(None, None))



