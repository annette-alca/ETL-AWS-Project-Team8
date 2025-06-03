import boto3 
import pandas as pd 
# import duckdb 
from datetime import datetime
from io import StringIO
import json
import os
import dotenv # for local implementation
from decimal import Decimal

def lambda_transform(events, context):
    # extracting data from s3 and converting to df

    if events["total_new_files"] == 0:
        return {'message':'completed transformation', 'timestamp':None,
            'total_new_files':0, 'new_keys':[]}
    
    dotenv.load_dotenv() #for local implementation
    s3_client = boto3.client("s3")
    df_list = []
    ingestion_bucket = os.environ["INGESTION_S3"]
    processed_bucket = os.environ["PROCESSED_S3"]
    list_of_keys = []
    for new_key in events["new_keys"]:
        table_name, new_df = append_json_raw_tables(s3_client, ingestion_bucket, new_key, processed_bucket, events["timestamp"])
        transformed_dict = mvp_transform_df(s3_client, table_name, new_df, processed_bucket)
        list_of_keys.append(save_to_s3(processed_bucket, transformed_dict, events["timestamp"]))

def key_to_df(s3_client, table_name, bucket_name):
    new_json = s3_client.get_object(Bucket=bucket_name, Key=f"raw_data/{table_name}_all.json")
    new_df = pd.read_json(StringIO(new_json["Body"].read().decode("utf-8")), orient="index")
    new_df.to_csv("tempdata/dim_staff_test.csv")
    return new_df

def save_to_s3(bucket_name, transformed_dict, extract_time):
    # need to change doc string
    """
    Utility function, stores a parquet file in an S3 bucket.  Object key is derived from table_name and extract_time arguments

    Args:
        extract_client (Object): a boto3 client object to query the S3 bucket
        bucket_name (Object): an S3 bucket name where the JSON object is stored_
        new_dict_list (_type_): _description_
        table_name (str): DB table name to be queried
        extract_time (str) : UTC timestamp of query

    Returns:
        key (str): S3 Object key derived from table_name and extract_time
    """
    for transform_name, new_df in transformed_dict.items():
        date, time = extract_time.split('T')
        key = f"dev/{transform_name}/{date}/{transform_name}_{time}.json"
        new_df.to_parquet(f"s3://{bucket_name}/dev/{transform_name}/{date}/{transform_name}_{time}.parquet")
    return key

def mvp_transform_df(s3_client, table_name, new_df, processed_bucket):
    match table_name:
        case "staff":
            department_df = key_to_df(s3_client, "department", processed_bucket)
            dim_staff = pd.merge(new_df, department_df, how= "left", on= "department_id")
            dim_staff = dim_staff.loc[:,["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]
            return {"dim_staff": dim_staff}
        case "address":
            address_df = key_to_df(s3_client, "address", processed_bucket)
            dim_location = address_df.loc[:,["address_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]]
            return {"dim_location": dim_location}
        case "counterparty":
            address_df = key_to_df(s3_client, "address", processed_bucket)
            dim_counterparty = pd.merge(new_df, address_df, how= "left", left_on="legal_address_id", right_on="address_id")
            dim_counterparty = dim_counterparty.loc[:, [
                "counterparty_id", "counterparty_legal_name", "address_line_1", 
                "address_line_2", "district", "city", "postal_code", "country", "phone"
                ]
            ]
            dim_counterparty.columns = [
                "counterparty_id", "counterparty_legal_name", "counterparty_legal_address_line_1", 
                "counterparty_legal_address_line_2", "counterparty_legal_district", "counterparty_legal_city", 
                "counterparty_legal_postal_code", "counterparty_legal_country", "counterparty_legal_phone_number"
            ]
            return {"dim_counterparty": dim_counterparty}
        case "design":
            dim_design = new_df.loc[:,["design_id", "design_name", "file_location", "file_name"]]
            return {"dim_design": dim_design}
        case "currency":
            dim_currency = new_df.loc[:,["currency_id", "currency_code"]]
            currency_dict = {"GBP":"British pound", "USD": "US dollar", "EUR":"Euro","CHF":"Swiss franc"}
            for row in range(len(new_df)):
                new_df.loc[row,"currency_name"] = currency_dict[new_df.loc[row,"currency_code"]]
            return {"dim_currency": dim_currency}
        
        case "sales_order":
            for col in ['created_at','last_updated']:
                new_df[col] = new_df[col].astype(str)
            new_df[['created_date','created_time']] = new_df['created_at'].str.split(" ", expand=True)
            new_df[['last_updated_date','last_updated_time']] = new_df['last_updated'].str.split("T", expand=True)

            fact_sales_order = new_df.loc[:,["sales_order_id", "created_date", "created_time",
                            "last_updated_date", "last_updated_time", "staff_id",
                            "counterparty_id", "units_sold", "unit_price", "currency_id",
                            "design_id", "agreed_payment_date", "agreed_delivery_date",
                            "agreed_delivery_location_id"]]
            fact_sales_order.replace({'staff_id':'sales_staff_id'})
            print(fact_sales_order)

            dates = pd.concat([new_df['created_date'], new_df['last_updated_date'], 
                    new_df['agreed_delivery_date'], new_df['agreed_payment_date']], ignore_index=True)
            dim_date = pd.DataFrame(columns=['date_id','year','month','date'])
            dim_date['date_id'] = dates
            dim_date.drop_duplicates(inplace=True, ignore_index=True)
            dim_date[['year','month','date']] = dim_date['date_id'].str.split("-", expand=True)
            #still need to add month and day of the week. there is a weekday method in datetime
            print(dim_date)
            
            return {"fact_sales_order": fact_sales_order, "dim_date":dim_date}

def append_json_raw_tables(s3_client, ingestion_bucket, new_json_key, processed_bucket):
        table_name = new_json_key.split('/')[1]
        main_json_key_overwritten = f"db_state/{table_name}_all.json"
        
        new_json = s3_client.get_object(Bucket=ingestion_bucket, Key=new_json_key)
        new_df = pd.read_json(StringIO(new_json["Body"].read().decode("utf-8")))

        try:
            main_json = s3_client.get_object(Bucket=processed_bucket, Key = main_json_key_overwritten)
            main_df =  pd.read_json(StringIO(main_json["Body"].read().decode("utf-8")))
            merged_df = pd.concat([main_df, new_df], ignore_index=True)
        except s3_client.exceptions.NoSuchKey:
            merged_df = new_df.copy()

        json_buffer = StringIO()

        merged_df.to_json(json_buffer, indent=2, orient="index", default_handler=serialise_object)
        
        s3_client.put_object(Bucket=processed_bucket, Body=json_buffer.getvalue(), 
            Key=main_json_key_overwritten)
        
        return (table_name, new_df)

def backlog_transform_to_parquet():
    additional_tables = {"payment":["fact_sales_order", "dim_date"], 
                    "payment_type":["dim_date"], 
                    "purchase_order":["fact_sales_order","dim_date"], 
                    "transaction":["fact_sales_order", "dim_date"]}
    
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

    
events = {'message': 'completed ingestion', 'timestamp': '2025-06-02T16:12:23.510861', 
          'total_new_files': 1, 
          'new_keys': ['dev/sales_order/2025-06-02/sales_order_13:56:16.189960.json']}
lambda_transform(events, None)