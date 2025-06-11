import boto3
import pandas as pd
from datetime import datetime, UTC
import os
from decimal import Decimal
import awswrangler as wr
import json

import dotenv  # for local runs

def lambda_transform(events, context):
    """
    AWS Lambda entry point for transforming newly ingested JSON data files into processed Parquet files
    Args:
        events (dict):
            - 'timestamp': Event trigger time
            - 'total_new_files': Count of new files
            - 'new_keys': List of raw JSON file keys
        context(None): AWS Lambda context object

    Returns:
        dict:
            - 'message': Status message
            - 'timestamp': Completion time
            - 'total_new_files': Number of transformed files
            - 'new_keys': List of new Parquet file keys
    """

    if events["total_new_files"] == 0:
        return {
            "message": "completed transformation",
            "timestamp": events["timestamp"],
            "total_new_files": 0,
            "new_keys": [],
        }

    dotenv.load_dotenv()  ## for local implementation
    s3_client = boto3.client("s3")
    ingestion_bucket = os.environ["INGESTION_S3"]
    processed_bucket = os.environ["PROCESSED_S3"]
    list_of_transformed_keys = []
    for new_key in events["new_keys"]:
        table_name, new_df = append_json_raw_tables(
            s3_client, ingestion_bucket, new_key, processed_bucket
        )
        transformed_dict = mvp_transform_df(
            s3_client, table_name, new_df, processed_bucket
        )
        parquet_key_list = save_parquet_to_s3(processed_bucket, transformed_dict, events["timestamp"])
        if len(parquet_key_list):
            list_of_transformed_keys += parquet_key_list
        
    timestamp = datetime.now(UTC).isoformat()
    timestamp = timestamp.replace("+00:00", "")
    return {
        "message": "completed transformation",
        "timestamp": timestamp,
        "total_new_files": len(list_of_transformed_keys),
        "new_keys": list_of_transformed_keys,
    }

def table_name_to_df(s3_client, table_name, bucket_name):
    """
    Loads a table's full historical JSON data from S3 into a pandas DataFrame

    Args:
        s3_client (Object): Boto3 S3 client for accessing the bucket
        table_name (str): Name of the table to load
        bucket_name (str): S3 bucket containing the db_state JSON files

    Returns:
        pandas.DataFrame: DataFrame containing the table's full data
    """
        
    """key is generated in next line"""
    key = f"db_state/{table_name}_all.json"
    new_object = s3_client.get_object(Bucket=bucket_name, Key=key)
    new_json = json.loads(new_object["Body"].read().decode("utf-8"))
    new_df = pd.DataFrame.from_dict(data=new_json, orient='columns')

    return new_df


def save_parquet_to_s3(bucket_name, transformed_dict, extract_time):
    """
    Saves transformed DataFrames as Parquet files to an S3 bucket with timestamped keys

    Args:
        bucket_name (str): Target S3 bucket for storing the Parquet files
        transformed_dict (dict): Dictionary of table names to pandas DataFrames
        extract_time (str): timestamp string used to structure S3 keys

    Returns:
        list: List of S3 keys for the saved Parquet files
    """

    key_list =[]
    if not transformed_dict: #None from mvp_transform because table is part of backlog
        return key_list
    for transform_name, new_df in transformed_dict.items():
        date, time = extract_time.split("T")
        key = f"dev/{transform_name}/{date}/{transform_name}_{time}.parquet"

        wr.s3.to_parquet(
            df=new_df,
            path=f"s3://{bucket_name}/{key}",
            dataset=False
        )
        key_list.append(key)
    return key_list


def mvp_transform_df(s3_client, table_name, new_df, processed_bucket):
    """
    Applies table-specific transformation logic to raw DataFrame
    Depending on the table name, this function merges the new data with reference data when required

    Args:
        s3_client (Object): S3 client for accessing reference and state files
        table_name (str): Name of the table being transformed
        new_df (pandas.DataFrame): Raw ingested data to transform
        processed_bucket (str): S3 bucket containing historical processed data

    Returns:
        dict: Dictionary of transformed DataFrames keyed by their target table names 
    """

    match table_name:
        case "staff":
            department_df = table_name_to_df(s3_client, "department", processed_bucket)
            dim_staff = pd.merge(new_df, department_df, how="left", on="department_id")
            dim_staff = dim_staff.loc[
                :,
                [
                    "staff_id",
                    "first_name",
                    "last_name",
                    "department_name",
                    "location",
                    "email_address",
                ],
            ]
            return {"dim_staff": dim_staff}
        case "address":
            address_df = table_name_to_df(s3_client, "address", processed_bucket)
            dim_location = address_df.loc[
                :,
                [
                    "address_id",
                    "address_line_1",
                    "address_line_2",
                    "district",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ],
            ]
            dim_location.rename(columns={"address_id":"location_id"},inplace=True)
            return {"dim_location": dim_location}
        
        case "counterparty":
            address_df = table_name_to_df(s3_client, "address", processed_bucket)
            dim_counterparty = pd.merge(
                new_df,
                address_df,
                how="left",
                left_on="legal_address_id",
                right_on="address_id",
            )
            dim_counterparty = dim_counterparty.loc[
                :,
                [
                    "counterparty_id",
                    "counterparty_legal_name",
                    "address_line_1",
                    "address_line_2",
                    "district",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ],
            ]
            dim_counterparty.columns = [
                "counterparty_id",
                "counterparty_legal_name",
                "counterparty_legal_address_line_1",
                "counterparty_legal_address_line_2",
                "counterparty_legal_district",
                "counterparty_legal_city",
                "counterparty_legal_postal_code",
                "counterparty_legal_country",
                "counterparty_legal_phone_number",
            ]
            return {"dim_counterparty": dim_counterparty}

        case "design":
            dim_design = new_df.loc[
                :, ["design_id", "design_name", "file_location", "file_name"]
            ]
            dim_design.sort_values(by="design_id", inplace=True)
            return {"dim_design": dim_design}
        
        case "currency":
            currency_dict = {
                "GBP": "British pound",
                "USD": "US dollar",
                "EUR": "Euro",
                "CHF": "Swiss franc",
            }
            for row in range(len(new_df)):
                new_df.loc[row, "currency_name"] = currency_dict[
                    new_df.loc[row, "currency_code"]
                ]
            dim_currency = new_df.loc[:, ["currency_id", "currency_code", "currency_name"]]
            return {"dim_currency": dim_currency}

        case "sales_order":
            for date_col in [
                "created_at",
                "agreed_delivery_date",
                "agreed_payment_date",
            ]:
                new_df[date_col] = new_df[date_col].astype(str)

            new_df[["created_date", "created_time"]] = new_df["created_at"].str.split(
                "T", n=1, expand=True
            )
        
            new_df[["last_updated_date", "last_updated_time"]] = new_df[
                "last_updated"
            ].str.split("T", n=1, expand=True)


            fact_sales_order = new_df.loc[
                :,
                [
                    "sales_order_id",
                    "created_date",
                    "created_time",
                    "last_updated_date",
                    "last_updated_time",
                    "staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "design_id",
                    "agreed_payment_date",
                    "agreed_delivery_date",
                    "agreed_delivery_location_id",
                ],
            ]
            fact_sales_order.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

            # generating dim_date
            new_dates = pd.concat(
                [
                    new_df["created_date"],
                    new_df["last_updated_date"],
                    new_df["agreed_delivery_date"],
                    new_df["agreed_payment_date"],
                ],
                ignore_index=True,
            )
            #below merging with dbstate Series main_dates
            try:
                new_object = s3_client.get_object(Bucket=processed_bucket, Key="db_state/date_all.json")
                dates_dict = json.loads(new_object["Body"].read().decode("utf-8"))
                main_dates = dates_dict.values()

                main_set = set(main_dates)
                new_set = set(new_dates)
                unique_new_set = new_set.difference(main_set)

                if len(unique_new_set) == 0: #no nwq unique dates, just return sales data
                    return {"fact_sales_order": fact_sales_order}
                
                unique_new_dates = pd.Series(list(unique_new_set))
                merged_set = main_set.union(new_set)
                merged_dates = pd.Series(list(merged_set))
            
            except s3_client.exceptions.NoSuchKey:
                unique_new_dates = new_dates.drop_duplicates(ignore_index=True)
                merged_dates = unique_new_dates.copy()

            wr.s3.to_json(merged_dates, path=f"s3://{processed_bucket}/db_state/date_all.json")

            dim_date = pd.DataFrame()
            dim_date["date_str"] = unique_new_dates
            dim_date["date_id"] = [
                datetime.strptime(date, "%Y-%m-%d") for date in unique_new_dates
            ]

            dim_date["year"] = [d.year for d in dim_date["date_id"]]
            dim_date["month"] = [d.month for d in dim_date["date_id"]]
            dim_date["day"] = [d.day for d in dim_date["date_id"]]

            dim_date.drop(columns=["date_str"], inplace=True)
            dim_date["day_of_week"] = [d.weekday() for d in dim_date["date_id"]]
            dim_date["day_name"] = [d.day_name() for d in dim_date["date_id"]]
            dim_date["month_name"] = [d.month_name() for d in dim_date["date_id"]]
            dim_date["quarter"] = [d.quarter for d in dim_date["date_id"]]
            
            return {"dim_date": dim_date, "fact_sales_order": fact_sales_order}


def append_json_raw_tables(s3_client, ingestion_bucket, new_json_key, processed_bucket):
    """
    Appends new ingested JSON data to existing processed data and updates the db_state in S3

    Args:
        s3_client (Object): S3 client for fetching and writing objects
        ingestion_bucket (str): Name of the S3 bucket containing new raw data
        new_json_key (str): Key of the new JSON file in the ingestion bucket
        processed_bucket (str): Name of the S3 bucket to update with merged data

    Returns:
        tuple: (table_name, new_df)
            - table_name (str): Extracted table name from the S3 key
            - new_df (pandas.DataFrame): DataFrame created from the new JSON file
    """

    print(f'json_s3: {s3_client}')
    table_name = new_json_key.split("/")[1]

    main_json_key_overwritten = f"db_state/{table_name}_all.json"
    new_object = s3_client.get_object(Bucket=ingestion_bucket, Key=new_json_key)
    new_json = json.loads(new_object["Body"].read().decode("utf-8"))
    new_df = pd.DataFrame.from_dict(data=new_json, orient='columns')

    try:
        main_df = table_name_to_df(s3_client, table_name, processed_bucket)
        merged_df = pd.concat([main_df, new_df], ignore_index=True)
    except s3_client.exceptions.NoSuchKey:
        merged_df = new_df.copy()

    df_dict = merged_df.T.to_dict()
    df_list = list(df_dict.values())
    
    s3_client.put_object(Bucket=processed_bucket, Body=json.dumps(df_list, default=serialise_object, indent=2), 
            Key=main_json_key_overwritten)
  
    return (table_name, new_df)


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
#     events = {
#   "message": "completed ingestion",
#   "timestamp": "2025-06-05T10:45:00.5",
#   "total_new_files": 4,
#   "new_keys": []
# }
#     pprint(lambda_transform(events, None))




