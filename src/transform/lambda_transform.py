import boto3
import pandas as pd
from datetime import datetime, UTC
from io import StringIO
import os
from decimal import Decimal
import dotenv  # for local runs

def lambda_transform(events, context):
    # extracting data from s3 and converting to df
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
    """key is generated in next line"""
    new_json = s3_client.get_object(
        Bucket=bucket_name, Key=f"db_state/{table_name}_all.json"
    )
    new_df = pd.read_json(
        StringIO(new_json["Body"].read().decode("utf-8")), orient="index"
    )
    return new_df


def save_parquet_to_s3(bucket_name, transformed_dict, extract_time):
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
    key_list =[]
    if not transformed_dict: #None from mvp_transform because table is part of backlog
        return key_list
    for transform_name, new_df in transformed_dict.items():
        date, time = extract_time.split("T")
        key = f"dev/{transform_name}/{date}/{transform_name}_{time}.parquet"
        new_df.to_parquet(
            f"s3://{bucket_name}/{key}"
        )
        key_list.append(key)
    return key_list


def mvp_transform_df(s3_client, table_name, new_df, processed_bucket):
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
            return {"dim_design": dim_design}
        case "currency":
            dim_currency = new_df.loc[:, ["currency_id", "currency_code"]]
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
            return {"dim_currency": dim_currency}

        case "sales_order":
            for date_col in [
                "created_at",
                "agreed_delivery_date",
                "agreed_payment_date",
            ]:
                new_df[date_col] = new_df[date_col].astype(str)
            new_df[["created_date", "created_time"]] = new_df["created_at"].str.split(
                " ", n=1, expand=True
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
                main_dates = table_name_to_df(s3_client, "date", processed_bucket)
                main_set = set(main_dates[0])
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


            json_buffer = StringIO()
            merged_dates.to_json(
                json_buffer, indent=2, orient="index", default_handler=serialise_object
            )
            s3_client.put_object(
                Bucket=processed_bucket,
                Body=json_buffer.getvalue(),
                Key="db_state/date_all.json"
            )

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
            
            return {"fact_sales_order": fact_sales_order, "dim_date": dim_date}


def append_json_raw_tables(s3_client, ingestion_bucket, new_json_key, processed_bucket):
    table_name = new_json_key.split("/")[1]
    main_json_key_overwritten = f"db_state/{table_name}_all.json"
    new_json = s3_client.get_object(Bucket=ingestion_bucket, Key=new_json_key)
    new_df = pd.read_json(StringIO(new_json["Body"].read().decode("utf-8")))

    try:
        main_df = table_name_to_df(s3_client, table_name, processed_bucket)
        merged_df = pd.concat([main_df, new_df], ignore_index=True)
    except s3_client.exceptions.NoSuchKey:
        merged_df = new_df.copy()

    json_buffer = StringIO()

    merged_df.to_json(
        json_buffer, indent=2, orient="index", default_handler=serialise_object
    )

    s3_client.put_object(
        Bucket=processed_bucket,
        Body=json_buffer.getvalue(),
        Key=main_json_key_overwritten,
    )

    return (table_name, new_df)


def backlog_transform_to_parquet():
    additional_tables = {
        "payment": ["fact_sales_order", "dim_date"],
        "payment_type": ["dim_date"],
        "purchase_order": ["fact_sales_order", "dim_date"],
        "transaction": ["fact_sales_order", "dim_date"],
    }


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


if __name__ == "__main__":
    events = {
  "message": "completed ingestion",
  "timestamp": "2025-06-04T12:25:43.983758",
  "total_new_files": 3,
  "new_keys": ["dev/transaction/2025-06-04/transaction_10:04:38.826283.json",
               "dev/sales_order/2025-06-04/sales_order_09:44:39.566927.json",
               "dev/staff/2025-06-02/staff_11:12:22.779187.json"
                ]
}
    print(lambda_transform(events, None))


'''"dev/address/2025-05-30/address_11:24:04.260569.json",
                    "dev/counterparty/2025-05-29/counterparty_11:40:56.079206.json",
                    "dev/currency/2025-05-29/currency_11:40:56.166425.json",
                    "dev/department/2025-05-29/department_11:40:56.242970.json",
                    "dev/design/2025-05-29/design_11:40:56.408504.json",
                    "dev/payment/2025-05-29/payment_11:40:57.662024.json",
                    "dev/payment_type/2025-05-29/payment_type_11:41:04.860129.json",
                    "dev/sales_order/2025-06-02/sales_order_09:46:02.338097.json",
                    "dev/staff/2025-05-29/staff_11:41:07.526864.json",
                    "dev/transaction/2025-05-29/transaction_11:41:08.378130.json"'''



