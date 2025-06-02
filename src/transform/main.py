import boto3 
import pandas as pd 
# import duckdb 
from datetime import datetime
from io import StringIO
import json
import os
import dotenv # for local implementation

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
    for new_key in events["new_keys"]:
        new_df = append_json_raw_tables(s3_client, ingestion_bucket, new_key, processed_bucket)
        print(new_df)
    #     response = s3_client.get_object(
    #         Bucket="team-08-ingestion-20250528081548341900000001", 
    #         Key=file
    #     )
    #     body = response["Body"].read().decode("utf-8")
    #     df_list.append(pd.read_json(StringIO(body)))
    # print(df_list)
    
    
    # date_time = str(datetime.now())

    # file_path_staff = "tempdata/staff/2025-05-28/staff_12:25:18.184743.json"
    # file_path_department = "tempdata/department/2025-05-28/department_12:25:18.164813.json"
    # file_path_address = "tempdata/address/2025-05-28/address_15:44:01.469582.json"

    # staff_df = pd.read_json(file_path_staff)
    # department_df = pd.read_json(file_path_department)
    # address_df = pd.read_json(file_path_address)

    
    # #method 1 - creates dim_staff table using SQL query/duckdb
    # dim_staff = duckdb.sql("SELECT staff_df.first_name, staff_df.last_name, department_df.department_name, department_df.location, staff_df.email_address FROM staff_df JOIN department_df ON department_df.department_id = staff_df.department_id").df()
    
    # #method 2 creates dim_staff table using pandas 
    # dim_staff = pd.merge(staff_df, department_df, how= "inner", on= "department_id")
    # dim_staff = dim_staff.loc[:,["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]

    # dim_location = address_df.loc[:,["address_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]]
    
    # #converts the dataframe to a parquet file and saves to the specified file path
    # dim_staff.to_parquet(f"tempdata/dim_staff/{date_time}.parquet")
    # dim_location.to_parquet(f"tempdata/dim_location/{date_time}.parquet")
    # #####

    # dim_location.to_csv(f"tempdata/dim_location/{date_time}.txt")

    # #reads parquet file to check the contents are correct
    # df=pd.read_parquet(f"tempdata/dim_location/{date_time}.parquet") 
    # print(df)

    
    
    
# how to name the df correctly
# what happens if we dont have enough data to complete a table from the df updated - use old df? 

def mvp_transform_to_parquet():
   table_names = {"address":["dim_location"], "counterparty":["dim_counterparty"], 
                  "currency":["dim_currency"], "department":["dim_staff"], 
                   "design":["dim_design"], 
                    "sales_order":["fact_sales_order", "dim_date"],
                   "staff":["dim_staff"], 
}
   
def append_json_raw_tables(s3_client, ingestion_bucket, new_json_key, processed_bucket):
        table_name = new_json_key.split('/')[1]
        main_json_key = f"raw_data/{table_name}_all.json"
        try:
            main_json = s3_client.get_object(Bucket=processed_bucket, Key =main_json_key)
            main_df =  pd.read_json(StringIO(main_json["Body"].read().decode("utf-8")))
        except s3_client.exceptions.NoSuchKey:
            main_df = pd.DataFrame()

        new_json = s3_client.get_object(Bucket=ingestion_bucket, Key=new_json_key)
        new_df = pd.read_json(StringIO(new_json["Body"].read().decode("utf-8")))

        merged_df = pd.concat([main_df, new_df], ignore_index=True)

        s3_client.put_object(Bucket=processed_bucket, Body=merged_df.to_json(merged_df, indent=2), 
            Key=main_json_key)
        
        return new_df


        




        


def backlog_transform_to_parquet():
    additional_tables = {"payment":["fact_sales_order", "dim_date"], 
                    "payment_type":["dim_date"], 
                    "purchase_order":["fact_sales_order","dim_date"], 
                    "transaction":["fact_sales_order", "dim_date"]}

    

lambda_transform({'message':'completed ingestion', 'timestamp':None,
            'total_new_files':2, 'new_keys':["dev/payment/2025-05-30/payment_14:54:53.938730.json", "dev/purchase_order/2025-05-30/purchase_order_14:54:54.041603.json", "dev/transaction/2025-05-30/transaction_14:54:54.101691.json"]}, None)