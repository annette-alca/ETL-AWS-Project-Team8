import boto3 
import pandas as pd 
import duckdb 
from datetime import datetime




def dim_staff():
    date_time = str(datetime.now())

    file_path_staff = "tempdata/staff/2025-05-28/staff_12:25:18.184743.json"
    file_path_department = "tempdata/department/2025-05-28/department_12:25:18.164813.json"
    file_path_address = "tempdata/address/2025-05-28/address_15:44:01.469582.json"

    staff_df = pd.read_json(file_path_staff)
    department_df = pd.read_json(file_path_department)
    address_df = pd.read_json(file_path_address)


    
    #method 1 - creates dim_staff table using SQL query/duckdb
    dim_staff = duckdb.sql("SELECT staff_df.first_name, staff_df.last_name, department_df.department_name, department_df.location, staff_df.email_address FROM staff_df JOIN department_df ON department_df.department_id = staff_df.department_id").df()
    
    #method 2 creates dim_staff table using pandas 
    dim_staff = pd.merge(staff_df, department_df, how= "inner", on= "department_id")
    dim_staff = dim_staff.loc[:,["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]]

    dim_location = address_df.loc[:,["address_id", "address_line_1", "address_line_2", "district", "city", "postal_code", "country", "phone"]]
    
    #converts the dataframe to a parquet file and saves to the specified file path
    dim_staff.to_parquet(f"tempdata/dim_staff/{date_time}.parquet")
    dim_location.to_parquet(f"tempdata/dim_location/{date_time}.parquet")
    #####

    dim_location.to_csv(f"tempdata/dim_location/{date_time}.txt")

    #reads parquet file to check the contents are correct
    df=pd.read_parquet(f"tempdata/dim_location/{date_time}.parquet") 
    print(df)

    
    
    
# how should we create tables with multiple files?
# for input are we given files or do we get it ourselves from s3 bucket using boto3? 
# which files do we select from boto3
# one function for creating each table / one function to create all?

dim_staff()