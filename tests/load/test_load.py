from src.load.lambda_load import parquet_to_df, insert_df_into_warehouse, lambda_load
import pandas as pd
from moto import mock_aws
import pytest
import boto3
import os 
import dotenv
from pg8000.native import Connection


@pytest.fixture 
def aws_credentials():
    os.environ["aws_access_key_id"]="test"
    os.environ["aws_secret_access_key"]="test"
    os.environ["aws_session_token"]="test"
    os.environ["aws_security_token"]="test"
    os.environ["aws_region"]="eu-west-2"

@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws(aws_credentials):
        yield boto3.client('s3')

@pytest.fixture
def s3_client_bucket_with_parquet_file(s3_client):
    s3_client.create_bucket(
        Bucket= "processed_bucket", 
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2',
        }
    )
    s3_client.upload_file("tests/data/dim_design.parquet", "processed_bucket", "dev/dim_design.parquet")
    s3_client.upload_file("tests/data/fact_sales_order.parquet", "processed_bucket", "dev/fact_sales_order.parquet")
    return s3_client


class TestParquetToDFFunction:

    # read parquet file and convert to df function
    def test_parquet_to_df(self, s3_client_bucket_with_parquet_file):
        file_key = "dim_design.parquet"
        expected_df = pd.read_parquet(f"tests/data/{file_key}")

        result = parquet_to_df(f'dev/{file_key}', "processed_bucket")

        assert type(result) == pd.core.frame.DataFrame 
        assert all([result.columns[i] == expected_df.columns[i] for i in range(len(result.columns))])
        assert result.loc[0, "design_id"] == 8
        assert result.loc[0, "design_name"] == "Wooden"

        fact_file_key = "fact_sales_order.parquet"
        expected_fact_df = pd.read_parquet(f"tests/data/{fact_file_key}")
        fact_result = parquet_to_df(f'dev/{fact_file_key}', "processed_bucket")
        assert type(fact_result) == pd.core.frame.DataFrame 
        assert all([fact_result.columns[i] == expected_fact_df.columns[i] for i in range(len(fact_result.columns))])

    
# def test_view_location_dim():
#     expected_df = pd.read_parquet(f"tests/data/dim_location.parquet")
#     print(expected_df)
#     print(expected_df[["location_id","city","district"]])

# function to insert df into warehouse using pg8000 

@pytest.fixture(scope='class')
def test_db():
    dotenv.load_dotenv()
    user = os.environ["LOCALUSER"]
    database = os.environ["LOCALDB"]
    password = os.environ["LOCALPASSWORD"]

    test_db = Connection(database=database, user=user, password=password)
    test_db.run('DROP TABLE IF EXISTS dim_location;')
    test_db.run("""CREATE TABLE dim_location (
               location_id int, 
                address_line_1 varchar,
                address_line_2 varchar,
                district varchar,
                city varchar,
                postal_code varchar,
                country varchar,
                phone varchar);""")
    
    test_db.run('DROP TABLE IF EXISTS fact_sales_order;')
    test_db.run("""CREATE TABLE fact_sales_order (
                "sales_record_id" SERIAL primary key,
                "sales_order_id" int,
                "created_date" date,
                "created_time" time,
                "last_updated_date" date,
                "last_updated_time" time,
                "sales_staff_id" int,
                "counterparty_id" int,
                "units_sold" int,
                "unit_price" numeric(10, 2),
                "currency_id" int,
                "design_id" int,
                "agreed_payment_date" date,
                "agreed_delivery_date" date,
                "agreed_delivery_location_id" int);""")
    return test_db


class TestDFInsertToWarehouseFunction:

    def test_df_inserted_into_empty_warehouse_table(self, test_db):
        df = pd.read_parquet(f"tests/data/dim_location.parquet")
        insert_df_into_warehouse(test_db, df, "dim_location")
        result = test_db.run("SELECT * FROM dim_location;")
        column_names = [column["name"] for column in test_db.columns]
        assert column_names[0] == "location_id"
        assert result[0][0] == 1
        assert column_names[4] == "city"
        assert result[0][4] == "New Patienceburgh"
        assert result[1][4] == "Aliso Viejo"
        assert len(result) == 30

        fact_df = pd.read_parquet(f"tests/data/fact_sales_order.parquet")
        insert_df_into_warehouse(test_db, fact_df, "fact_sales_order")
        fact_result = test_db.run("SELECT * FROM fact_sales_order;")
        fact_column_names = [column["name"] for column in test_db.columns]
        assert fact_column_names[0] == "sales_record_id"
        assert fact_result[0][0] == 1
        assert fact_result[0][1] == 14549
        assert fact_column_names[11] == "design_id"
        assert fact_result[0][11] == 322
        assert len(fact_result) == 1


    def test_df_insert_into_warehouse_with_existing_data(self, test_db):
        # read test data
        fact_df = pd.read_parquet(f"tests/data/fact_sales_order.parquet")
        # modify 'units_sold' entry
        fact_df['units_sold'] = 200
        insert_df_into_warehouse(test_db, fact_df, "fact_sales_order")       

        result = test_db.run("Select * from fact_sales_order;")

        assert len(result) == 2
        assert result[0][1] == result[1][1]
        assert result[0][8] != result[1][8] and result[1][8] == 200

class TestLoadLambdaHandler:
    def test_lambda_load_integration(self, s3_client, s3_client_bucket_with_parquet_file, test_db, monkeypatch):
            print(s3_client.list_objects_v2(Bucket='processed_bucket'),'<<<< PRINT S3 ')
            monkeypatch.setenv("PROCESSED_S3", 'processed_bucket')
            monkeypatch.setattr("src.load.lambda_load.create_conn", lambda _: test_db)

            events = {
                "total_new_files": 1,
                "new_keys": ["dev/fact_sales_order"]
            }
            result = lambda_load(events, context=None)
            print (result)
