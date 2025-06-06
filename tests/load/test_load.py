from src.load.lambda_load import parquet_to_df, insert_df_into_warehouse
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
    s3_client.upload_file("tests/data/dim_design.parquet", "processed_bucket", "dim_design.parquet")
    return s3_client


# read parquet file and convert to df function
def test_parquet_to_df(s3_client_bucket_with_parquet_file):
    file_key = "dim_design.parquet"
    expected_df = pd.read_parquet(f"tests/data/{file_key}")

    result = parquet_to_df(file_key, "processed_bucket")

    assert type(result) == pd.core.frame.DataFrame 
    assert all([result.columns[i] == expected_df.columns[i] for i in range(len(result.columns))])
    assert result.loc[0, "design_id"] == 8
    assert result.loc[0, "design_name"] == "Wooden"

# def test_view_location_dim():
#     expected_df = pd.read_parquet(f"tests/data/dim_location.parquet")
#     print(expected_df)
#     print(expected_df[["location_id","city","district"]])

# function to insert df into warehouse using pg8000 

@pytest.fixture
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
    return test_db

def test_df_inserted_into_warehouse(test_db):
    df = pd.read_parquet(f"tests/data/dim_location.parquet")
    insert_df_into_warehouse(test_db, df, "dim_location")
    result = test_db.run("SELECT * FROM dim_location;")
    column_names = [column["name"] for column in test_db.columns]
    assert column_names[0] == "location_id"
    assert column_names[4] == "city"
    assert result[0][4] == "New Patienceburgh"
    assert result[1][4] == "Aliso Viejo"
    print(result)



