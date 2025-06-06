from src.load.lambda_load import parquet_to_df
import pandas as pd
from moto import mock_aws
import pytest
import boto3
import os 
import dotenv

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

# function to insert df into warehouse using pg8000 

@pytest.fixture
def db():
    dotenv.load_dotenv()
    user = os.environ["LOCALUSER"]
    database = os.environ["LOCALDB"]
    password = os.environ["LOCALPASSWORD"]
    
    test_db = Connection(database=database, user=user, password=password)
    test_db.run('DROP TABLE IF EXISTS ')

def test_df_inserted_into_warehouse():
    pass 
