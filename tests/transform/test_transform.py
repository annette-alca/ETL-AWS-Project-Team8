import pytest
import pandas as pd
from io import StringIO
from src.transform.lambda_transform import *
import boto3
from moto import mock_aws
import os
import json
from pprint import pprint

@pytest.fixture
def staff_df():
    return pd.read_json("tests/data/staff.json").reset_index(drop=True)

@pytest.fixture
def department_df():
    return pd.read_json("tests/data/department.json").reset_index()

@pytest.fixture
def sales_order_df():
    return pd.read_json("tests/data/sales_order.json").reset_index()

@pytest.fixture(scope='module')
def aws_credentials():
    os.environ["aws_access_key_id"]="Test"
    os.environ["aws_secret_access_key"]="test"
    os.environ["aws_session_token"]="test"
    os.environ["aws_security_token"]="test"
    os.environ["aws_region"]="eu-west-2"

@pytest.fixture(scope='class')
def s3_boto(aws_credentials):
    with mock_aws(aws_credentials):

        yield boto3.client('s3')


@pytest.fixture(scope='class')
def mock_s3_buckets(s3_boto):
    """Create ingestion and processed buckets for test functions"""
    
    bucket_1 = "ingestion-bucket"
    bucket_2 = "processed-bucket"

    key = "dev/staff"
    
    with open("./tests/data/staff.json", "r") as jsonfile:
        body = json.dumps(json.load(jsonfile))          
        s3_boto.create_bucket(Bucket=bucket_1,
                            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})

        s3_boto.put_object(Bucket=bucket_1, Key=key, Body=body.encode("utf-8"))
        # output = s3_boto.list_objects_v2(Bucket=bucket_1, Prefix=key)
       

    s3_boto.create_bucket(Bucket=bucket_2,
                            CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
    


##  MVP Transform to be refactored to use mock S3 and latest updates

class TestMVPTransformDFStaff:

    @pytest.mark.skip
    def test_mvp_transform_df_staff(self, monkeypatch, staff_df, department_df):
        # Patch key_to_df to return department_df
        from src.transform import lambda_transform
        monkeypatch.setattr(lambda_transform, "table_name_to_df", lambda *_: department_df)

        staff_result = mvp_transform_df(None, "staff", staff_df, None)

        print(staff_result)
        print(staff_result["dim_staff"])

        assert "dim_staff" in staff_result
        staff_df = staff_result["dim_staff"]
        assert not staff_df.empty
        assert set(["staff_id", 
                    "first_name", 
                    "last_name", 
                    "department_name", 
                    "location", 
                    "email_address"]).issubset(staff_df.columns)
        
        # Extract the row where staff_id is 1
        staff_row = staff_df[staff_df["staff_id"] == 1].iloc[0]

        # Assert fields
        assert staff_row["first_name"] == "Jeremie"
        assert staff_row["last_name"] == "Franey"
        assert staff_row["email_address"] == "jeremie.franey@terrifictotes.com"
        assert staff_row["department_name"] == "Purchasing" 
        assert staff_row["location"] == "Manchester"   


    
    @pytest.mark.skip
    def test_mvp_transform_df_sales_order(self, sales_order_df):
        sales_result = mvp_transform_df(None,"sales_order", sales_order_df, None)
        
        assert "fact_sales_order" in sales_result
        sales_order_df = sales_result["fact_sales_order"]
        assert not sales_order_df.empty
        assert set(['sales_order_id', 'created_date', 'created_time', 'last_updated_date',
        'last_updated_time', 'staff_id', 'counterparty_id', 'units_sold',
        'unit_price', 'currency_id', 'design_id', 'agreed_payment_date',
        'agreed_delivery_date', 'agreed_delivery_location_id']).issubset(sales_order_df.columns)
            
        assert "dim_date" in sales_result
        dim_date_df = sales_result["dim_date"]
        assert not dim_date_df.empty
        print(dim_date_df.columns)
        assert set(['date_id', 'year', 'month', 'date']).issubset(dim_date_df.columns)






class TestAppendJSONRaw:

    def test_append_json_raw_except_with_empty_processed_bucket(self, s3_boto,mock_s3_buckets):
        new_json_key = 'dev/staff'
        processed_key = 'db_state/staff_all.json'  

        # Assert bucket is empty before test
        assert s3_boto.list_objects_v2(Bucket='processed-bucket')['KeyCount'] == 0 

        result = append_json_raw_tables(s3_boto, 'ingestion-bucket', new_json_key, 'processed-bucket')
  
        processed_obj = s3_boto.get_object(Bucket="processed-bucket", Key=processed_key)
        processed_json = json.loads(processed_obj["Body"].read().decode("utf-8"))
        processed_json_0 = processed_json['0']

        # Assert new key has been added to bucket
        assert s3_boto.list_objects_v2(Bucket='processed-bucket')['KeyCount'] == 1
        # Assert length of dataframe 
        assert len(result[1]) == 4
        # Assert new_df and merged df have the same data
        assert ((result[1].iloc[0]['first_name'])) == processed_json_0['first_name']
        assert len(processed_json) == len(result[1]) # 4


    def test_append_json_raw_with_existing_data(self, s3_boto,mock_s3_buckets):
        new_json_key = 'dev/staff'
        processed_key = 'db_state/staff_all.json'
        with open("./tests/data/staff_add.json", "r") as jsonfile:
            body = json.dumps(json.load(jsonfile))
            s3_boto.put_object(Bucket='ingestion-bucket', Key=new_json_key, Body=body.encode("utf-8"))

        result = append_json_raw_tables(s3_boto, 'ingestion-bucket', new_json_key, 'processed-bucket')

        processed_obj = s3_boto.get_object(Bucket="processed-bucket", Key=processed_key)
        processed_json = json.loads(processed_obj["Body"].read().decode("utf-8"))
        processed_json_0 = processed_json['0']
        processed_json_4 = processed_json['4']

        # Assert that processed is longer than new df
        assert len(processed_json) > len(result[1]) 
        # Assert length of processed data has increased        
        assert len(processed_json) > 4
        # Assert that first row of new df is same as relative row in processed
        assert ((result[1].iloc[0]['first_name'])) == processed_json_4['first_name']

        



     

