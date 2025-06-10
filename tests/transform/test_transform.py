import pytest
import pandas as pd
from io import StringIO
from src.transform.lambda_transform import *
import boto3
from moto import mock_aws
import os
import json
from pprint import pprint

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
    s3_boto.create_bucket(Bucket=bucket_1,
                        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
    s3_boto.create_bucket(Bucket=bucket_2,
                        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})

    #INGESTION UPLOADS

    with open("./tests/data/staff.json", "r") as jsonfile:
        body = json.dumps(json.load(jsonfile))          
    
        s3_boto.put_object(Bucket=bucket_1, Key=key, Body=body.encode("utf-8"))
        # output = s3_boto.list_objects_v2(Bucket=bucket_1, Prefix=key)
    
    with open("./tests/data/department.json", "r") as jsonfile:
        body = json.dumps(json.load(jsonfile))          
    
        s3_boto.put_object(Bucket=bucket_1, Key="dev/department", Body=body.encode("utf-8"))

    with open("./tests/data/address.json", "r") as jsonfile:
        body = json.dumps(json.load(jsonfile))          
    
        s3_boto.put_object(Bucket=bucket_1, Key="dev/address", Body=body.encode("utf-8"))

    with open("./tests/data/counterparty.json", "r") as jsonfile:
        body = json.dumps(json.load(jsonfile))          
    
        s3_boto.put_object(Bucket=bucket_1, Key="dev/counterparty", Body=body.encode("utf-8"))

    with open("./tests/data/sales_order.json", "r") as jsonfile:
        body = json.dumps(json.load(jsonfile))          
    
        s3_boto.put_object(Bucket=bucket_1, Key="dev/sales_order", Body=body.encode("utf-8"))



class TestAppendJSONRaw:

    def test_append_json_raw_except_with_empty_processed_bucket(self, s3_boto, mock_s3_buckets):
        new_json_key = 'dev/staff'
        processed_key = 'db_state/staff_all.json'  

        # Assert bucket is empty before test
        assert s3_boto.list_objects_v2(Bucket='processed-bucket')['KeyCount'] == 0 

        result = append_json_raw_tables(s3_boto, 'ingestion-bucket', new_json_key, 'processed-bucket')
  
        processed_obj = s3_boto.get_object(Bucket="processed-bucket", Key=processed_key)
        processed_json = json.loads(processed_obj["Body"].read().decode("utf-8"))
        processed_json_0 = processed_json[0]

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
        processed_json_0 = processed_json[0]
        processed_json_4 = processed_json[4]

        # Assert that processed is longer than new df
        assert len(processed_json) > len(result[1]) 
        # Assert length of processed data has increased        
        assert len(processed_json) > 4
        # Assert that first row of new df is same as relative row in processed
        assert ((result[1].iloc[0]['first_name'])) == processed_json_4['first_name']

class TestMVPTransformDF:

    def test_transform_staff_case(self, s3_boto, mock_s3_buckets):
        ingestion_bucket = "ingestion-bucket"
        processed_bucket = "processed-bucket"
        staff_key = "dev/staff"
        department_key = "dev/department"

        append_json_raw_tables(s3_boto, ingestion_bucket, department_key, processed_bucket)
        table_name, new_df = append_json_raw_tables(s3_boto, ingestion_bucket,staff_key, processed_bucket)

        ## run transformation
        result = mvp_transform_df(s3_boto, table_name , new_df, processed_bucket)
        
        dim_staff = result["dim_staff"]

        ## assert
        assert isinstance(result, dict)
        assert "dim_staff" in result
        assert not dim_staff.empty

        transformed_columns = [
            "staff_id", "first_name", "last_name",
            "department_name", "location", "email_address"
        ]
        assert set(transformed_columns).issubset(dim_staff.columns)

        transformed_row_by_staff_id = dim_staff[dim_staff["staff_id"] == 1].iloc[0]
        assert transformed_row_by_staff_id["first_name"] == "Jeremie"
        assert transformed_row_by_staff_id["department_name"] == "Purchasing"

    def test_transform_address_case(self, s3_boto, mock_s3_buckets):
        ingestion_bucket = "ingestion-bucket"
        processed_bucket = "processed-bucket"
        address_key = "dev/address"
        
        table_name, new_df = append_json_raw_tables(s3_boto, ingestion_bucket,address_key, processed_bucket)

        ## run transformation
        result = mvp_transform_df(s3_boto, table_name, new_df, processed_bucket)
        dim_location = result["dim_location"]

        ## assert structure
        assert isinstance(result, dict)
        assert "dim_location" in result
        assert not dim_location.empty

        expected_columns = [
            "location_id", "address_line_1", "address_line_2",
            "district", "city", "postal_code", "country", "phone"
        ]
        assert set(expected_columns).issubset(dim_location.columns)
        
        ## assert content 
        transformed_row_by_address_id = dim_location.iloc[1,:]
        assert transformed_row_by_address_id["location_id"] == 2
        assert transformed_row_by_address_id["city"] == 'Aliso Viejo'
        assert dim_location.loc[1,"country"]=="San Marino"


    def test_transform_counterparty_case(self, s3_boto, mock_s3_buckets):
        
        ingestion_bucket = "ingestion-bucket"
        processed_bucket = "processed-bucket"
        counterparty_key = "dev/counterparty"

        table_name, new_df = append_json_raw_tables(s3_boto, ingestion_bucket,counterparty_key, processed_bucket)

        ## run transformation
        result = mvp_transform_df(s3_boto, table_name, new_df, processed_bucket)
        dim_counterparty = result["dim_counterparty"]

        ## assert structure
        assert isinstance(result, dict)
        assert "dim_counterparty" in result
        assert not dim_counterparty.empty

        expected_columns = [
            "counterparty_id", "counterparty_legal_name", "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2", "counterparty_legal_district", "counterparty_legal_city", 
            "counterparty_legal_postal_code", "counterparty_legal_country", "counterparty_legal_phone_number"
        ]
        assert set(expected_columns).issubset(dim_counterparty.columns)

        ## assert content 
        assert dim_counterparty.loc[2, "counterparty_id"] == 3
        assert dim_counterparty.loc[2, "counterparty_legal_address_line_1"] == "179 Alexie Cliffs"
        assert dim_counterparty.loc[2, "counterparty_legal_phone_number"] == "9621 880720"

    def test_transform_design_case(self, s3_boto, mock_s3_buckets):
        processed_bucket = "processed-bucket"

        with open("./tests/data/design.json", "r") as design_file:
            design_body = json.load(design_file)

        new_df = pd.DataFrame(design_body)

        result = mvp_transform_df(s3_boto, "design", new_df, processed_bucket)
        dim_design = result["dim_design"]

        assert isinstance(result, dict)
        assert "dim_design" in result
        assert not dim_design.empty

        expected_columns = [
            "design_id", "design_name", "file_location", "file_name"
        ]
        
        assert set(expected_columns).issubset(dim_design.columns)
        assert dim_design.loc[0, "design_id"] == 8
        assert dim_design.loc[0, "design_name"] == "Wooden"
        assert dim_design.loc[5, "design_id"] == 10
        assert dim_design.loc[4, "file_name"] == "plastic-20221206-bw3l.json"

    def test_transform_currency_case(self, s3_boto, mock_s3_buckets):
        processed_bucket = "processed-bucket"

        with open("tests/data/currency.json") as currency_file:
            currency_body = json.load(currency_file)

        new_df = pd.DataFrame(currency_body)

        result = mvp_transform_df(s3_boto, "currency", new_df, processed_bucket)
        dim_currency = result["dim_currency"]

        assert isinstance(result, dict)
        assert "dim_currency" in result
        assert not dim_currency.empty

        expected_columns = ["currency_id", "currency_name", "currency_code"] 

        assert set(expected_columns).issubset(dim_currency.columns)

        assert dim_currency.loc[0, "currency_id"] == 1
        assert dim_currency.loc[0, "currency_code"] == "GBP"
        assert dim_currency.loc[0, "currency_name"] == "British pound"
        assert dim_currency.loc[2, "currency_id"] == 3
        assert dim_currency.loc[2, "currency_code"] == "EUR"
        assert dim_currency.loc[2, "currency_name"] == "Euro"

    def test_transform_sales_order_and_date_case(self, s3_boto, mock_s3_buckets):        
        ingestion_bucket = "ingestion-bucket"
        processed_bucket = "processed-bucket"
        sales_key = "dev/sales_order"
        
        table_name, new_df = append_json_raw_tables(s3_boto, ingestion_bucket, sales_key, processed_bucket)

        ## run transformation
        result = mvp_transform_df(s3_boto, table_name, new_df, processed_bucket)
        
        assert len(result) == 2
        fact_result = result['fact_sales_order']
        dim_date_result = result['dim_date']

        fact_cols_expected = [
                    "sales_order_id",
                    "created_date",
                    "created_time",
                    "last_updated_date",
                    "last_updated_time",
                    "sales_staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "design_id",
                    "agreed_payment_date",
                    "agreed_delivery_date",
                    "agreed_delivery_location_id",
                ]
        
        assert set(fact_cols_expected).issubset(fact_result.columns)

        assert fact_result.loc[0, 'sales_order_id'] == 14454
        assert fact_result.loc[0, 'design_id'] == 203
        assert fact_result.loc[0, 'sales_staff_id'] == 7
        assert fact_result.loc[0, 'counterparty_id'] == 20
        assert fact_result.loc[0, 'agreed_delivery_location_id'] == 24

        dim_date_expected = ['date_id',
                             'year',
                             'month',
                             'day',
                             'day_of_week',
                             'day_name',
                             'month_name',
                             'quarter']


        assert set(dim_date_expected).issubset(dim_date_result.columns)

        assert str(dim_date_result.loc[0, 'date_id']) == '2025-06-02 00:00:00'
        assert dim_date_result.loc[0, 'day_name'] == 'Monday'
        assert dim_date_result.loc[0, 'month'] == 6
        assert dim_date_result.loc[0, 'quarter'] == 2
        assert dim_date_result.loc[0, 'month_name'] == 'June'

class TestSerialiseObjectFunction:

    def test_serialise_object_returns_isoformat(self):
        test_datetime = datetime(2025, 6, 6, 9, 22, 10, 153000) 
        assert isinstance(serialise_object(test_datetime), str)

    def test_serialise_object_returns_float(self):
        test_decimal = Decimal('339985.77')
        assert isinstance(serialise_object(test_decimal), float)

    def test_serialise_object_raises_TypeError(self):
        with pytest.raises(TypeError):
            serialise_object('test_string')
   
class TestSaveParquetToS3:

    def test_save_parquet_to_s3(self, s3_boto, mock_s3_buckets):
        
        assert s3_boto.list_objects_v2(Bucket='processed-bucket')['KeyCount'] == 0 
 
        ingestion_bucket = "ingestion-bucket"
        processed_bucket = "processed-bucket"
        sales_key = "dev/sales_order"
        
        table_name, new_df = append_json_raw_tables(s3_boto, ingestion_bucket, sales_key, processed_bucket)

        ## run transformation
        transformed_dict = mvp_transform_df(s3_boto, table_name, new_df, processed_bucket)
        saved_parquet = 'dev/fact_sales_order/2022/fact_sales_order_14:20.parquet'
        save_parquet_to_s3(processed_bucket, transformed_dict,"2022T14:20")
        assert s3_boto.list_objects_v2(Bucket='processed-bucket')['Contents'][3]['Key'] == saved_parquet
        

class TestTransformLambdaHandler:

    def test_lambda_transform_with_no_event(self, s3_boto, mock_s3_buckets, monkeypatch):
        test_events = {
                "message": "completed ingestion",
                "timestamp": "2025-06-10T10:04:36.847261",
                "total_new_files": 0,
                "new_keys": []
                }
        
        monkeypatch.setenv("INGESTION_S3", 'ingestion-bucket')
        monkeypatch.setenv("PROCESSED_S3", 'processed-bucket')
        result = lambda_transform(test_events, None)
        assert len(result) == 4
        assert result.keys() == {'message', 'timestamp', 'total_new_files', 'new_keys'}
       

    def test_lambda_transform_with_1_event_and_no_data(self, s3_boto, mock_s3_buckets, monkeypatch):
        test_events = {
                "message": "completed ingestion",
                "timestamp": "2025-06-10T10:04:36.847261",
                "total_new_files": 1,
                "new_keys": ['dev/sales_order']
                }
        
        monkeypatch.setenv("INGESTION_S3", 'ingestion-bucket')
        monkeypatch.setenv("PROCESSED_S3", 'processed-bucket')

        print(s3_boto)
        monkeypatch.setattr("boto3.client", lambda _: s3_boto)

        result = lambda_transform(test_events, None)
        

        assert len(result) == 4
        assert result.keys() == {'message', 'timestamp', 'total_new_files', 'new_keys'}
        assert result['total_new_files'] == 2
       
        
        
        

        

    

