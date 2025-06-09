from src.extract.lambda_extract import *
import pytest 
import boto3
from moto import mock_aws
from unittest.mock import Mock, patch
import dotenv

@pytest.fixture(scope="module")
def db():
    extract_client = boto3.client('s3')
    db = create_conn(extract_client)
    yield db
    db.close()

@pytest.fixture(scope='class')
def aws_credentials():
    os.environ["aws_access_key_id"]="Test"
    os.environ["aws_secret_access_key"]="test"
    os.environ["aws_session_token"]="test"
    os.environ["aws_security_token"]="test"
    os.environ["aws_region"]="eu-west-2"

@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws(aws_credentials):
        yield boto3.client('s3')

@pytest.fixture
def s3_client_with_bucket(s3_client):
    bucket_name = "test_bucket"
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    return s3_client, bucket_name

@pytest.fixture()
def db(s3_client):
    dotenv.load_dotenv()
    mock_password = Mock(return_value=os.environ["DBPASSWORD"])
    with patch('src.extract.lambda_extract.get_db_password', mock_password):
        return create_conn(s3_client)

@patch("src.extract.lambda_extract.get_last_timestamps")
@patch("src.extract.lambda_extract.get_data")
@patch("src.extract.lambda_extract.save_to_s3")
@patch("src.extract.lambda_extract.create_conn")
class TestLambdaExtract:
    def test_lambda_extract_returns_a_dict(self, mock_create_conn, mock_save_to_s3,
                                mock_get_data, mock_get_last_timestamps, 
                                s3_client):
        
        # mocking/patching 
        mock_create_conn.return_value = s3_client
        s3_client.create_bucket(
            Bucket="team-08-ingestion-20250528081548341900000001", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
            ) 

        mock_get_last_timestamps.return_value = ({'address': '2025-06-09T14:09:23.675428', 'counterparty': '2025-06-09T14:09:23.680175', 'currency': '2025-06-09T14:09:23.685458', 'department': '2025-06-09T14:09:23.690808', 'design': '2025-06-09T14:09:23.696537', 'payment': '2025-06-09T14:09:23.703961', 'payment_type': '2025-06-09T14:09:23.708820', 'purchase_order': '2025-06-09T14:09:23.716083', 'sales_order': '2025-06-09T14:09:23.723256', 'staff': '2025-06-09T14:09:23.728582', 'transaction': '2025-06-09T14:09:23.736180'}, 'db_state/extraction_timestamps.json')
       
        mock_get_data.side_effect = [
            ([{
            "address_id": 30,
            "address_line_1": "0336 Ruthe Heights",
            "address_line_2": "null",
            "district": "Buckinghamshire",
            "city": "Lake Myrlfurt",
            "postal_code": "94545-4284",
            "country": "Falkland Islands (Malvinas)",
            "phone": "1083 286132",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }], "2025-06-09T13:24:39.123889")] + [([], "2025-06-09T13:24:39.123889")] * 10
        
        mock_save_to_s3.side_effect = ["dev/address/2025-06-06/address_08:53:25.773840.json"] + [None] * 10
        
        result = lambda_extract(None, None)
        assert type(result) == dict
        assert len(result) == 4

    def test_lambda_extract_returns_a_dict_with_correct_keys(self, mock_create_conn, mock_save_to_s3,
                                mock_get_data, mock_get_last_timestamps, 
                                s3_client):
        
        # mocking/patching 
        mock_create_conn.return_value = s3_client
        s3_client.create_bucket(
            Bucket="team-08-ingestion-20250528081548341900000001", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
            ) 
        
        mock_get_last_timestamps.return_value = ({'address': '2025-06-09T14:09:23.675428', 'counterparty': '2025-06-09T14:09:23.680175', 'currency': '2025-06-09T14:09:23.685458', 'department': '2025-06-09T14:09:23.690808', 'design': '2025-06-09T14:09:23.696537', 'payment': '2025-06-09T14:09:23.703961', 'payment_type': '2025-06-09T14:09:23.708820', 'purchase_order': '2025-06-09T14:09:23.716083', 'sales_order': '2025-06-09T14:09:23.723256', 'staff': '2025-06-09T14:09:23.728582', 'transaction': '2025-06-09T14:09:23.736180'}, 'db_state/extraction_timestamps.json')
        mock_get_data.side_effect = [
            ([{
            "address_id": 30,
            "address_line_1": "0336 Ruthe Heights",
            "address_line_2": "null",
            "district": "Buckinghamshire",
            "city": "Lake Myrlfurt",
            "postal_code": "94545-4284",
            "country": "Falkland Islands (Malvinas)",
            "phone": "1083 286132",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }], "2025-06-09T13:24:39.123889")] + [([], "2025-06-09T13:24:39.123889")] * 10
        
        mock_save_to_s3.side_effect = ["dev/address/2025-06-06/address_08:53:25.773840.json"] + [None] * 10
        
        result = lambda_extract(None, None)
        expected_keys = ["message", "timestamp", "total_new_files", "new_keys"]
        for key in result:
            assert key in expected_keys
    
    def test_lambda_extract_dict_has_correct_data_types(self, mock_create_conn, mock_save_to_s3,
                                mock_get_data, mock_get_last_timestamps, 
                                s3_client):
        
        # mocking/patching 
        mock_create_conn.return_value = s3_client
        s3_client.create_bucket(
            Bucket="team-08-ingestion-20250528081548341900000001", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
            ) 

        mock_get_last_timestamps.return_value = ({'address': '2025-06-09T14:09:23.675428', 'counterparty': '2025-06-09T14:09:23.680175', 'currency': '2025-06-09T14:09:23.685458', 'department': '2025-06-09T14:09:23.690808', 'design': '2025-06-09T14:09:23.696537', 'payment': '2025-06-09T14:09:23.703961', 'payment_type': '2025-06-09T14:09:23.708820', 'purchase_order': '2025-06-09T14:09:23.716083', 'sales_order': '2025-06-09T14:09:23.723256', 'staff': '2025-06-09T14:09:23.728582', 'transaction': '2025-06-09T14:09:23.736180'}, 'db_state/extraction_timestamps.json')
       
        mock_get_data.side_effect = [
            ([{
            "address_id": 30,
            "address_line_1": "0336 Ruthe Heights",
            "address_line_2": "null",
            "district": "Buckinghamshire",
            "city": "Lake Myrlfurt",
            "postal_code": "94545-4284",
            "country": "Falkland Islands (Malvinas)",
            "phone": "1083 286132",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }], "2025-06-09T13:24:39.123889")] + [([], "2025-06-09T13:24:39.123889")] * 10
        
        mock_save_to_s3.side_effect = ["dev/address/2025-06-06/address_08:53:25.773840.json"] + [None] * 10

        result = lambda_extract(None, None)
        assert type(result["message"]) == str
        assert type(result["timestamp"]) == str
        assert type(result["total_new_files"]) == int
        assert type(result["new_keys"]) == list
        assert len(result["new_keys"]) == result["total_new_files"]
    
    def test_lambda_extract_returns_correct_values(self, mock_create_conn, mock_save_to_s3,
                                mock_get_data, mock_get_last_timestamps, 
                                s3_client):
        # mocking/patching 
        mock_create_conn.return_value = s3_client
        s3_client.create_bucket(
            Bucket="team-08-ingestion-20250528081548341900000001", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
            ) 

        mock_get_last_timestamps.return_value = ({'address': '2025-06-09T14:09:23.675428', 'counterparty': '2025-06-09T14:09:23.680175', 'currency': '2025-06-09T14:09:23.685458', 'department': '2025-06-09T14:09:23.690808', 'design': '2025-06-09T14:09:23.696537', 'payment': '2025-06-09T14:09:23.703961', 'payment_type': '2025-06-09T14:09:23.708820', 'purchase_order': '2025-06-09T14:09:23.716083', 'sales_order': '2025-06-09T14:09:23.723256', 'staff': '2025-06-09T14:09:23.728582', 'transaction': '2025-06-09T14:09:23.736180'}, 'db_state/extraction_timestamps.json')
       
        mock_get_data.side_effect = [
            ([{
            "address_id": 30,
            "address_line_1": "0336 Ruthe Heights",
            "address_line_2": "null",
            "district": "Buckinghamshire",
            "city": "Lake Myrlfurt",
            "postal_code": "94545-4284",
            "country": "Falkland Islands (Malvinas)",
            "phone": "1083 286132",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }], "2025-06-09T13:24:39.123889")] + [([], "2025-06-09T13:24:39.123889")] * 10
        
        mock_save_to_s3.side_effect = ["dev/address/2025-06-06/address_08:53:25.773840.json"] + [None] * 10
        
        result = lambda_extract(None, None)
        
        assert result["message"] == 'completed ingestion'
        assert result["timestamp"] == "2025-06-09T13:24:39.123889"
        assert result["total_new_files"] == 1
        assert result["new_keys"] == ["dev/address/2025-06-06/address_08:53:25.773840.json"]


class TestGetDataFunction:    
    def test_get_data_from_database_first_ingestion(self, db):
        new_dict_list, extract_time = get_data(db, "department")
        assert isinstance(new_dict_list, list)
        assert len(new_dict_list) >= 8
        assert isinstance(new_dict_list[0], dict)
        for key in ["department_id", "department_name", "location", 
                    "manager", "created_at", "last_updated"]:
            assert key in new_dict_list[0].keys()
        assert isinstance(extract_time, str)

    def test_get_data_when_there_are_no_updates(self, db):
        new_dict_list, _ = get_data(db, "department", "2025-05-28")
        assert isinstance(new_dict_list, list)
        assert len(new_dict_list) ==0

    def test_get_data_handles_DataBaseError(self, db):
        last_extract = "2025-04-04"
        new_dict_list, extract_time = get_data(db, "fake_table",last_extract)
        assert new_dict_list == []
        assert extract_time == last_extract

class TestSaveToS3:
    def test_save_to_s3_when_sql_table_has_values(self, s3_client_with_bucket):
        extract_client, bucket_name = s3_client_with_bucket
        extract_time = "2025-05-30T10:33.4323"
        new_dict_list = [{'fake':1}, {'fake':2}]
        table_name = 'fake_data'
        date, time = extract_time.split('T')
        expected_key = f"dev/{table_name}/{date}/{table_name}_{time}.json"
        result = save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time)
        assert result == expected_key
        file_content = extract_client.get_object(Bucket=bucket_name, Key= expected_key)
        file_dict = json.loads(file_content["Body"].read().decode("utf-8"))
        assert file_dict == new_dict_list

    def test_save_to_s3_when_sql_table_is_empty(self, s3_client_with_bucket):
        extract_client, bucket_name = s3_client_with_bucket
        extract_time = "2025-05-30T10:33.4323"
        new_dict_list = []
        table_name = 'fake_data'
        result = save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time)
        assert result == None

class TestGetLastTimeStamps:
    def test_get_last_timestamps_returns_dict_and_key(self, s3_client_with_bucket):
        s3_client, bucket_name = s3_client_with_bucket
        result = get_last_timestamps(s3_client, bucket_name)
        assert type(result) == tuple
        assert len(result) == 2
        assert type(result[0]) == dict
        assert type(result[1]) == str

class TestGetDbPassword:
    def test_get_db_password_returns_str(self, s3_client):
        extract_client = s3_client
        extract_client.create_bucket(
            Bucket="bucket-to-hold-tf-state-for-terraform", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
            )
        extract_client.upload_file("tests/data/secrets.json", "bucket-to-hold-tf-state-for-terraform", 'secrets/secrets.json')
        result = get_db_password(extract_client)
        assert type(result) == str
        assert result == "password" 

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
