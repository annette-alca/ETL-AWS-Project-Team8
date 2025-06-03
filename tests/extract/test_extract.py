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

# @pytest.fixture(scope='class')
# def aws_credentials():
#     os.environ["aws_access_key_id"]="Test"
#     os.environ["aws_secret_access_key"]="test"
#     os.environ["aws_session_token"]="test"
#     os.environ["aws_security_token"]="test"
#     os.environ["aws_region"]="eu-west-2"

@pytest.fixture
def s3_client():
    with mock_aws():
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

def test_get_data_from_database_first_ingestion(db):
    new_dict_list, extract_time = get_data(db, "department")
    assert isinstance(new_dict_list, list)
    assert len(new_dict_list) >= 8
    assert isinstance(new_dict_list[0], dict)
    for key in ["department_id", "department_name", "location", 
                "manager", "created_at", "last_updated"]:
        assert key in new_dict_list[0].keys()
    assert isinstance(extract_time, str)

def test_get_data_when_there_are_no_updates(db):
    new_dict_list, extract_time = get_data(db, "department", "2025-05-28")
    assert isinstance(new_dict_list, list)
    assert len(new_dict_list) ==0

def test_get_data_handles_DataBaseError(db):
    last_extract = "2025-04-04"
    new_dict_list, extract_time = get_data(db, "fake_table",last_extract)
    assert new_dict_list == []
    assert extract_time == last_extract

@pytest.mark.skip
def test_transactions_db_for_updates(db):
    new_dict_list, extract_time = get_data(db, "transaction", "2025-05-29T14:01")
    assert isinstance(new_dict_list, list)
    assert len(new_dict_list) == 0

def test_save_to_s3_when_sql_table_has_values(s3_client_with_bucket):
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

def test_save_to_s3_when_sql_table_is_empty(s3_client_with_bucket):
    extract_client, bucket_name = s3_client_with_bucket
    extract_time = "2025-05-30T10:33.4323"
    new_dict_list = []
    table_name = 'fake_data'
    result = save_to_s3(extract_client, bucket_name, new_dict_list, table_name, extract_time)
    assert result == None

