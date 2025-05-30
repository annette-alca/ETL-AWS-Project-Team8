from src.extract.lambda_extract import *
import pytest 
import boto3

@pytest.fixture(scope="module")
def db():
    extract_client = boto3.client('s3')
    db = create_conn(extract_client)
    yield db
    db.close()


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

@pytest.mark.skip
def test_transactions_db_for_updates(db):
    new_dict_list, extract_time = get_data(db, "transaction", "2025-05-29 14:01")
    print(f'New dict list: {new_dict_list}  <<<<<<<<<<<<<<<<<')
    assert isinstance(new_dict_list, list)
    assert len(new_dict_list) == 0



