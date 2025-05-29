from src.extract.main import *
import pytest 

@pytest.fixture(scope="module")
def db():
    db = create_conn()
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


