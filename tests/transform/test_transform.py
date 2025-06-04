import pytest
import pandas as pd
from io import StringIO
from src.transform.lambda_transform import *

@pytest.fixture
def staff_df():
    return pd.read_json("tests/data/staff.json").reset_index(drop=True)

@pytest.fixture
def department_df():
    return pd.read_json("tests/data/department.json").reset_index()

def test_mvp_transform_df_staff(monkeypatch, staff_df, department_df):
    # Patch key_to_df to return department_df
    from src.transform import lambda_transform
    monkeypatch.setattr(lambda_transform, "key_to_df", lambda *_: department_df)

    result = mvp_transform_df(None, "staff", staff_df, None)

    print(result)
    print(result["dim_staff"])

    assert "dim_staff" in result
    df = result["dim_staff"]
    assert not df.empty
    assert set(["staff_id", 
                "first_name", 
                "last_name", 
                "department_name", 
                "location", 
                "email_address"]).issubset(df.columns)
    
    # Extract the row where staff_id is 1
    staff_row = df[df["staff_id"] == 1].iloc[0]

    # Assert fields
    assert staff_row["first_name"] == "Jeremie"
    assert staff_row["last_name"] == "Franey"
    assert staff_row["email_address"] == "jeremie.franey@terrifictotes.com"
    assert staff_row["department_name"] == "Purchasing" 
    assert staff_row["location"] == "Manchester"        
