from src.extract.lambda_extract import lambda_extract
import pytest


def test_something():
    assert lambda_extract(None, None) == "team8"
