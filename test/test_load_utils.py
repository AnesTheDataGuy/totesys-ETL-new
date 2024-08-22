import pytest
import boto3
import os
from moto import mock_aws
from src.utils.load_utils import *


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for S3 bucket."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3

def test_find_processed_data_bucket(s3):
    bucket_name = find_processed_data_bucket()
    assert bucket_name == "totesys-processed-data-000000"

def test_get_secret():
    secret_name = "my_secret"
    secret_value = get_secret(secret_name)
    assert secret_value == "test_secret"

def test_connect_to_db():
    credentials = {
        "host": "localhost",
        "port": 5432,
        "database": "my_db",
        "user": "my_user",
        "password": "my_password"
    }
    connection = connect_to_db(credentials)
    assert connection.is_connected()

def test_convert_parquet_to_df(s3):
    parquet_file = "example.parquet"
    df = convert_parquet_to_df(parquet_file)
    assert isinstance(df, pl.DataFrame)
