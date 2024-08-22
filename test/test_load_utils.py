import pytest
import boto3
import os
from moto import mock_aws
from src.utils.load_utils import (
    find_processed_data_bucket, 
    get_secret, 
    connect_to_db,
    convert_parquet_to_df
)
from pg8000.native import Connection
from dotenv import load_dotenv, find_dotenv
import json
import polars as pl
from io import BytesIO

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
load_dotenv(env_file)

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_DATAWAREHOUSE = os.getenv("PG_DATAWAREHOUSE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")


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


@pytest.fixture(scope="function")
def s3_with_parquet(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        # creating parquet file
        data = {'col1': [1, 2], 'col2': [3, 4]}
        df = pl.DataFrame(data)
        data_buffer_parquet = BytesIO()
        parquet = df.write_parquet(data_buffer_parquet)
        data_buffer_parquet.seek(0)
        parquet = data_buffer_parquet.read()
        s3.put_object(
            Body=parquet,
            Bucket="totesys-processed-data-000000",
            Key="test_parquet.parquet"
        )
        yield s3


@pytest.fixture(scope="function")
def no_s3(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        yield s3


@pytest.fixture(scope="function")
def secretsmanager(aws_credentials):
    with mock_aws():
        database_dict = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "database": PG_DATABASE,
            "port": PG_PORT,
        }
        secretsmanager = boto3.client("secretsmanager")
        secretsmanager.create_secret(
            Name="totesys_datawarehouse_credentials", SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


class TestFindBucket:

    def test_find_processed_data_bucket_finds_bucket(self, s3):
        bucket_name = find_processed_data_bucket()
        assert bucket_name == "totesys-processed-data-000000"

    def test_find_processed_data_bucket_raises_exception(self, no_s3):
        assert find_processed_data_bucket() == "No processed data bucket found"


class TestGetSecret:

    def test_get_secret_returns_exception(self, secretsmanager):
        secret_name = "nonexistent_secret"
        with pytest.raises(Exception):
            get_secret(secret_name)

    def test_get_secret_runs_correctly(self, secretsmanager):
        secret_name = "totesys_datawarehouse_credentials"
        credentials = get_secret(secret_name)
        expected = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "database": PG_DATABASE,
            "port": PG_PORT,
        }
        assert credentials == expected


class TestConnectToDB:

    def test_connect_to_db(self, secretsmanager ):
        secret_name = "totesys_datawarehouse_credentials"
        credentials = get_secret(secret_name)
        assert not isinstance(connect_to_db(credentials), Exception)


class TestConvertParquetToDF:

    def test_function_reads_parquet_from_s3(self, s3_with_parquet):
        parquet_file = "test_parquet.parquet"
        df = convert_parquet_to_df(parquet_file)
        print(df)
        assert isinstance(df, pl.DataFrame)

    def test_function_returns_error_message_when_file_is_not_parquet(self, s3_with_parquet):
        parquet_file = "test_csv.csv"
        assert convert_parquet_to_df(parquet_file) == f"{parquet_file} is not a .parquet file."

    def test_function_encounters_client_error_when_parquet_is_not_found(self, s3_with_parquet):
        parquet_file = "invalid_parquet.parquet"
        df = convert_parquet_to_df(parquet_file)
        print(df)
        assert df.startswith('Failed to get parquet file:')
