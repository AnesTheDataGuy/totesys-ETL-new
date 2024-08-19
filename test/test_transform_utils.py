import pytest, boto3, os
from moto import mock_aws
from src.utils.transform_utils import finds_data_buckets, convert_csv_to_parquet
from datetime import datetime as dt
import pandas as pd
from io import BytesIO


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for S3 bucket."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_raw(aws_credentials):
    """Mocked S3 client with raw data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3

@pytest.fixture(scope="function")
def s3_processed(aws_credentials):
    """Mocked S3 client with processed data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3

@pytest.fixture(scope="function")
def s3_no_buckets(aws_credentials):
    """Mocked S3 client with no buckets."""
    with mock_aws():
        s3_nobuckets = boto3.client("s3")
        yield s3_nobuckets

@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mocked S3 client with no buckets."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


def test_no_buckets_found(s3_no_buckets):

    result = finds_data_buckets()
    assert result == 'No buckets found'


def test_no_raw_bucket_found(s3_processed):
    result = finds_data_buckets()
    assert result == 'No raw data bucket found'


def test_no_processed_bucket_found(s3_raw):
    result = finds_data_buckets()
    assert result == 'No processed data bucket found'


def test_returns_buckets_when_found(s3):
    result = finds_data_buckets()
    assert result == ('totesys-raw-data-000000','totesys-processed-data-000000')


def test_csv_file_not_found(s3):
    result = convert_csv_to_parquet('test.csv')
    assert result == "csv file not found"

def test_convert_csv_to_parquet(s3):
    s3.put_object(
        Body="""test,test2,test3
                1,2,3
                5,6,7
                8,9,10""",
        Bucket='totesys-raw-data-000000',
        Key='test.csv'
    )
    result = convert_csv_to_parquet('test.csv')
    parquet_buffer = BytesIO(result)
    df_read_parquet = pd.read_parquet(parquet_buffer)
    data = {'test': [1,5,8], 'test2': [2,6,9], 'test3': [3,7,10]}
    df = pd.DataFrame(data)

    assert isinstance(df_read_parquet, pd.DataFrame)
    assert df.equals(df_read_parquet)

def test_returns_appropriate_message_if_file_is_not_csv(s3):
    s3.put_object(
        Body="""Apple""",
        Bucket='totesys-raw-data-000000',
        Key='test.txt'
    )
    result = convert_csv_to_parquet('test.txt')
    assert result == "test.txt is not a .csv file."