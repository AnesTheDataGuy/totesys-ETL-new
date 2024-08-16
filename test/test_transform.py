import pytest, boto3, os, shutil
from moto import mock_aws
from src.test_functions.transform_testing import lambda_handler as transform
from datetime import datetime as dt


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
    """Mocked S3 client with raw data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.create_bucket(
            Bucket="totesys-processed-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture(scope="function")
def s3_raw_only(aws_credentials):
    """Mocked S3 client with raw data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@pytest.fixture(scope="function")
def s3_proc_only(aws_credentials):
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


class DummyContext:  # Dummy context class used for testing
    pass


event = {}
context = DummyContext()


def test_s3_finds_no_buckets(s3_no_buckets):
    assert transform(event, context) == "No buckets found"


def test_s3_finds_raw_bucket_only(s3_raw_only):
    assert transform(event, context) == "No processed data bucket found"


def test_s3_finds_processed_bucket_only(s3_proc_only):
    assert transform(event, context) == "No raw data bucket found"

