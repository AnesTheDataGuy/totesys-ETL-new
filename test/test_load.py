import pytest
import boto3
import os
from moto import mock_aws

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for S3 bucket."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


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


def test_