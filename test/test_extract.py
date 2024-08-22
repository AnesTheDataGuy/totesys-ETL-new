import pytest
import boto3
import os
import shutil
import json
from moto import mock_aws
from src.lambda_functions.extract import lambda_handler, get_secret
from datetime import datetime as dt
from dotenv import load_dotenv, find_dotenv
import csv

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
load_dotenv(env_file)

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")

year = dt.now().year
month = dt.now().month
day = dt.now().day
hour = dt.now().hour
if len(str(hour)) == 1:
    hour = "0" + str(hour)
minute = dt.now().minute
if len(str(minute)) == 1:
    minute = "0" + str(minute)
second = dt.now().second
if len(str(second)) == 1:
    second = "0" + str(second)

table_data = [
    "payment_type.csv",
    "transaction.csv",
    "currency.csv",
    "payment.csv",
    "sales_order.csv",
    "design.csv",
    "address.csv",
    "counterparty.csv",
    "staff.csv",
    "department.csv",
    "purchase_order.csv",
]
# data_dir = "./data/table_data/"
# check_file_dir = data_dir + "check_s3_file/"
# #    if os.path.isfile(f'{data_dir}{table}'):
# #        os.remove(f'{data_dir}{table}')

# if os.path.isdir(data_dir):
#     shutil.rmtree(data_dir)

# os.makedirs(data_dir)
# os.mkdir(check_file_dir)

"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
BEFORE RUNNING THE TESTS, PLEASE MAKE SURE TO:
1. Run  database/test_db.sql: psql -f database/test_db.sql
2. Make sure that you have testing and development environment configured
    (.env.testing and .env.development)
3. Switch to testing env (export ENV=testing in the CLI)

Switch back to .env.development to use the online database.
"""


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
        yield s3


@pytest.fixture(scope="function")
def s3_no_buckets(aws_credentials):
    """Mocked S3 client with no buckets."""
    with mock_aws():
        s3_nobuckets = boto3.client("s3")
        yield s3_nobuckets


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
            Name="totesys_database_credentials", SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


@pytest.fixture(scope="function")
def secretsmanager_broken(aws_credentials):
    with mock_aws():
        database_dict = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "database": "steve",
            "port": PG_PORT,
        }
        secretsmanager = boto3.client("secretsmanager")
        secretsmanager.create_secret(
            Name="totesys_database_credentials", SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


class DummyContext:  # Dummy context class used for testing
    pass


class TestLambdaHandler:
    # @pytest.mark.skip()
    @pytest.mark.it("Raise exception if raw data bucket is not found")
    def test_bucket_does_not_exist(self, s3_no_buckets, secretsmanager):
        event = {}
        context = DummyContext()
        with pytest.raises(Exception):
            lambda_handler(event, context)

    # @pytest.mark.skip()
    @pytest.mark.it("script succesfully connects to database")
    def test_succesfully_connects_to_database(self, s3, secretsmanager):
        event = {}
        context = DummyContext()
        assert not isinstance(lambda_handler(event, context), Exception)

    @pytest.mark.it(
        "returns exception when failing to connect to database due to wrong credentials"
    )
    def test_fails_to_connect_to_database(self, s3, secretsmanager_broken):
        event = {}
        context = DummyContext()
        with pytest.raises(Exception):
            lambda_handler(event, context)

    
    @pytest.mark.it(
        "Successfully uploads csv files to /source and /history/timepath when /source is empty"
    )
    def test_uploads_csv_to_source_and_history_first_call(self, s3, secretsmanager):
        # UP TO HERE! WE HAVE TO MOCK DATETIME TO CREATE HISTORY DIRS
        path_source = "source/"
        path_history ="history/"
        suffix_source = "_new" 
        suffix_history = "_differences"
        event = {}
        context = DummyContext()
        lambda_handler(event, context)
        listing = s3.list_objects_v2(Bucket="totesys-raw-data-000000")
        assert len(listing["Contents"]) == 11*2
        expected_files_in_source = {
            f"{path_source}sales_order{suffix_source}.csv": 0,
            f"{path_source}design{suffix_source}.csv": 0,
            f"{path_source}currency{suffix_source}.csv": 0,
            f"{path_source}staff{suffix_source}.csv": 0,
            f"{path_source}counterparty{suffix_source}.csv": 0,
            f"{path_source}address{suffix_source}.csv": 0,
            f"{path_source}department{suffix_source}.csv": 0,
            f"{path_source}purchase_order{suffix_source}.csv": 0,
            f"{path_source}payment_type{suffix_source}.csv": 0,
            f"{path_source}payment{suffix_source}.csv": 0,
            f"{path_source}transaction{suffix_source}.csv": 0,
        }
        expected_files_in_history = {
            f"{path_history}sales_order{suffix_history}.csv": 0,
            f"{path_history}design{suffix_history}.csv": 0,
            f"{path_history}currency{suffix_history}.csv": 0,
            f"{path_history}staff{suffix_history}.csv": 0,
            f"{path_history}counterparty{suffix_history}.csv": 0,
            f"{path_history}address{suffix_history}.csv": 0,
            f"{path_history}department{suffix_history}.csv": 0,
            f"{path_history}purchase_order{suffix_history}.csv": 0,
            f"{path_history}payment_type{suffix_history}.csv": 0,
            f"{path_history}payment{suffix_history}.csv": 0,
            f"{path_history}transaction{suffix_history}.csv": 0,
        }
        for i in range(len(listing)):
            assert f'{listing["Contents"][i]["Key"]}' in expected_files_in_source
            assert f'{listing["Contents"][i]["Key"]}' in expected_files_in_history

    # @pytest.mark.it("Successfully compares new db queries with _original.csv")
    # def test_compares_csv_to_original(self, s3, secretsmanager):
    #     event = {}
    #     context = DummyContext()
    #     assert not lambda_handler(event, context)
