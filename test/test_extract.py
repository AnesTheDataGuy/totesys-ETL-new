import pytest
import boto3
import os
import shutil
import json
from moto import mock_aws
from src.lambda_functions.extract import lambda_handler, get_secret, compare_csvs
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
data_dir = "./data/table_data/"
check_file_dir = data_dir + "check_s3_file/"
#    if os.path.isfile(f'{data_dir}{table}'):
#        os.remove(f'{data_dir}{table}')

if os.path.isdir(data_dir):
    shutil.rmtree(data_dir)

os.makedirs(data_dir)
os.mkdir(check_file_dir)

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


@pytest.fixture(scope="function")
def read_csv():
    with open("data/table_data/check_s3_file/test_csv1.csv", "r") as reader:
        test_csv_1 = csv.reader(reader)
    with open("data/table_data/check_s3_file/test_csv2.csv", "r") as reader:
        test_csv_2 = csv.reader(reader)
    return test_csv_1, test_csv_2


class DummyContext:  # Dummy context class used for testing
    pass


class TestGetSecret:

    @pytest.mark.it("get secret returns the correct credentials data")
    def test_get_secret_returns_correct_credentials(self, secretsmanager):
        assert get_secret()["user"] == PG_USER
        assert get_secret()["password"] == PG_PASSWORD
        assert get_secret()["host"] == PG_HOST

    @pytest.mark.it(
        "get secret raises an error if secret_name is not in secretsmanager"
    )
    def test_get_secret_failed(self, secretsmanager):
        with pytest.raises(Exception):
            get_secret("imposter_steve")


# class TestCompareCsvs:

#     @pytest.mark.it("Returns a csv file")
#     def test_file_exists(self, read_csv):
#         result = compare_csvs(*read_csv)
#         assert os.path.exists("differences.csv")

#     @pytest.mark.it("Returns a csv file containing changes between the two csvs")
#     def test_change_in_database(self, read_csv):
#         result = compare_csvs(*read_csv)
#         with open("differences.csv", "r") as reader:
#             differences = csv.reader(reader)
#             assert differences == ["11", "12", "13", "14", "15"]

#     @pytest.mark.it("Returns None when both csvs are the same")
#     def test_no_change_in_database(self):
#         with open("data/table_data/check_s3_file/test_csv1.csv", "r") as reader:
#             test_csv_1 = csv.reader(reader)
#         result = compare_csvs(test_csv_1, test_csv_1)
#         assert result is None


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

    # The test below checks files are written locally but we no longer care about it being written locally so it's skipped
    @pytest.mark.skip()
    @pytest.mark.it(
        "script succesfully writes csv files containing database data to local folder"
    )
    def test_succesfully_save_datatables_to_csv(self, s3, secretsmanager):
        saved_csv_path = data_dir
        expected_files = {
            "sales_order.csv": 0,
            "design.csv": 0,
            "currency.csv": 0,
            "staff.csv": 0,
            "counterparty.csv": 0,
            "address.csv": 0,
            "department.csv": 0,
            "purchase_order.csv": 0,
            "payment_type.csv": 0,
            "payment.csv": 0,
            "transaction.csv": 0,
        }
        event = {}
        context = DummyContext()
        lambda_handler(event, context)
        folder_content = os.listdir(saved_csv_path)
        for file in expected_files:
            assert file in folder_content

    @pytest.mark.it(
        "Successfully uploads original files to s3 bucket when bucket is empty"
    )
    def test_uploads_csv_to_raw_data_bucket(self, s3, secretsmanager):
        file_path = "/original/"
        event = {}
        context = DummyContext()
        lambda_handler(event, context)
        listing = s3.list_objects_v2(Bucket="totesys-raw-data-000000")
        assert len(listing["Contents"]) == 11
        expected_files = {
            f"{file_path}sales_order_original.csv": 0,
            f"{file_path}design_original.csv": 0,
            f"{file_path}currency_original.csv": 0,
            f"{file_path}staff_original.csv": 0,
            f"{file_path}counterparty_original.csv": 0,
            f"{file_path}address_original.csv": 0,
            f"{file_path}department_original.csv": 0,
            f"{file_path}purchase_order_original.csv": 0,
            f"{file_path}payment_type_original.csv": 0,
            f"{file_path}payment_original.csv": 0,
            f"{file_path}transaction_original.csv": 0,
        }
        for i in range(len(listing)):
            assert f'{listing["Contents"][i]["Key"]}' in expected_files

    # @pytest.mark.it("Successfully compares new db queries with _original.csv")
    # def test_compares_csv_to_original(self, s3, secretsmanager):
    #     event = {}
    #     context = DummyContext()
    #     assert not lambda_handler(event, context)
