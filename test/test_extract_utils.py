import pytest
import boto3
import os
import json
import csv
from moto import mock_aws
from src.utils.extract_utils import *
from dotenv import load_dotenv, find_dotenv


env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
load_dotenv(env_file)

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
AWS_SECRET = os.getenv("AWS_SECRET")

print(f"\n <><><><> {PG_USER} ,{AWS_SECRET}")


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
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        """,
            Bucket="totesys-raw-data-000000",
            Key="test_csv_extra_rows.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@nc.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket="totesys-raw-data-000000",
            Key="test_csv_extra_rows_new.csv",
        )

        yield s3


@pytest.fixture(scope="function")
def s3_no_dt_changes(aws_credentials):
    """Mocked S3 client with raw data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@nc.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket="totesys-raw-data-000000",
            Key="test_csv_no_diff.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@nc.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket="totesys-raw-data-000000",
            Key="test_csv_no_diff_new.csv",
        )

        yield s3


@pytest.fixture(scope="function")
def s3_rows_edited(aws_credentials):
    """Mocked S3 client with raw data bucket."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@nc.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket="totesys-raw-data-000000",
            Key="test_csv_edited_rows.csv",
        )

        s3.put_object(
            Body="""staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
        1,John,Doe,1,john.doe@this_has_been_edited.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        2,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        3,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        4,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        5,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        6,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        7,John,Doe,1,john.doe@example.com,2023-08-10 08:00:00,2023-08-10 08:00:00
        8,Jane,Smith,2,jane.smith@example.com,2023-08-11 09:15:00,2023-08-11 09:15:00
        9,Robert,Johnson,3,robert.johnson@example.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        10,Steve,Imposter,1,steve_imposter@nc.com,2023-08-12 10:30:00,2023-08-12 10:30:00
        11,Stevie,Impostah,1,stevie_impostah@kastriot.com,2024-08-12 10:30:00,2024-08-12 10:30:00""",
            Bucket="totesys-raw-data-000000",
            Key="test_csv_edited_rows_new.csv",
        )

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
            Name=AWS_SECRET, SecretString=json.dumps(database_dict)
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
            Name=AWS_SECRET, SecretString=json.dumps(database_dict)
        )
        yield secretsmanager


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


class TestCompareCsvs:
    # @pytest.mark.skip()
    @pytest.mark.it("Creates a csv file")
    def test_file_exists(self, s3, secretsmanager):
        s3.download_file(
            "totesys-raw-data-000000",
            "test_csv_extra_rows.csv",
            "/tmp/test_csv_extra_rows.csv",
        )
        s3.download_file(
            "totesys-raw-data-000000",
            "test_csv_extra_rows_new.csv",
            "/tmp/test_csv_extra_rows_new.csv",
        )

        compare_csvs("test_csv_extra_rows")
        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_extra_rows_differences")
        ]

        assert len(csv_path_list) > 0

    # @pytest.mark.skip()
    @pytest.mark.it(
        "Creates a csv file containing changes between the two csvs (new dt has extra rows)"
    )
    def test_change_in_datatable_extra_rows(self, s3, secretsmanager):
        s3.download_file(
            "totesys-raw-data-000000",
            "test_csv_extra_rows.csv",
            "/tmp/test_csv_extra_rows.csv",
        )
        s3.download_file(
            "totesys-raw-data-000000",
            "test_csv_extra_rows_new.csv",
            "/tmp/test_csv_extra_rows_new.csv",
        )

        compare_csvs("test_csv_extra_rows")
        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_extra_rows_differences")
        ]
        print(csv_path_list)
        with open(f"/tmp/{csv_path_list[0]}", "r") as reader:
            next(reader)
            differences = csv.reader(reader)

            assert list(differences) == [
                [
                    "10",
                    "Steve",
                    "Imposter",
                    "1",
                    "steve_imposter@nc.com",
                    "2023-08-12 10:30:00",
                    "2023-08-12 10:30:00",
                ],
                [
                    "11",
                    "Stevie",
                    "Impostah",
                    "1",
                    "stevie_impostah@nc.com",
                    "2024-08-12 10:30:00",
                    "2024-08-12 10:30:00",
                ],
            ]

    # @pytest.mark.skip()
    @pytest.mark.it("Writes empty csv file when both csvs are the same")
    def test_no_change_in_database(self, s3_no_dt_changes, secretsmanager):
        s3_no_dt_changes.download_file(
            "totesys-raw-data-000000",
            "test_csv_no_diff.csv",
            "/tmp/test_csv_no_diff.csv",
        )
        s3_no_dt_changes.download_file(
            "totesys-raw-data-000000",
            "test_csv_no_diff_new.csv",
            "/tmp/test_csv_no_diff_new.csv",
        )

        compare_csvs("test_csv_no_diff")
        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_no_diff_differences")
        ]
        with open(f"/tmp/{csv_path_list[0]}", "r") as reader:
            next(reader)
            differences = csv.reader(reader)
            assert list(differences) == []

    # @pytest.mark.skip()
    @pytest.mark.it(
        "Creates a csv file containing changes between the two csvs (new dt has edited rows)"
    )
    def test_change_in_datatable_edited_rows(self, s3_rows_edited, secretsmanager):
        s3_rows_edited.download_file(
            "totesys-raw-data-000000",
            "test_csv_edited_rows.csv",
            "/tmp/test_csv_edited_rows.csv",
        )
        s3_rows_edited.download_file(
            "totesys-raw-data-000000",
            "test_csv_edited_rows_new.csv",
            "/tmp/test_csv_edited_rows_new.csv",
        )

        compare_csvs("test_csv_edited_rows")
        csv_path_list = [
            filename
            for filename in os.listdir("/tmp")
            if filename.startswith("test_csv_edited_rows_differences")
        ]
        print(csv_path_list)
        with open(f"/tmp/{csv_path_list[0]}", "r") as reader:
            next(reader)
            differences = csv.reader(reader)

            assert list(differences) == [
                [
                    "1",
                    "John",
                    "Doe",
                    "1",
                    "john.doe@this_has_been_edited.com",
                    "2023-08-10 08:00:00",
                    "2023-08-10 08:00:00",
                ],
                [
                    "11",
                    "Stevie",
                    "Impostah",
                    "1",
                    "stevie_impostah@kastriot.com",
                    "2024-08-12 10:30:00",
                    "2024-08-12 10:30:00",
                ],
            ]
