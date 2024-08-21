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
        s3.put_object(Body=
        """staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
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
        Key='test_csv.csv'
        )
        
        s3.put_object(Body=
        """staff_id,first_name,last_name,department_id,email_address,created_at,last_updated
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
        Key='test_csv_new.csv'
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

    @pytest.mark.it("Creates a csv file")
    def test_file_exists(self, s3, secretsmanager):
        s3.download_file("totesys-raw-data-000000", 'test_csv.csv', '/tmp/test_csv.csv')
        s3.download_file("totesys-raw-data-000000", 'test_csv_new.csv', '/tmp/test_csv_new.csv')  
        
        result = compare_csvs('test_csv')
        csv_path_list = [filename for filename in os.listdir('/tmp') if filename.startswith('test_csv_differences')]
        
        assert len(csv_path_list) > 0
    
    
    @pytest.mark.it(
            "Creates a csv file containing changes between the two csvs"
            )
    def test_change_in_database(self):
        print(f"\n ><><><>< {os.listdir('/tmp')}")
        csv_path_list = [filename for filename in os.listdir('/tmp') if filename.startswith('test_csv_differences')]
        print(csv_path_list)
        with open(f'/tmp/{csv_path_list[0]}', 'r') as reader:
            next(reader)
            differences = csv.reader(reader)

            assert list(differences) == [
                ['10', 'Steve', 'Imposter', '1', 'steve_imposter@nc.com',
                '2023-08-12 10:30:00', '2023-08-12 10:30:00 '
                ],
                ['11', 'Stevie', 'Impostah', '1', 'stevie_impostah@nc.com',
                 '2024-08-12 10:30:00', '2024-08-12 10:30:00 '
                ]
            ]
            
    """

    @pytest.mark.it("Writes empty csv file when both csvs are the same")
    def test_no_change_in_database(self, secretsmanager):
        result = compare_csvs('test_csv_2.csv', 'test_csv_2.csv')
        csv_path_list = [filename for filename in os.listdir('.') if filename.startswith('test_csv_2.csv_differences_')]
        with open(csv_path_list[0], 'r') as reader:
            next(reader)
            differences = csv.reader(reader)
            assert list(differences) == []
"""