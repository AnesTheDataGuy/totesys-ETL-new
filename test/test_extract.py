import pytest, boto3, os, shutil
from moto import mock_aws
from src.test_functions.extract_testing import lambda_handler
from datetime import datetime as dt

year = dt.now().year
month = dt.now().month
day = dt.now().day
hour = dt.now().hour
minute = dt.now().minute
second = dt.now().second

table_data = ['payment_type.csv','transaction.csv', 'currency.csv',
'payment.csv', 'sales_order.csv', 'design.csv', 'address.csv',
'counterparty.csv', 'staff.csv', 'department.csv', 'purchase_order.csv']
data_dir = './data/table_data/'
check_file_dir =data_dir+'check_s3_file/'

#for table in table_data:
#    if os.path.isfile(f'{data_dir}{table}'):
#        os.remove(f'{data_dir}{table}')

if os.path.isdir(data_dir):
    shutil.rmtree(data_dir)

os.mkdir(data_dir)
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


class DummyContext: # Dummy context class used for testing
    pass

# @pytest.mark.skip()
@pytest.mark.it("Returns appropriate message if raw data bucket is not found")
def test_bucket_does_not_exist(s3_no_buckets):
    event = {}
    context = DummyContext()
    expected = "No raw data bucket found"
    assert lambda_handler(event, context) == expected

# @pytest.mark.skip()
@pytest.mark.it("script succesfully connects to database")
def test_succesfully_connects_to_database(s3):
    event = {}
    context = DummyContext()
    assert lambda_handler(event, context) != None

# @pytest.mark.skip()
@pytest.mark.it("script succesfully writes csv files containing database data to local folder")
def test_succesfully_save_datatables_to_csv(s3):
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

#@pytest.mark.skip()
@pytest.mark.it("Successfully uploads files with correct time stamp key to s3 bucket")
def test_uploads_csv_to_raw_data_bucket(s3):
    saved_csv_path = check_file_dir
    event = {}
    context = DummyContext()
    res = lambda_handler(event, context)
    listing = s3.list_objects_v2(Bucket="totesys-raw-data-000000")
    time_prefix = f"{year}/{month}/{day}/{hour}-{minute}-{second}/"
    assert len(listing["Contents"]) == 11
    expected_files = {
        f"{time_prefix}sales_order.csv": 0,
        f"{time_prefix}design.csv": 0,
        f"{time_prefix}currency.csv": 0,
        f"{time_prefix}staff.csv": 0,
        f"{time_prefix}counterparty.csv": 0,
        f"{time_prefix}address.csv": 0,
        f"{time_prefix}department.csv": 0,
        f"{time_prefix}purchase_order.csv": 0,
        f"{time_prefix}payment_type.csv": 0,
        f"{time_prefix}payment.csv": 0,
        f"{time_prefix}transaction.csv": 0,
    }
    for i in range(len(listing)):
        assert f'{listing["Contents"][i]["Key"]}' in expected_files
    assert res == f"Successfully uploaded raw data to totesys-raw-data-000000"

    s3.download_file("totesys-raw-data-000000", f"{time_prefix}payment.csv", f"{saved_csv_path}payment.csv")
