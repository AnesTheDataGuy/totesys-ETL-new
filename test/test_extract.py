import pytest, boto3, os
from moto import mock_aws
from src.lambda_functions.extract import lambda_handler


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-raw-data-000000",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3

@pytest.fixture(scope="function")
def s3_no_buckets(aws_credentials):
    with mock_aws():
        s3_nobuckets = boto3.client("s3")       
        yield s3_nobuckets

class DummyContext:
    pass

@pytest.mark.skip()
def test_succesfully_upload_json_file(s3):
    event = {}
    context = DummyContext()
    lambda_handler(event, context)
    listing = s3.list_objects_v2(Bucket="totesys-raw-data-000000")
    assert len(listing["Contents"]) == 1
    assert listing["Contents"][0]["Key"] == "test_db.csv"

@pytest.mark.skip()
def test_bucket_does_not_exist(s3_no_buckets):
    event = {}
    context = DummyContext()
    expected = "No raw data bucket found"
    assert lambda_handler(event, context) == expected
    

@pytest.mark.skip()
@pytest.mark.it("script succesfully connects to database")
def test_succesfully_connects_to_database(s3):
    event = {}
    context = DummyContext()
    assert lambda_handler(event, context) != None

@pytest.mark.it("script succesfully writes csv files containing database data to local folder")
def test_succesfully_save_datatables_to_csv(s3):
    saved_csv_path = "./data/test_saved_csv/"
    expected_files = ["test_sales_order.csv", 
               "test_design.csv", 
               "test_currency.csv", 
               "test_staff.csv", 
               "test_counterparty.csv", 
               "test_address.csv", 
               "test_department.csv", 
               "test_purchase_order.csv", 
               "test_payment_type.csv", 
               "test_payment.csv", 
               "test_transaction.csv"]
    event = {}
    context = DummyContext()
    lambda_handler(event, context)
    folder_content = os.listdir(saved_csv_path)
    for file in expected_files:
        assert file in folder_content
