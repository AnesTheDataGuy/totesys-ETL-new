# import pytest, boto3, os
# from moto import mock_aws
# from src.lambda_functions.extract import lambda_handler

# @pytest.fixture(scope="function")
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "test"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
#     os.environ["AWS_SECURITY_TOKEN"] = "test"
#     os.environ["AWS_SESSION_TOKEN"] = "test"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

# @pytest.fixture(scope="function")
# def s3(aws_credentials):
#     with mock_aws():
#         s3 = boto3.client("s3")

#         yield s3

# class DummyContext:
#     pass


# #@pytest.mark.it("script succesfully finds database bucket")
# #def test_connects_to_database_bucket():
# #    assert lambda_handler()

# def test_succesfully_upload_json_file(s3):
#     s3.create_bucket(
#     Bucket="totesys-raw-data-000000",
#     CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#         )
#     event = {}
#     context = DummyContext()
#     lambda_handler(event,context)
#     listing = s3.list_objects_v2(Bucket="totesys-raw-data-000000")
#     assert len(listing["Contents"]) == 1
#     assert listing["Contents"][0]["Key"] == "test_db.json"

# def test_bucket_does_not_exist(s3): #rewrite
#     event = {}
#     context = DummyContext()
#     lambda_handler(event,context)
#     listing = s3.list_buckets()
#     print(listing)
