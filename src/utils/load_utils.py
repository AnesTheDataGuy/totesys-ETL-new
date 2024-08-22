import boto3
import logging
from io import StringIO, BytesIO
import polars as pl
from botocore.exceptions import ClientError
from io import BytesIO
from pg8000.native import Connection
import json


def find_processed_data_bucket():
    """
    This function finds the processed data bucket on AWS S3.

    Contains error handling for if the processed data bucket is missing.

    Returns:
        processed_data_bucket (string): string containing full name of the processed data bucket
    """
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found_processed = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-processed-data-"):
            processed_data_bucket = bucket["Name"]
            found_processed = True
            break

    if not found_processed:
        logging.error("No processed data bucket found")
        return "No processed data bucket found"

    return processed_data_bucket

def get_secret(secret_name):
    """
    Initialises a boto3 secrets manager client and retrieves secret from secrets manager
    based on argument given, with the default argument set to the database credentials.
    The secret returned should be a dictionary with 5 keys:
    user - the username for the database
    password - the password for the user
    host - the url of the server hosting the database
    port - which port we are using to connect with the database
    database - the name of the database that we want to connect to
    """
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="eu-west-2")

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        logging.error(e)
        raise Exception(f"Can't retrieve secret due to {e}")

    return json.loads(get_secret_value_response["SecretString"])


def connect_to_db(credentials):
    """
    Uses the secret obtained in the get_secret method to establish a 
    connection to the database
    """
    return Connection(
        user=credentials["user"],
        password=credentials["password"],
        host=credentials["host"],
        database=credentials["database"],
        port=credentials["port"],
    )

def convert_parquet_to_df(parquet):
    """
    This takes in a parquet file name, finds this file within the processed data bucket then
    converts it to a parquet file in buffer storage.

    Args:
        parquet (string): Name of csv file

    Returns:
        df (DataFrame object): a DataFrame object created from a parquet file conversion
    """
    if not parquet.endswith(".parquet"):
        return f"{parquet} is not a .parquet file."

    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket, Key=parquet
        )  # change f string for when we finalise extract structure
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)

    return df