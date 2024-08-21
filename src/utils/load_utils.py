import boto3
import logging
from io import StringIO, BytesIO
import polars as pl
from botocore.exceptions import ClientError
from io import BytesIO


def finds_data_bucket():
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


def read_parquet(bytes_io):
    """
    This function reads a parquet file from a BytesIO object into a DataFrame.

    Args:
        bytes_io (BytesIO): BytesIO object containing the parquet file data

    Returns:
        df (pl.DataFrame): DataFrame containing the data from the parquet file
    """
    try:
        df = pl.read_parquet(bytes_io)
        return df
    except Exception as e:
        logging.error(f"Failed to read parquet file: {e}")
        return e



