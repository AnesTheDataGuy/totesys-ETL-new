import boto3
import logging
from botocore.exceptions import ClientError
from src.utils.transform_utils import finds_data_buckets

csvs = [
    "sales_order",
    "design",
    "currency",
    "staff",
    "counterparty",
    "address",
    "department",
    "purchase_order",
    "payment_type",
    "payment",
    "transaction",
]

def lambda_handler(event, context):
    """
    This function finds data buckets, converts the csvs to parquet, then uploads this
    to the processed data bucket.

    Args:
        event (dict): time prefix provided by extract function
        context (dict): AWS provided context

    Returns:
        dict: dictionary with time prefix to be used in the load function
    """
    s3_client = boto3.client("s3")

    prefix = event["time_prefix"]

    _, processed_data_bucket = finds_data_buckets()

    for file in csvs:
        parquet = convert_csv_to_parquet(file)

        try:
            s3_client.put_object(
                Body=parquet,
                Bucket=processed_data_bucket,
                Key=f"{prefix}{file}.parquet",
            )

        except ClientError as e:
            logging.error(e)
            return f"Failed to upload file"

    return {"time_prefix": prefix}