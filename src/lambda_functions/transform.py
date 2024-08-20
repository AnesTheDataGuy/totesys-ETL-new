import boto3
import logging
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import polars as pl

csvs = [
    "sales_order.csv",
    "design.csv",
    "currency.csv",
    "staff.csv",
    "counterparty.csv",
    "address.csv",
    "department.csv",
    "purchase_order.csv",
    "payment_type.csv",
    "payment.csv",
    "transaction.csv",
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
        file = file[:-4]
        try:
            s3_client.put_object(
                Body=parquet,
                Bucket=processed_data_bucket,
                Key=f"/history/{prefix}/{file}.parquet",
            )

        except ClientError as e:
            logging.error(e)
            return f"Failed to upload file"

    return {"time_prefix": prefix}


def finds_data_buckets():
    """
    This function finds the raw data and processed data buckets on AWS S3.

    Contains error handling for if either or both data buckets are missing.

    Returns:
        raw_data_bucket (string): string containing full name of the raw data bucket
        processed_data_bucket (string): string containing full name of the processed data bucket
    """
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found_processed = False
    found_raw = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            raw_data_bucket = bucket["Name"]
            found_raw = True
        if bucket["Name"].startswith("totesys-processed-data-"):
            processed_data_bucket = bucket["Name"]
            found_processed = True
        if found_raw and found_processed:
            break

    if not found_raw and not found_processed:
        logging.error("No buckets found")
        return "No buckets found"
    elif not found_raw:
        logging.error("No raw data bucket found")
        return "No raw data bucket found"
    elif not found_processed:
        logging.error("No processed data bucket found")
        return "No processed data bucket found"

    return raw_data_bucket, processed_data_bucket


def convert_csv_to_parquet(csv):
    """
    This takes in a csv file name, finds this file within the raw data bucket then
    converts it to a parquet file in buffer storage.

    Args:
        csv (string): Name of csv file

    Returns:
        parquet (string): This string contains parquet file data converted from csv format.
    """
    if csv[-4:] != ".csv":
        return f"{csv} is not a .csv file."

    s3_client = boto3.client("s3")

    raw_data_bucket, _ = finds_data_buckets()
    try:
        res = s3_client.get_object(
            Bucket=raw_data_bucket, Key=f"{csv}"
        )  # change f string for when we finalise extract structure
        csv_data = res["Body"].read().decode("utf-8")
    except ClientError as e:
        return "csv file not found"

    data_buffer_csv = StringIO(csv_data)
    df = pl.read_csv(data_buffer_csv)

    data_buffer_parquet = BytesIO()
    parquet = df.write_parquet(data_buffer_parquet)
    data_buffer_parquet.seek(0)

    parquet = data_buffer_parquet.read()

    return parquet
