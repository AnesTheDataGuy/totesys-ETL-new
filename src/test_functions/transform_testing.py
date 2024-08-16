import boto3, logging, os, csv
from botocore.exceptions import ClientError
from io import StringIO
import pandas as pd

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
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found_processed = False
    found_raw = False
    print(1)
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

    prefix = event["time_prefix"]

    csv_dfs = {}

    for table in csvs:
        res = s3_client.get_object(Bucket=raw_data_bucket, Key=prefix + table)
        csv_data = res["Body"].read().decode("utf-8")
        data_buffer = StringIO(csv_data)
        df = pd.read_csv(data_buffer)
        csv_dfs[table] = df

    print(csv_dfs)
