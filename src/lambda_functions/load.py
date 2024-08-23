from src.utils.load_utils import (
    find_processed_data_bucket, 
    get_secret, 
    connect_to_db,
    convert_parquet_to_df
)
import boto3
import logging
import csv
import json
import re
import subprocess
import os
from datetime import datetime as dt
from pg8000.native import Connection, Error
from botocore.exceptions import ClientError
from io import StringIO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


parquet_tables = [
    "sales_order.parquet",
    "design.parquet",
    "currency.parquet",
    "staff.parquet",
    "counterparty.parquet",
    "address.parquet",
    "department.parquet",
    "purchase_order.parquet",
    "payment_type.parquet",
    "payment.parquet",
    "transaction.parquet",
]


def lambda_handler(event, context):

    ## find latest timestamped folder in history folder 
    ## using 'time_prefix'
    ## convert parquet to dataframe
    ## make changes in line with star schema
    ## convert back to parquet
    credentials = get_secret("totesys_data_warehouse_credentials")
    s3_client = boto3.client("s3")
    time_prefix = event['time_prefix']
    table_dfs = {}

    try:
        conn = connect_to_db(credentials)
        for parquet in parquet_tables:
            file_path = f'/history/{time_prefix}/{parquet}'
            table_dfs[parquet] = convert_parquet_to_df(file_path)
            # call insert_df_into_psql here

    finally:
        conn.close()

lambda_handler({'time_prefix': '2024_8_21_15:15:14'}, 0)