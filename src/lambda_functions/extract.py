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
from src.utils.extract_utils import *
from pprint import pprint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

data_tables = [
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

"""

history/year/month/day/hh:mm:s/files with no prefix

first time ->
    write source csv

then ->
    compare db query to source csv.
    write history csv
    We dont want to overwrite the old csv >>>>>>>>>>>>>>>>>>>>> overwrite (refresh) source csv


bucket:
    /source/
            sales_order_source.csv
            design_source.csv
            ...
    /history/
            2024/08/16/hh_mm_ss
    OR

    /history/
            sales_order/
            desing_source

    OR
    sales_order/
            source/
                    sales_order/
                    design_source/
                    ...
            history/
                    sales_order/
                    design_source/
"""

def lambda_handler(event, context):
    """
    Wrapper function that allows us to run our utils functions together, and allows 
    us to invoke them all in AWS
    """
    db_credentials = get_secret()
    s3_client = boto3.client("s3")
    raw_data_bucket = connect_to_bucket(s3_client)
    time_prefix = create_time_prefix_for_file()
    bucket_content = s3_client.list_objects(Bucket=raw_data_bucket)
    print(bucket_content)

    if bucket_content.get("Contents"):
        bucket_files = [dict_["Key"] for dict_ in bucket_content["Contents"]]
    else:
        bucket_files = []
    pprint(f"\n <<<{bucket_files}: ")

    try:
        conn = connect_to_db(db_credentials)
        for data_table_name in data_tables:
            print()
            query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{data_table_name}';"
            column_names = conn.run(query)
            header = []
            for column in column_names:
                header.append(column[0])

            query = f"SELECT * FROM {data_table_name};"
            data_rows = conn.run(query)
            file_data = [header] + data_rows

            if not f"/source/{data_table_name}/{data_table_name}_original.csv" in bucket_files:
                create_and_upload_to_bucket(
                    file_data, s3_client, raw_data_bucket, data_table_name, True
                )
                print("\n _ORIGINAL CSV FILES NOT FOUND")
            else:
                print("\n _ORIGINAL CSV FILES FOUND")
                create_and_upload_to_bucket(
                    file_data, s3_client, raw_data_bucket, data_table_name, False
                )
                # file_buffer = StringIO()
                # csv.writer(file_buffer).writerows(file_data)
                
                s3_client.download_file(Bucket=raw_data_bucket, 
                Key=f'/source/{data_table_name}/{data_table_name}_original.csv',
                Filename=f'/tmp/{data_table_name}.csv')

                s3_client.download_file(Bucket=raw_data_bucket, 
                Key=f'/source/{data_table_name}/{data_table_name}_new.csv',
                Filename=f'/tmp/{data_table_name}_new.csv')
                
                changes_csv = compare_csvs(f'/source/{data_table_name}/{data_table_name}_original.csv', f'/source/{data_table_name}/{data_table_name}_new.csv')
                
                s3_client.upload_file(Bucket=raw_data_bucket, Filename=f"/tmp/{changes_csv}", 
                Key=f'/history/{data_table_name}/{changes_csv}')
                
                os.remove(f'/tmp/{data_table_name}.csv')
                os.remove(f'/tmp/{data_table_name}_new.csv')
                # print(f"\n FILE BUFFER: {file_buffer}")
                # file_to_save = bytes(file_to_save.getvalue(), encoding="utf-8")

        logging.info(f"Successfully uploaded raw data to {raw_data_bucket}")

    except Error as e:
        logging.error(e)
        raise Exception(f"Connection to database failed: {e}")

    finally:
        if "conn" in locals():
            conn.close()

    return {"time_prefix": time_prefix}