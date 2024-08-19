import boto3
import logging
import csv
import json
from datetime import datetime as dt
from pg8000.native import Connection, Error
from botocore.exceptions import ClientError
from io import StringIO
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
    overwrite (refresh) source csv


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

all_data_file_path = "/source/"

def create_time_prefix_for_file():
    current_time = dt.now()
    year = current_time.year
    month = current_time.month
    day = current_time.day
    hour = current_time.hour
    if len(str(hour)) == 1:
        hour = "0"+str(hour)
    minute = current_time.minute
    if len(str(minute)) == 1:
        minute = "0"+str(minute)
    second = current_time.second
    if len(str(second)) == 1:
        second = "0"+str(second)
    return f"{year}_{month}_{day}_{hour}:{minute}:{second}_"


def get_secret(secret_name="totesys_database_credentials"):
    # Create a Secrets Manager client
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


def connect_to_bucket(client):
    buckets = client.list_buckets()
    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            return bucket["Name"]
    logging.error("No raw data bucket found")
    raise Exception("No raw data bucket found")          

def connect_to_db(credentials):
    return Connection(
            user=credentials["user"],
            password=credentials["password"],
            host=credentials["host"],
            database=credentials["database"],
            port=credentials["port"]
        )

def create_and_upload_to_bucket(data,client,bucket,filename):
    file_to_save = StringIO()            
    csv.writer(file_to_save).writerows(data)
    file_to_save = bytes(file_to_save.getvalue(), encoding="utf-8")

    try:
        response = client.put_object(
            Body=file_to_save,
            Bucket=bucket,
            Key=f"{all_data_file_path}{filename}_original.csv",
        )

    except ClientError as e:
        logging.error(e)
        raise Exception("Failed to upload file")

def lambda_handler(event, context):
    db_credentials = get_secret()
    s3_client = boto3.client("s3")
    raw_data_bucket = connect_to_bucket(s3_client)
    time_prefix = create_time_prefix_for_file()
    bucket_content = s3_client.list_objects(Bucket=raw_data_bucket)
    pprint(bucket_content)
   
    if bucket_content.get('Contents'):
        bucket_files = [dict_['Key'] for dict_ in bucket_content['Contents']]
    else:
        bucket_files = []
    print(f"\n <<<bucket_files: ")
    try:
        conn = connect_to_db(db_credentials)
        for data_table_name in data_tables:
            query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{data_table_name}';"
            column_names = conn.run(query)
            header = []
            for column in column_names:
                header.append(column[0])

            query = f"SELECT * FROM {data_table_name};"
            data_rows = conn.run(query)
            file_data = [header] + data_rows

            if not f"{data_table_name}_original.csv" in bucket_files:              
                create_and_upload_to_bucket(file_data,s3_client,raw_data_bucket,data_table_name)
                print("\n _ORIGINAL CSV FILES NOT FOUND")
            else:
                print("\n _ORIGINAL CSV FILES FOUND")
                file_buffer = StringIO()            
                csv.writer(file_buffer).writerows(file_data)
                print(f"\n FILE BUFFER: {file_buffer}")
                #file_to_save = bytes(file_to_save.getvalue(), encoding="utf-8")
                
        logging.info(f"Successfully uploaded raw data to {raw_data_bucket}")

    except Error as e:
        logging.error(e)
        raise Exception(f"Connection to database failed: {e}")

    finally:
        if 'conn' in locals():
            conn.close()

    return {"time_prefix": time_prefix}