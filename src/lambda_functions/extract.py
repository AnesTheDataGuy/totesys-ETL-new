import boto3, logging, os, csv
import json
from datetime import datetime as dt
from pg8000.native import Connection, Error
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv
from io import StringIO

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

year = dt.now().year
month = dt.now().month
day = dt.now().day
hour = dt.now().hour
minute = dt.now().minute
second = dt.now().second

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_secret():

    secret_name = "totesys_database_credentials"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response["SecretString"])
    return secret


def lambda_handler(event, context):
    db_credentials = get_secret()
    db_user = db_credentials["user"]
    db_password = db_credentials["password"]
    db_database = db_credentials["database"]
    db_host = db_credentials["host"]
    db_port = db_credentials["port"]
    save_file_path_prefix = "./data/table_data/"
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            raw_data_bucket = bucket["Name"]
            found = True
            break

    if not found:
        logging.error("No raw data bucket found")
        return "No raw data bucket found"

    time_prefix = f"{year}/{month}/{day}/{hour}:{minute}:{second}/"

    try:
        conn = Connection(
            user=db_user,
            password=db_password,
            host=db_host,
            database=db_database,
            port=db_port,
        )

        for data_table in data_tables:
            query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{data_table}';"
            column_names = conn.run(query)
            header = []
            for column in column_names:
                header.append(column[0])

            query = f"SELECT * FROM {data_table};"
            data_rows = conn.run(query)

            file_to_save = StringIO()
            file_data = [header] + data_rows
            csv.writer(file_to_save).writerows(file_data)
            file_to_save = bytes(file_to_save.getvalue(), encoding="utf-8")

            try:
                response = s3_client.put_object(
                    Body=file_to_save,
                    Bucket=raw_data_bucket,
                    Key=f"{time_prefix}{data_table}.csv",
                )

            except ClientError as e:
                logging.error(e)
                return f"Failed to upload file"

        logging.info(f"Successfully uploaded raw data to {raw_data_bucket}")

    except Error as e:
        logging.error(e["M"])
        return f"Connection to database failed: {e['M']}"

    finally:
        conn.close()

    return {"time_prefix": time_prefix}
