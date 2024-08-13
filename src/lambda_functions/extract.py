import boto3, logging, os
from datetime import datetime as dt
from pg8000.native import Connection, Error
from botocore.exceptions import ClientError
from dotenv import load_dotenv,  find_dotenv

env_file = find_dotenv(f'.env.{os.getenv("ENV")}')
load_dotenv(env_file)


PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
DATABASE = os.getenv("PG_DATABASE")
HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")

year = dt.now().year
month = dt.now().month
day = dt.now().day
hour = dt.now().hour
minute = dt.now().minute
second = dt.now().second

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
print(f"\n<<< {PG_USER}")

def lambda_handler(event, context):
    file_path_prefix = "./data/"

    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found = False
    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-raw-data-"):
            raw_data_bucket = bucket["Name"]
            found = True
            break
    if not found:
        return "No raw data bucket found"
    
    if raw_data_bucket:
        
        prefix = f'{year}/{month}/{day}/{hour}-{minute}-{second}/'

        response_upload = s3_client.upload_file(
            f"{file_path_prefix}test_db.csv", raw_data_bucket, "test_db.csv" #2024/08/13/HHMMSS/10csvs
        )

        try:
            conn = Connection(
                user=PG_USER, password=PG_PASSWORD, database=DATABASE, port=PORT
            )
            query = "SELECT * FROM currency;"
            response_db = conn.run(query)
            print(response_db.text)

        except Error as e:
            print(f"Connection to database failed: {e}")

        finally:
            conn.close()
            
        return response_db