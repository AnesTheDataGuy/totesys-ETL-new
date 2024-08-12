import boto3, logging, os
from datetime import datetime as dt
from pg8000.native import Connection
from dotenv import load_dotenv

load_dotenv(override=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    test_file_path = "./data/test_db.json"

    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    #print(buckets)
    for bucket in buckets['Buckets']:
        #print(f"\n<<< bucket: {bucket}")
        if bucket['Name'].startswith("totesys-raw-data-"):
            #print(f"<<< found")
            raw_data_bucket_name = bucket['Name']
            break 
    print(f"<<< found bucket: {raw_data_bucket_name}")
           
    if raw_data_bucket_name:
        response = s3_client.upload_file(test_file_path, raw_data_bucket_name, "test_db.json")
        
        conn = Connection(
            user = os.getenv("user"),
            password=os.getenv("password"),
            database=os.getenv("database"),
            host=os.getenv("host"),
            port=os.getenv("port")
        )
        
