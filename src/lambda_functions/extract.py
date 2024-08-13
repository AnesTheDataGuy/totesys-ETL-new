# import boto3, logging, os
# from datetime import datetime as dt
# from pg8000.native import Connection
# from botocore.exceptions import ClientError
# from dotenv import load_dotenv

# load_dotenv()

# PG_USER = os.getenv("PG_USER")
# PG_PASSWORD = os.getenv("PG_PASSWORD")
# DATABASE = os.getenv("PG_DATABASE")
# HOST = os.getenv("PG_HOST")
# PORT = os.getenv("PG_PORT")

# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# def lambda_handler(event, context):
#     test_file_path = "./data/test_db.json"


#     s3_client = boto3.client("s3")
#     buckets = s3_client.list_buckets()

#     #if statement to check buckets
#     for bucket in buckets['Buckets']:

#         if bucket['Name'].startswith("totesys-raw-data-"):
#             raw_data_bucket = bucket['Name']
#             break 
    
#     if raw_data_bucket:
#         response = s3_client.upload_file(test_file_path, raw_data_bucket, "test_db.json")
        
#         conn = Connection(
#             user= PG_USER,
#             password= PG_PASSWORD,
#             database= DATABASE,
#             host= HOST,
#             port= PORT
#         )
        
def extract():
    pass