import boto3, logging, os, csv
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

data_tables = ["sales_order", 
               "design", 
               "currency", 
               "staff", 
               "counterparty", 
               "address", 
               "department", 
               "purchase_order", 
               "payment_type", 
               "payment", 
               "transaction"]

year = dt.now().year
month = dt.now().month
day = dt.now().day
hour = dt.now().hour
minute = dt.now().minute
second = dt.now().second

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    read_file_path_prefix = "./data/"
    save_file_path_prefix = "./data/test_saved_csv/"
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
            f"{read_file_path_prefix}test_db.csv", raw_data_bucket, "test_db.csv" #2024/08/13/HHMMSS/10csvs
        )

        try:
            conn = Connection(
                user=PG_USER, password=PG_PASSWORD, database=DATABASE, port=PORT
            )
            for data_table in data_tables:
                #data type in csv?
                query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{data_table}';"
                column_names = conn.run(query)
                query = f"SELECT * FROM {data_table};"
                data_rows = conn.run(query)
                     
                with open(f"{save_file_path_prefix}test_{data_table}.csv", 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                     #Write the column headers
                    csvwriter.writerow(column_names)
                     #Write the data rows
                    csvwriter.writerows(data_rows)

        except Error as e:
            print(f"Connection to database failed: {e}")

        finally:
            conn.close()
            
        return data_rows
    

    # Execute a query to fetch all rows from the table
    #cursor.execute(f"SELECT * FROM {table_name};")

   

