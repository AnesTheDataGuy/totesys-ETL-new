import boto3
import logging
from io import StringIO, BytesIO
import polars as pl
from botocore.exceptions import ClientError
from io import BytesIO, StringIO
from pg8000.native import Connection
import json
import pyarrow
from src.utils.queries import star_schema_queries, parquet_to_sql_queries

parquet_tables = [
    'fact_sales_order.parquet'
    'dim_staff.parquet',
    'dim_location.parquet',
    'dim_counterparty.parquet',
    'dim_date.parquet',
    'dim_currency.parquet',
    'dim_design.parquet',
]

def find_processed_data_bucket():
    """
    This function finds the processed data bucket on AWS S3.

    Contains error handling for if the processed data bucket is missing.

    Returns:
        processed_data_bucket (string): string containing full name of the processed data bucket
    """
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    found_processed = False

    for bucket in buckets["Buckets"]:
        if bucket["Name"].startswith("totesys-processed-data-"):
            processed_data_bucket = bucket["Name"]
            found_processed = True
            break

    if not found_processed:
        logging.error("No processed data bucket found")
        return "No processed data bucket found"

    return processed_data_bucket

def get_secret(secret_prefix="totesys-credentials"):
    """
    Initialises a boto3 secrets manager client and retrieves secret from secrets manager
    based on argument given, with the default argument set to the database credentials.
    The secret returned should be a dictionary with 5 keys:
    user - the username for the database
    password - the password for the user
    host - the url of the server hosting the database
    port - which port we are using to connect with the database
    database - the name of the database that we want to connect to
    """   
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name="eu-west-2")
    
    try:
        get_secrets_lists_response = client.list_secrets()
        for secret in get_secrets_lists_response['SecretList']:
            if secret['Name'].startswith(secret_prefix):
                secret_name = secret['Name']
                break
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(e)
        raise Exception(f"Can't retrieve secret due to {e}")

    return json.loads(secret_value_response["SecretString"])

def connect_to_db(credentials):
    """
    Uses the secret obtained in the get_secret method to establish a 
    connection to the database
    """
    return Connection(
        user=credentials["user"],
        password=credentials["password"],
        host=credentials["host"],
        database=credentials["database"],
        port=credentials["port"],
    )

def convert_parquet_to_df(parquet):
    """
    This takes in a parquet file name, finds this file within the processed data bucket then
    converts it to a parquet file in buffer storage.

    Args:
        parquet (string): Name of csv file

    Returns:
        df (DataFrame object): a DataFrame object created from a parquet file conversion
    """
    if not parquet.endswith(".parquet"):
        return f"{parquet} is not a .parquet file."

    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket, Key=parquet
        )  
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)
    # df = pl.read_parquet(res)

    return df

# def create_all_tables():
#     try:
#         credentials = get_secret("totesys_data_warehouse_credentials")
#         db = connect_to_db(credentials)

#         for query in parquet_to_sql_queries:
#             db.run(query)

#     finally:
#         if "db" in locals():
#             db.close()

# def insert_df_into_psql(df, table_name):
    '''
    This function should iterate through a dataframe and 
    insert each row into a psql database using SQL queries. 
    
    Arguments:
        - Dataframe to be uploaded to SQL

    Returns: 
        - Message indicating successful upload
    '''
    try:
    # connect to Date Warehouse
        credentials = get_secret("totesys_data_warehouse_credentials")
        db = connect_to_db(credentials)
    # create table for parquet
        query = f'CREATE TABLE "{table_name}" '
    # Have a for loop that iterates through rows of dataframes
        for row in df.iter_rows():
            query = '''
                INSERT INTO
                '''
            db.run()
        
    # Upload each row using INSERT statement into Data Warehouse
    finally:
        if "db" in locals():
            db.close()

def populate_fact_sales(time_prefix):
    '''
    '''
    # read fact_sales_order.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket, 
            Key=f'/history/{time_prefix}/fact_sales_order.parquet'
        )  
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)

    # reorder columns
    new_order = [
    "sales_order_id", "created_date", "created_time",
    "last_updated_date", "last_updated_time", "staff_id",
    "counterparty_id", "units_sold", "unit_price",
    "currency_id", "design_id", "agreed_payment_date",
    "agreed_delivery_date", "agreed_delivery_location_id"
    ]
    df = df.select(new_order)
    # convert the datetime column to string format
    df = df.with_columns([
        pl.col("created_date").cast(pl.String),
        pl.col("created_time").cast(pl.String),
        pl.col("last_updated_date").cast(pl.String),
        pl.col("last_updated_time").cast(pl.String)
        ])
    # insert into fact_sales_order table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            query = f'''INSERT INTO "fact_sales_order"
                ("sales_order_id", "created_date",
                "created_time", "last_updated_date", "last_updated_time",
                "sales_staff_id", "counterparty_id", "units_sold",
                "unit_price", "currency_id", "design_id",
                "agreed_payment_date", "agreed_delivery_date",
                "agreed_delivery_location_id"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL database successfully populated'
    except ClientError as e:
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()

def populate_dim_staff(time_prefix):
    '''
    '''
    # read dim_staff.parquet from bucket
    s3_client = boto3.client("s3")

    processed_data_bucket = find_processed_data_bucket()
    try:
        res = s3_client.get_object(
            Bucket=processed_data_bucket, 
            Key=f'/history/{time_prefix}/dim_staff.parquet'
        )  
        parquet_data = res["Body"].read()
    except ClientError as e:
        logging.error(f"Failed to get parquet file: {e}")
        return f"Failed to get parquet file: {e}"

    parquet_data_buffer = BytesIO(parquet_data)
    df = pl.read_parquet(parquet_data_buffer)

    # reorder columns
    new_order = [
    "first_name", "last_name", "department_name", "location", "email_address"
    ]
    df = df.select(new_order)

    # insert into fact_sales_order table in data warehouse
    try:
        credentials = get_secret("totesys-data-warehouse-credentials-")
        db = connect_to_db(credentials)
        for row in df.iter_rows():
            query = f'''INSERT INTO "fact_sales_order"
                ("sales_order_id", "created_date",
                "created_time", "last_updated_date", "last_updated_time",
                "sales_staff_id", "counterparty_id", "units_sold",
                "unit_price", "currency_id", "design_id",
                "agreed_payment_date", "agreed_delivery_date",
                "agreed_delivery_location_id"
                )
                VALUES
                {row}
                '''
            db.run(query)
        return 'SQL database successfully populated'
    except ClientError as e:
        raise Exception('Could not update data warehouse')
    finally:
        if "db" in locals():
            db.close()