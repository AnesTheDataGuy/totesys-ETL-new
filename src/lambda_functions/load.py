from src.utils.load_utils import (
    find_processed_data_bucket, 
    get_secret, 
    connect_to_db,
    convert_parquet_to_df,
    populate_fact_sales
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


def lambda_handler(event, context):

    # before we load we need to drop rows from tables in
    # data warehouse

    ## find latest timestamped folder in history folder 
    ## using 'time_prefix'
    time_prefix = event['time_prefix']
    parquet_file_path = f'/history/{time_prefix}/'

    ## connect to data warehouse
    credentials = get_secret("totesys-data-warehouse-credentials-")
    
    try:
        conn = connect_to_db(credentials)
            populate_fact_sales()

            logging.info(f'Load complete for {table}')

    except ClientError as e:
            logging.error(e)
            return "Failed to insert data into warehouse"
    
    finally:
        if "conn" in locals():
            conn.close()
    
    ## loop through all parquet files

    ## convert parquet to dataframe


    # try:
    #     conn = connect_to_db(credentials)
    #     for parquet in parquet_tables:
    #         file_path = f'/history/{time_prefix}/{parquet}'
    #         table_dfs[parquet] = convert_parquet_to_df(file_path)
    #         # call insert_df_into_psql here

    # finally:
    #     conn.close()
