import boto3
import logging
from botocore.exceptions import ClientError
from src.utils.transform_utils import finds_data_buckets, convert_csv_to_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

csvs = [
    "sales_order.csv",
    "design.csv",
    "currency.csv",
    "staff.csv",
    "counterparty.csv",
    "address.csv",
    "department.csv",
    "purchase_order.csv",
    "payment_type.csv",
    "payment.csv",
    "transaction.csv",
]


def lambda_handler(event, context):
    """
    This function finds data buckets, converts the csvs to parquet, then uploads this
    to the processed data bucket.

    Args:
        event (dict): time prefix provided by extract function
        context (dict): AWS provided context

    Returns:
        dict: dictionary with time prefix to be used in the load function
    """

    logger.info("Lambda function invoked.")
    logger.info(f"Event received: {event}")

    s3_client = boto3.client("s3")

    prefix = event["time_prefix"]
    logger.info(f"Processing files with time prefix: {prefix}")

    _, processed_data_bucket = finds_data_buckets()
    logger.info(f"Found processed data bucket: {processed_data_bucket}")

    for file in csvs:
        logger.info(f"Starting processing for file: {file}")
        try:
            parquet = convert_csv_to_parquet(file)
            file = file[:-4]

            if parquet is None:
                logger.info("{file} only contains headers, continue....")
                continue
            else:
                logger.info(f"{file} converted to parquet")

                s3_client.put_object(
                    Body=parquet,
                    Bucket=processed_data_bucket,
                    Key=f"history/{prefix}{file}.parquet",
                )
                logger.info(f"Successfully uploaded {file}.parquet to {processed_data_bucket}")

        except ClientError as e:
            logger.error(f"Failed to upload file {file}: {e}")
    
    logger.info("All files processed and uploaded successfully.")
    logger.info("Lambda function completed.")

    return {"time_prefix": prefix}
