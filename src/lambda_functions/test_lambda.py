import logging
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    buckets = s3_client.list_buckets()
    for bucket in buckets:
        if bucket.startswith("testing-bucket"):
            my_bucket = bucket
    logger.info(my_bucket)
