import logging
import time
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pipeline_app.config import get_settings

logger = logging.getLogger(__name__)

def build_s3_client():
    settings = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def ensure_bucket_exists(max_retries: int = 20, delay_seconds: int = 2) -> None:
    settings = get_settings()
    client = build_s3_client()
    for attempt in range(1, max_retries + 1):
        try:
            client.head_bucket(Bucket=settings.minio_bucket)
            logger.info("minio bucket %s already exists", settings.minio_bucket)
            return
        except ClientError:
            try:
                client.create_bucket(Bucket=settings.minio_bucket)
                logger.info("minio bucket %s created", settings.minio_bucket)
                return
            except ClientError as error:
                logger.info("waiting for minio bucket create attempt %s/%s: %s", attempt, max_retries, error)
        except Exception as error:
            logger.info("waiting for minio attempt %s/%s: %s", attempt, max_retries, error)
        time.sleep(delay_seconds)
    raise RuntimeError(f"minio bucket {settings.minio_bucket} was not ready in time")