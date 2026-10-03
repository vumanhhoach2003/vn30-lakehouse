
import io
import os
import boto3
import logging
from datetime import datetime

#Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get MinIO endpoint from environment or default to localhost
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")

# MinIO connection settings
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

BUCKET = "bronze"
TODAY  = datetime.today().strftime("%Y-%m-%d")

def ensure_bucket_exists(bucket_name):
    """Checks if a bucket exists, and creates it if it doesn't."""
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        logger.info(f"Bucket '{bucket_name}' not found. Creating it...")
        s3.create_bucket(Bucket=bucket_name)

# Auto-ensure the bronze bucket exists when this module is imported
ensure_bucket_exists(BUCKET)


def upload_parquet(df, s3_key):
    """
    Upload a DataFrame to MinIO as a Parquet file.
    Note: coerce_timestamps='ms' is used to ensure compatibility with Spark 3.x.
    """
    if 'ingested_at' in df.columns:
        df['ingested_at'] = df['ingested_at'].astype('datetime64[ms]')

    # Convert DataFrame to Parquet format in memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', coerce_timestamps='ms', allow_truncated_timestamps=True)
    
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(f"Uploaded Parquet -> s3://{BUCKET}/{s3_key}")


