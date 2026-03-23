import json
import logging
import os
import urllib.parse
from datetime import datetime, UTC
import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def get_partition_path(date_obj):
    """Generate silver partition path based on the date."""
    return f"silver/crypto/year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}"

def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event))
    
    # Ideally, this lambda is triggered by an S3 event from the bronze layer.
    # We will extract the bucket and key from the event payload.
    # If it's invoked manually without S3 event structure, we fall back to today's date.
    
    source_bucket = None
    source_key = None
    
    if "Records" in event and len(event["Records"]) > 0:
        record = event["Records"][0]
        if "s3" in record:
            source_bucket = record["s3"]["bucket"]["name"]
            source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
            
    if not source_bucket or not source_key:
        # Fallback for manual/scheduled invocation
        now = datetime.now(UTC)
        source_bucket = "crypto-analytics-pipeline"
        source_key = f"bronze/crypto/year={now.year}/month={now.month:02d}/day={now.day:02d}/data.json"
        logger.info(f"No S3 event found, falling back to: s3://{source_bucket}/{source_key}")
    
    logger.info(f"Reading from s3://{source_bucket}/{source_key}")
    
    # Download JSON from S3 to memory
    try:
        response = s3.get_object(Bucket=source_bucket, Key=source_key)
        raw_data = json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error reading object {source_key} from bucket {source_bucket}. Ensure it exists.")
        raise e
        
    df = pd.DataFrame(raw_data)
    
    if df.empty:
        logger.warning("Dataframe is empty. Nothing to transform.")
        return {"statusCode": 200, "body": "Empty data."}
    
    # Add partition column
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")
    df["date"] = date_str
    
    # Save parquet to Lambda's /tmp/ directory
    tmp_parquet_file = "/tmp/crypto.parquet"
    logger.info(f"Writing parquet to {tmp_parquet_file}")
    
    df.to_parquet(
        tmp_parquet_file,
        engine="fastparquet",
        index=False
    )
    
    # Generate S3 key for Silver layer
    partition_path = get_partition_path(datetime.now(UTC))
    silver_key = f"{partition_path}/crypto.parquet"
    
    logger.info(f"Uploading parquet to s3://{source_bucket}/{silver_key}")
    
    try:
        s3.upload_file(tmp_parquet_file, source_bucket, silver_key)
        logger.info("Upload to Silver layer successful.")
    except Exception as e:
        logger.error(f"Failed to upload parquet: {e}")
        raise e
        
    # Optional check: clean up /tmp space
    if os.path.exists(tmp_parquet_file):
        os.remove(tmp_parquet_file)
        
    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Successfully transformed and uploaded to {silver_key}"})
    }
