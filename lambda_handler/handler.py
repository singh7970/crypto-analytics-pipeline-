import json
import logging
import time
from datetime import datetime, UTC
import boto3
import requests

# ==============================
# CONFIGURATION
# ==============================
BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "sparkline": False
}
TOTAL_PAGES = 5        # 5 pages = 500 records
RETRIES = 3
SLEEP_TIME = 5         # seconds between pages
BUCKET_NAME = "crypto-analytics-pipeline"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def fetch_data(params, retries=RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait = int(e.response.headers.get("Retry-After", 15 * (attempt + 1)))
                logger.warning(f"Rate limit hit! Attempt {attempt+1} failed. Retrying in {wait} seconds...")
                time.sleep(wait)
                continue
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            time.sleep(2)
    raise Exception("API failed after retries")

def lambda_handler(event, context):
    logger.info("Starting ingestion lambda...")
    all_data = []
    
    # We use the lambda invocation time for ingestion_time
    now = datetime.now(UTC)
    ingestion_time = now.isoformat()

    for page in range(1, TOTAL_PAGES + 1):
        logger.info(f"Fetching page {page}")
        PARAMS["page"] = page
        
        try:
            data = fetch_data(PARAMS)
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}. Stopping and returning data collected so far.")
            break
            
        for record in data:
            record["ingestion_time"] = ingestion_time
            
        all_data.extend(data)
        time.sleep(SLEEP_TIME)
        
    logger.info(f"Total records fetched: {len(all_data)}")
    
    if not all_data:
        return {
            "statusCode": 500,
            "body": "No data was fetched from the API."
        }
    
    # Upload to S3 directly from memory
    s3_key = f"bronze/crypto/year={now.year}/month={now.month:02d}/day={now.day:02d}/data.json"
    logger.info(f"Uploading to S3: s3://{BUCKET_NAME}/{s3_key}")
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(all_data),
            ContentType="application/json"
        )
        logger.info("Upload successful.")
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise e
        
    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Successfully ingested {len(all_data)} records to {s3_key}"})
    }
