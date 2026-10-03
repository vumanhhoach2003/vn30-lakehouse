
import sys
import os
import logging
from datetime import datetime

# Imports from the ingestion package
from ingestion.stock_api     import get_stock_data
from ingestion.load_to_minio import upload_parquet, TODAY

# Setup Logging 
logger = logging.getLogger(__name__)

def run_pipeline():
    logger.info("Starting Lakehouse Bronze Pipeline...")

    
    # VN30 Stock Data (Ingest to Bronze as Parquet)
    
    logger.info(" Fetching VN30 stock data ")
    stock_data = get_stock_data()

    logger.info(f"Uploading {len(stock_data)} tickers as Parquet to MinIO...")
    for ticker, df in stock_data.items():
        s3_key = f"stock_data/vn30/ingested_date={TODAY}/{ticker}.parquet"
        upload_parquet(df, s3_key)


    logger.info("Pipeline complete! Check MinIO at http://localhost:9001")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
