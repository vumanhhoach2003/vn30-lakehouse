"""
bronze_to_silver.py — Clean & Transform VN30 Stock Data
Bronze Layer -> Silver Layer
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main(bronze_input_path, silver_output_path):
    # Create Spark session (Simplified to use spark-defaults.conf) 
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Bronze to Silver: Spark session created.")

    try:
        # Create database
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse")

        # Register/Read Bronze table 
        print(f"Reading data from Bronze path: {bronze_input_path}")
        
        # Read Bronze Parquet data 
        df = spark.read.parquet(bronze_input_path)

        # Data Quality & Transformations 
        print("Normalizing data types and cleaning data...")
        
        # Cast Data Types
        df = df.withColumn("time", F.to_date("time"))
        df = df.withColumn("open",   F.col("open").cast("double"))
        df = df.withColumn("high",   F.col("high").cast("double"))
        df = df.withColumn("low",    F.col("low").cast("double"))
        df = df.withColumn("close",  F.col("close").cast("double"))
        df = df.withColumn("volume", F.col("volume").cast("long"))

        # Remove Nulls
        df = df.dropna(subset=["time", "close", "volume"])

        # Remove Duplicates
        df = df.dropDuplicates(["ticker", "time", "open", "close"])

        # Filter Invalid Values
        df = df.filter(
            (F.col("close")  > 0) &
            (F.col("volume") > 0) &
            (F.col("high")   >= F.col("low"))
        )

        # Add Calculated & Audit Columns
        df = df \
            .withColumn("price_change", F.round(F.col("close") - F.col("open"), 2)) \
            .withColumn("price_change_pct", F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 2)) \
            .withColumn("transformed_at", F.current_timestamp()) \
            .withColumn("year",  F.year("time")) \
            .withColumn("month", F.month("time"))

        # Selecting final columns
        silver_df = df.select(
            "ticker", "time", "open", "high", "low", "close", "volume",
            "price_change", "price_change_pct", "year", "month", "transformed_at"
        )

        # Write to Silver layer & Register in Hive
        print(f"Writing Silver table to: {silver_output_path}")
        (silver_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("overwriteSchema", "true")
            .save(silver_output_path))
        
        # Explicitly register/refresh table in Hive
        spark.sql(f"DROP TABLE IF EXISTS lakehouse.silver_stock")
        spark.sql(f"""
            CREATE TABLE lakehouse.silver_stock 
            USING DELTA 
            LOCATION '{silver_output_path}'
        """)

    except Exception as e:
        print(f"Error in bronze to silver transformation: {e}")
        spark.stop()
        raise

    print("Bronze to Silver transformation completed successfully.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Bronze to Silver Stock Transformation")
    parser.add_argument("--bronze_input_path", required=True, help="S3A input path for Bronze Parquet data")
    parser.add_argument("--silver_output_path", required=True, help="S3A output path for Silver Delta table")
    args = parser.parse_args()
    
    main(args.bronze_input_path, args.silver_output_path)