
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main(dim_stock_path, fact_prices_path, top10_path):
    # Create Spark session 
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Silver to Gold: Spark session created.")

    try:
        # Read from silver table in Hive 
        print("Reading from lakehouse.silver_stock...")
        silver_df = spark.table("lakehouse.silver_stock")

        # Create Tables 
        
        # Dimension Table: DimStock
        print("Creating DimStock...")
        dim_stock = silver_df.select("ticker").distinct() \
                             .withColumn("updated_at", F.current_timestamp())

        # Fact Table: FactStockPrices
        print("Creating FactStockPrices...")
        fact_prices = silver_df.select(
            "ticker", "time", "open", "high", "low", "close", "volume", 
            "price_change", "price_change_pct"
        ).withColumn("created_at", F.current_timestamp())

        # Analysis: Top 10 Performance
        print("Calculating Top 10 Stocks...")
        window_spec = Window.partitionBy("ticker").orderBy("time")
        window_spec_desc = Window.partitionBy("ticker").orderBy(F.col("time").desc())
        df_perf = silver_df.withColumn("row_start", F.row_number().over(window_spec)) \
                           .withColumn("row_end", F.row_number().over(window_spec_desc))
        
        start_prices = df_perf.filter("row_start = 1").selectExpr("ticker as t1", "close as price_start")
        end_prices = df_perf.filter("row_end = 1").selectExpr("ticker as t2", "close as price_end")
        
        top_10 = start_prices.join(end_prices, F.col("t1") == F.col("t2")) \
            .withColumn("growth_pct", F.round(((F.col("price_end") - F.col("price_start")) / F.col("price_start")) * 100, 2)) \
            .selectExpr("t1 as ticker", "price_start", "price_end", "growth_pct") \
            .orderBy(F.desc("growth_pct")) \
            .limit(10)

        # Write Tables and Register in Hive 
        print("Writing Gold tables...")

        # DimStock
        dim_stock.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(dim_stock_path)
        spark.sql("DROP TABLE IF EXISTS lakehouse.dim_stock")
        spark.sql(f"CREATE TABLE lakehouse.dim_stock USING DELTA LOCATION '{dim_stock_path}'")

        # FactStockPrices
        fact_prices.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(fact_prices_path)
        spark.sql("DROP TABLE IF EXISTS lakehouse.fact_stock_prices")
        spark.sql(f"CREATE TABLE lakehouse.fact_stock_prices USING DELTA LOCATION '{fact_prices_path}'")

        # Top 10 Performance
        top_10.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(top10_path)
        spark.sql("DROP TABLE IF EXISTS lakehouse.top_10_performance")
        spark.sql(f"CREATE TABLE lakehouse.top_10_performance USING DELTA LOCATION '{top10_path}'")

        print("\n [TOP 10 STOCK PERFORMANCE]")
        top_10.show()

    except Exception as e:
        print(f"Error in silver to gold transformation: {e}")
        spark.stop()
        raise

    print("Silver to Gold transformation completed successfully.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Silver to Gold Stock Transformation")
    parser.add_argument("--dim_stock_output_path", required=True, help="S3A output path for DimStock")
    parser.add_argument("--fact_prices_output_path", required=True, help="S3A output path for FactStockPrices")
    parser.add_argument("--top10_output_path", required=True, help="S3A output path for Top 10 Performance Analysis")
    args = parser.parse_args()
    
    main(args.dim_stock_output_path, args.fact_prices_output_path, args.top10_output_path)
