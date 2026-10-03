from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'DUC',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'stock_analyzer_pipeline',
    default_args=default_args,
    description='Automated Lakehouse Pipeline: Ingest -> Silver -> Gold',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'stock', 'vn30'],
) as dag:

    # 1. Ingest Data (Python script runs in Airflow container)
    ingest_bronze = BashOperator(
        task_id='ingest_stock_data_to_bronze',
        bash_command='python /opt/airflow/app/main.py',
    )

    # 2. Bronze to Silver (Spark job runs in spark-master-2 container)
    # Using docker exec to trigger spark-submit on the spark master
    silver_transformation = BashOperator(
        task_id='transform_bronze_to_silver',
        bash_command="""
        docker exec spark-master-2 spark-submit \
            /opt/bitnami/spark/app/jobs/bronze_to_silver.py \
            --bronze_input_path s3a://bronze/stock_data/vn30/ \
            --silver_output_path s3a://silver/stock_data/vn30/
        """,
    )

    # 3. Silver to Gold 
    gold_transformation = BashOperator(
        task_id='transform_silver_to_gold',
        bash_command="""
        docker exec spark-master-2 spark-submit \
            /opt/bitnami/spark/app/jobs/silver_to_gold.py \
            --dim_stock_output_path s3a://gold/stock_data/dim_stock \
            --fact_prices_output_path s3a://gold/stock_data/fact_prices \
            --top10_output_path s3a://gold/stock_data/top_10_performance
        """,
    )

    # Set dependency order
    ingest_bronze >> silver_transformation >> gold_transformation
