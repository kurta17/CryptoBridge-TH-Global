from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# The DAG will run daily at 1 AM UTC to process previous day's data
with DAG(
    'crypto_daily_etl',
    default_args=default_args,
    description='Daily ETL pipeline for crypto data',
    schedule_interval='0 1 * * *',  # Run at 1 AM UTC daily
    start_date=datetime(2025, 9, 26),  # Start from today
    catchup=False,  # Don't run backfill jobs
    tags=['crypto', 'daily'],
) as dag:

    # Task 1: Fetch daily crypto data
    fetch_data = BashOperator(
        task_id='fetch_daily_data',
        bash_command='python /opt/airflow/scripts/fetch_daily_crypto_data.py --execution_date="{{ ds }}"'
    )

    # Task 2: Load data to ClickHouse (silver layer)
    load_silver = BashOperator(
        task_id='load_to_silver',
        bash_command='python /opt/airflow/scripts/load_daily_to_clickhouse.py --execution_date="{{ ds }}"'
    )

    # Task 3: Create gold layer
    create_gold = BashOperator(
        task_id='create_gold',
        bash_command='python /opt/airflow/scripts/create_daily_gold_layer.py --execution_date="{{ ds }}"'
    )

    # Set task dependencies
    fetch_data >> load_silver >> create_gold