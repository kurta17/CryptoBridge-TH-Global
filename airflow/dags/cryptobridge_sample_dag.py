"""
🚀 CryptoBridge Sample DAG
A demonstration DAG for CryptoBridge data processing workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import json

# Default arguments for the DAG
default_args = {
    "owner": "cryptobridge",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "cryptobridge_sample_dag",
    default_args=default_args,
    description="A sample CryptoBridge data processing DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["cryptobridge", "demo", "data-processing"],
)


def check_clickhouse_connection():
    """Check if ClickHouse is accessible"""
    import requests

    try:
        response = requests.get("http://clickhouse:8123/ping", timeout=10)
        if response.status_code == 200:
            print("✅ ClickHouse is accessible")
            return "ClickHouse connection successful"
        else:
            raise Exception(f"ClickHouse returned status code: {response.status_code}")
    except Exception as e:
        print(f"❌ ClickHouse connection failed: {e}")
        raise


def get_transaction_stats():
    """Get transaction statistics from ClickHouse"""
    import requests

    query = """
    SELECT 
        COUNT(*) as total_transactions,
        SUM(amount) as total_volume,
        AVG(amount) as avg_amount,
        COUNT(DISTINCT sender_country) as sender_countries,
        COUNT(DISTINCT receiver_country) as receiver_countries
    FROM cryptobridge.transactions 
    FORMAT JSON
    """

    try:
        response = requests.post(
            "http://clickhouse:8123/",
            params={"user": "analytics", "password": "analytics123"},
            data=query,
            timeout=30,
        )

        if response.status_code == 200:
            result = response.json()
            stats = result["data"][0]
            print("📊 Transaction Statistics:")
            print(f"   • Total Transactions: {stats['total_transactions']:,}")
            print(f"   • Total Volume: ${stats['total_volume']:,.2f}")
            print(f"   • Average Amount: ${stats['avg_amount']:,.2f}")
            print(f"   • Sender Countries: {stats['sender_countries']}")
            print(f"   • Receiver Countries: {stats['receiver_countries']}")
            return stats
        else:
            raise Exception(f"Query failed with status code: {response.status_code}")

    except Exception as e:
        print(f"❌ Failed to get transaction stats: {e}")
        raise


def analyze_fraud_patterns():
    """Analyze fraud patterns in the data"""
    import requests

    query = """
    SELECT 
        is_suspicious,
        COUNT(*) as count,
        AVG(risk_score) as avg_risk_score,
        SUM(amount) as total_volume
    FROM cryptobridge.transactions 
    GROUP BY is_suspicious
    FORMAT JSON
    """

    try:
        response = requests.post(
            "http://clickhouse:8123/",
            params={"user": "analytics", "password": "analytics123"},
            data=query,
            timeout=30,
        )

        if response.status_code == 200:
            result = response.json()
            print("🔍 Fraud Analysis Results:")
            for row in result["data"]:
                status = "Suspicious" if row["is_suspicious"] else "Normal"
                print(f"   • {status} Transactions: {row['count']:,}")
                print(f"     - Average Risk Score: {row['avg_risk_score']:.1f}")
                print(f"     - Total Volume: ${row['total_volume']:,.2f}")
            return result["data"]
        else:
            raise Exception(
                f"Fraud analysis failed with status code: {response.status_code}"
            )

    except Exception as e:
        print(f"❌ Failed to analyze fraud patterns: {e}")
        raise


def generate_daily_report(**context):
    """Generate a daily summary report"""
    # Get data from previous tasks
    transaction_stats = context["ti"].xcom_pull(task_ids="get_transaction_stats")
    fraud_analysis = context["ti"].xcom_pull(task_ids="analyze_fraud_patterns")

    report = {
        "date": context["ds"],
        "transaction_summary": transaction_stats,
        "fraud_analysis": fraud_analysis,
        "generated_at": datetime.now().isoformat(),
    }

    print("📋 Daily Report Generated:")
    print(json.dumps(report, indent=2, default=str))

    return report


# Define tasks
check_clickhouse_task = PythonOperator(
    task_id="check_clickhouse_connection",
    python_callable=check_clickhouse_connection,
    dag=dag,
)

transaction_stats_task = PythonOperator(
    task_id="get_transaction_stats",
    python_callable=get_transaction_stats,
    dag=dag,
)

fraud_analysis_task = PythonOperator(
    task_id="analyze_fraud_patterns",
    python_callable=analyze_fraud_patterns,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id="generate_daily_report",
    python_callable=generate_daily_report,
    dag=dag,
)

# Task to check if data files exist
check_data_files_task = BashOperator(
    task_id="check_data_files",
    bash_command='ls -la /data/generated/ || echo "No data files found"',
    dag=dag,
)

# Define task dependencies
check_clickhouse_task >> [transaction_stats_task, fraud_analysis_task]
[transaction_stats_task, fraud_analysis_task] >> generate_report_task
check_data_files_task
