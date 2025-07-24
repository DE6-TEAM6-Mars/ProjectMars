from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable


import pandas as pd
from datetime import datetime


# ─────────────────────────────────────────────
# DAG 설정: 매일 오전 10시 (KST 기준)
# ─────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}
dag = DAG(
    dag_id="eth_whale_redshift_only",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # UTC 01:00 → KST 10:00
    catchup=False,
)


def copy_to_redshift(**kwargs):
    run_date = kwargs["ds"]
    s3_key = f"eth/whale/{run_date}/top10000_holders_eth.csv"
    bucket = Variable.get("S3_BUCKET_NAME")
    region = "ap-northeast-2"

    access_key = Variable.get("AWS_ACCESS_KEY_ID")
    secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    redshift = PostgresHook(postgres_conn_id="RedshiftConn")

    create_sql = """
    CREATE TABLE IF NOT EXISTS raw_data.eth_top_holders (
        address VARCHAR,
        eth_balance VARCHAR,
        percentage VARCHAR,
        inserted_at TIMESTAMP
    );
    """

    copy_sql = f"""
    COPY raw_data.eth_top_holders
    FROM 's3://{bucket}/{s3_key}'
    CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
    REGION '{region}'
    FORMAT AS CSV
    IGNOREHEADER 1
    DELIMITER ','; 
    """

    with redshift.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
            cursor.execute("TRUNCATE TABLE raw_data.eth_top_holders")
            print(f"[INFO] Copying from s3://{bucket}/{s3_key} to Redshift table raw_data.eth_top_holders")
            cursor.execute(copy_sql)
        conn.commit()
# ─────────────────────────────────────────────
# DAG Task 연결
# ─────────────────────────────────────────────

copy_task = PythonOperator(
    task_id="copy_to_redshift",
    python_callable=copy_to_redshift,
    provide_context=True,
    dag=dag,
)
