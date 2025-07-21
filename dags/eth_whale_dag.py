from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable


import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
import os
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
    dag_id="eth_whale_wallets_daily_to_redshift",
    default_args=default_args,
    schedule_interval="0 1 * * *",  # UTC 01:00 → KST 10:00
    catchup=False,
)

# ─────────────────────────────────────────────
# Step 1: 크롤링 & CSV 저장 (inserted_at 포함)
# ─────────────────────────────────────────────
def crawl_and_save_csv(**kwargs):
    BASE_URL = "https://etherscan.io/accounts"
    HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}

    def parse_wallets_from_page(html):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")[1:]
        wallets = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 4:
                continue

            rank = cols[0].text.strip()
            try:
                span = cols[1].find("span", attrs={"data-highlight-target": True})
                address = span['data-highlight-target'].strip()
            except Exception:
                address = "N/A"

            name_tag = cols[2].text.strip()
            eth_balance = cols[3].text.strip()
            percentage = cols[4].text.strip()

            wallets.append({
                "rank": rank,
                "address": address,
                "address_nametag": name_tag,
                "eth_balance": eth_balance,
                "percentage": percentage
            })
        return wallets

    def crawl_top_eth_wallets(pages=10):
        all_wallets = []
        for page in range(1, pages + 1):
            url = f"{BASE_URL}/{page}?ps=100"
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200:
                print(f"Failed to fetch page {page}")
                continue
            wallets = parse_wallets_from_page(response.text)
            all_wallets.extend(wallets)
            time.sleep(1.5)
        return pd.DataFrame(all_wallets)

    # 날짜 기반 경로 및 inserted_at 추가
    run_date = kwargs['ds']  # yyyy-mm-dd
    local_dir = f"/tmp/eth_data/{run_date}"
    os.makedirs(local_dir, exist_ok=True)

    df = crawl_top_eth_wallets(pages=10)
    df["inserted_at"] = pd.to_datetime(run_date)  # 또는 datetime.utcnow()
    df.to_csv(f"{local_dir}/top1000_holders_eth.csv", index=False)

# ─────────────────────────────────────────────
# Step 2: S3 업로드
# ─────────────────────────────────────────────
def upload_to_s3(**kwargs):
    run_date = kwargs['ds']
    file_path = f"/tmp/eth_data/{run_date}/top1000_holders_eth.csv"
    s3_key = f"eth/whale/{run_date}/top1000_holders_eth.csv"

    bucket_name = Variable.get("S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="S3Conn")
    s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=bucket_name, replace=True)

    kwargs['ti'].xcom_push(key='s3_key', value=s3_key)

# ─────────────────────────────────────────────
# Step 3: Redshift COPY
# ─────────────────────────────────────────────
def copy_to_redshift(**kwargs):
    s3_key = kwargs['ti'].xcom_pull(key='s3_key', task_ids='upload_to_s3')
    bucket = Variable.get("S3_BUCKET_NAME")
    region = "ap-northeast-2"
    
    access_key = Variable.get("AWS_ACCESS_KEY_ID")
    secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    redshift = PostgresHook(postgres_conn_id="REDSHIFT_CONN_ID")

    create_sql = """
    CREATE TABLE IF NOT EXISTS raw_data.eth_top_holders (
        rank INT,
        address VARCHAR,
        address_nametag VARCHAR,
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
            cursor.execute(copy_sql)
        conn.commit()

# ─────────────────────────────────────────────
# DAG Task 연결
# ─────────────────────────────────────────────
crawl_task = PythonOperator(
    task_id="crawl_eth_wallets",
    python_callable=crawl_and_save_csv,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

copy_task = PythonOperator(
    task_id="copy_to_redshift",
    python_callable=copy_to_redshift,
    provide_context=True,
    dag=dag,
)

crawl_task >> upload_task >> copy_task
