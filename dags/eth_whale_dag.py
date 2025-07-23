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
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/webp,*/*;q=0.8"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://etherscan.io/",
    "Connection": "keep-alive",
}



    def parse_wallets_from_page(html):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")[1:]
        wallets = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            # 1차 필터: img 태그가 존재 → 거래소 or 컨트랙트
            if cols[2].find("img"):
                continue

            # 2차 필터: 네임태그가 존재 → 개인 주소 아님
            name_tag = cols[2].text.strip()
            if name_tag:
                continue

            rank = cols[0].text.strip()
            try:
                span = cols[1].find("span", attrs={"data-highlight-target": True})
                address = span['data-highlight-target'].strip()
            except Exception:
                address = "N/A"

            eth_balance = cols[3].text.strip()
            percentage = cols[4].text.strip()

            wallets.append({
                "address": address,
                "eth_balance": eth_balance,
                "percentage": percentage
            })
        return wallets

    def crawl_top_eth_wallets(pages=100):
        all_wallets = []
        for page in range(1, pages + 1):
            url = f"{BASE_URL}/{page}?ps=100"
            try:
                response = requests.get(url, headers=HEADERS, timeout=10)
                if response.status_code != 200:
                    print(f"[ERROR] Failed to fetch page {page} (Status code: {response.status_code})")
                    continue
                wallets = parse_wallets_from_page(response.text)
                all_wallets.extend(wallets)
            except requests.RequestException as e:
                print(f"[ERROR] Exception while fetching page {page}: {e}")
                continue

            time.sleep(3)  # ✅ 봇 차단 방지용 sleep 증가

        df = pd.DataFrame(all_wallets, columns=["address", "eth_balance", "percentage"])
        if df.empty:
            raise ValueError("[FAILURE] No wallet data was collected. Failing DAG.")  # ✅ 결과 비었을 때 DAG 실패 유도

        return df

    run_date = kwargs['ds']  # yyyy-mm-dd
    local_dir = f"/tmp/eth_data/{run_date}"
    os.makedirs(local_dir, exist_ok=True)

    df = crawl_top_eth_wallets(pages=100)
    df["inserted_at"] = pd.to_datetime(run_date)
    df.to_csv(f"{local_dir}/top10000_holders_eth.csv", index=False)

# ─────────────────────────────────────────────
# Step 2: S3 업로드
# ─────────────────────────────────────────────
def upload_to_s3(**kwargs):
    run_date = kwargs['ds']
    file_path = f"/tmp/eth_data/{run_date}/top10000_holders_eth.csv"
    s3_key = f"eth/whale/{run_date}/top10000_holders_eth.csv"

    bucket_name = Variable.get("S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="S3Conn")
    s3_hook.load_file(filename=file_path, key=s3_key, bucket_name=bucket_name, replace=True)

    kwargs['ti'].xcom_push(key='s3_key', value=s3_key)


# ─────────────────────────────────────────────
# Step 3: Redshift COPY
# ─────────────────────────────────────────────
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