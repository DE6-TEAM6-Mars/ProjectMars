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
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options

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

    def crawl_eth_wallets_selenium(pages=100):
        options = uc.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = uc.Chrome(options=options)

        all_wallets = []

        for page in range(1, pages + 1):
            url = f"https://etherscan.io/accounts/{page}?ps=100"
            driver.get(url)
            time.sleep(3)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table")
            if not table:
                continue
            rows = table.find_all("tr")[1:]

            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue

                # 1차 필터: img 태그 존재 시 스킵 (거래소/컨트랙트 주소)
                if cols[2].find("img"):
                    continue

                # 2차 필터: 네임태그 존재 시 스킵 (개인 주소 아님)
                if cols[2].text.strip():
                    continue

                try:
                    address = cols[1].find("span", {"data-highlight-target": True})["data-highlight-target"].strip()
                except Exception:
                    address = "N/A"

                eth_balance = cols[3].text.strip()
                percentage = cols[4].text.strip()

                all_wallets.append({
                    "address": address,
                    "eth_balance": eth_balance,
                    "percentage": percentage
                })

        driver.quit()
        return pd.DataFrame(all_wallets, columns=["address", "eth_balance", "percentage"])

    # 실행 날짜 기반 저장 경로 생성
    run_date = kwargs['ds']  # yyyy-mm-dd
    local_dir = f"/tmp/eth_data/{run_date}"
    os.makedirs(local_dir, exist_ok=True)

    # Selenium 기반 크롤링 수행
    df = crawl_eth_wallets_selenium(pages=100)

    if df.empty:
        raise ValueError("[FAILURE] No wallet data was collected. Failing DAG.")

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