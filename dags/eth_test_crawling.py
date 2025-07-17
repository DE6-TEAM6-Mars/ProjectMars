from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup
import requests
import pandas as pd
import time
import os

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 0,
}

dag = DAG(
    dag_id="eth_whale_crawl_test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="테스트용: Etherscan 크롤링만 수행",
)

def test_crawl_eth_whales(**kwargs):
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
        if not table:
            print("❌ No table found on page.")
            return []

        rows = table.find_all("tr")[1:]  # 헤더 제외
        wallets = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
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

    def crawl_top_eth_wallets(pages=1):
        all_wallets = []
        for page in range(1, pages + 1):
            url = f"{BASE_URL}/{page}?ps=100"
            print(f"▶ Fetching page {page}: {url}")
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200:
                print(f"❌ Failed to fetch page {page} - Status: {response.status_code}")
                continue
            wallets = parse_wallets_from_page(response.text)
            all_wallets.extend(wallets)
            time.sleep(1.5)
        return pd.DataFrame(all_wallets)

    df = crawl_top_eth_wallets(pages=1)

    if df.empty:
        print("⚠️ DataFrame is empty. 크롤링 실패 가능성 있음.")
    else:
        print(f"✅ 크롤링 성공: {len(df)} rows 수집됨")

    os.makedirs("/tmp/eth_test", exist_ok=True)
    df.to_csv("/tmp/eth_test/top100_eth_test.csv", index=False)

crawl_test_task = PythonOperator(
    task_id="test_crawl_task",
    python_callable=test_crawl_eth_whales,
    provide_context=True,
    dag=dag,
)
