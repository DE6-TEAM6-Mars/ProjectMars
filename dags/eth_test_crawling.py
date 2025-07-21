from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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
    description="테스트용: Etherscan 크롤링 (Selenium 기반)",
)

def test_crawl_eth_whales(**kwargs):
    def crawl_top_eth_wallets(pages=1):
        options = Options()
        options.add_argument("--headless")  # 브라우저 창 없이 실행
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=options)

        all_wallets = []

        for page in range(1, pages + 1):
            url = f"https://etherscan.io/accounts/{page}?ps=100"
            print(f"▶ Fetching page {page}: {url}")
            driver.get(url)
            time.sleep(3)  # JS 렌더링 기다림

            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.find("table")
            if not table:
                print(f"❌ No table found on page {page}")
                continue

            rows = table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) < 5:
                    continue
                try:
                    address = cols[1].find("span", {"data-highlight-target": True})["data-highlight-target"]
                except:
                    address = "N/A"

                all_wallets.append({
                    "rank": cols[0].text.strip(),
                    "address": address,
                    "address_nametag": cols[2].text.strip(),
                    "eth_balance": cols[3].text.strip(),
                    "percentage": cols[4].text.strip(),
                })

            time.sleep(1.5)

        driver.quit()
        return pd.DataFrame(all_wallets)

    # 실행
    df = crawl_top_eth_wallets(pages=1)

    if df.empty:
        print("⚠️ DataFrame is empty. 크롤링 실패 가능성 있음.")
    else:
        print(f"✅ 크롤링 성공: {len(df)} rows 수집됨")

    os.makedirs("/tmp/eth_test", exist_ok=True)
    df.to_csv("/tmp/eth_test/top100_eth_test.csv", index=False)

# DAG Task 정의
crawl_test_task = PythonOperator(
    task_id="test_crawl_task",
    python_callable=test_crawl_eth_whales,
    provide_context=True,
    dag=dag,
)
