import pendulum
import requests
import json

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(
    dag_id="daily_eth_price_etl",
    start_date=pendulum.datetime(2025, 7, 10, tz="Asia/Seoul"),
    schedule_interval="5 9 * * *",
    description="[TaskFlow] 매일 ETH 가격과 환율을 가져와 S3 마스터 파일과 Redshift 테이블에 추가하는 DAG",
    catchup=True,
    tags=["price", "s3", "api", "taskflow"],
    max_active_runs=1,
)

def daily_eth_price_etl():
    """
    ### 일일 ETH 가격 ETL 파이프라인
    1.  **Extract**: CryptoCompare와 Frankfurter API를 병렬로 호출하여 USD 가격과 환율을 가져옵니다.
    2.  **Transform**: 두 결과를 조합하여 최종 KRW 가격을 계산하고 하나의 레코드로 만듭니다.
    3.  **Load**: 최종 레코드를 S3의 일일 파일과 마스터 파일에 병렬로 저장합니다.
    """

    # --- 1. EXTRACT Tasks ---
    @task
    def get_eth_usd_price(logical_date: str) -> float:
        """CryptoCompare API에서 특정 날짜의 ETH 종가를 가져옵니다."""
        dt = pendulum.parse(logical_date)
        ts = int(dt.timestamp())
        url = f"https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=USD&ts={ts}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()['ETH']['USD']

    @task
    def get_usd_krw_fx_rate(logical_date: str) -> float:
        """Frankfurter.app API에서 특정 날짜의 USD/KRW 종가 환율을 가져옵니다."""
        date_str = pendulum.parse(logical_date).to_date_string()
        url = f"https://api.frankfurter.app/{date_str}?from=USD&to=KRW"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()['rates']['KRW']

    # --- 2. TRANSFORM Task ---
    @task
    def transform_price_data(price_usd: float, fx_rate: float, logical_date: str) -> dict:
        """추출된 데이터를 조합하여 최종 레코드를 만듭니다."""
        price_krw = price_usd * fx_rate
        date_str = pendulum.parse(logical_date).to_date_string()
        
        final_record = {
            "price_date": date_str,
            "price_usd": round(price_usd, 4),
            "price_krw": round(price_krw, 4)
        }
        return final_record

    # --- 3. LOAD Tasks ---
    @task
    def append_to_master_file(record: dict, s3_bucket: str):
        """처리된 레코드를 S3의 '마스터 파일'에 한 행 추가합니다."""
        s3_hook = S3Hook(aws_conn_id='S3Conn')
        master_file_key = "reference_data/eth_prices.jsonl"
        
        try:
            existing_content = s3_hook.read_key(key=master_file_key, bucket_name=s3_bucket)
        except Exception:
            existing_content = ""

        new_line = json.dumps(record, ensure_ascii=False)
        updated_content = (existing_content.strip() + '\n' + new_line) if existing_content else new_line
        
        s3_hook.load_string(
            string_data=updated_content,
            key=master_file_key,
            bucket_name=s3_bucket,
            replace=True,
        )
        print(f"마스터 파일에 한 행을 추가했습니다: s3://{s3_bucket}/{master_file_key}")

    load_to_redshift = PostgresOperator(
        task_id="load_data_to_redshift",
        postgres_conn_id="RedshiftConn",
        sql="""
            INSERT INTO analytics.dim_daily_prices (price_date, price_usd, price_krw)
            VALUES (
                '{{ ti.xcom_pull(task_ids='transform_price_data')['price_date'] }}',
                {{ ti.xcom_pull(task_ids='transform_price_data')['price_usd'] }},
                {{ ti.xcom_pull(task_ids='transform_price_data')['price_krw'] }}
            );
        """,
    )

    logical_date_str = "{{ data_interval_end.to_date_string() }}"
    
    # 1. Extract (두 태스크가 병렬로 실행됨)
    usd_price = get_eth_usd_price(logical_date_str)
    krw_rate = get_usd_krw_fx_rate(logical_date_str)
    
    # 2. Transform (usd_price와 krw_rate의 결과를 입력으로 받음)
    final_data_record = transform_price_data(usd_price, krw_rate, logical_date_str)

    # 3. Load (transform의 결과를 입력으로 받아 두 태스크가 병렬로 실행됨)
    s3_bucket_name = "de6-team6-bucket"
    final_data_record >> append_to_master_file(s3_bucket_name)
    final_data_record >> load_to_redshift

daily_eth_price_etl()