import pendulum
import requests
import logging
from utils import slack_callback
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

default_args = {
    'on_failure_callback': slack_callback,
}
@dag(
    dag_id="daily_eth_price_etl",
    default_args = default_args,
    start_date=pendulum.datetime(2025, 7, 9, tz="Asia/Seoul"),
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
    3.  **Load**: 최종 레코드를 S3의 마스터 파일과 Redshift 테이블에 병렬로 저장합니다.
    """

    # --- 1. EXTRACT Tasks ---
    @task
    def get_eth_usd_price(logical_date: str) -> float:
        """CryptoCompare API에서 특정 날짜의 ETH 종가를 가져옵니다."""
        logging.info(f"Extract 태스크 시작: ETH/USD 가격 조회를 시작합니다. (날짜: {logical_date})")
        dt = pendulum.parse(logical_date)
        ts = int(dt.timestamp())
        url = f"https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=USD&ts={ts}"
        
        response = requests.get(url)
        response.raise_for_status()
        
        price = response.json()['ETH']['USD']
        logging.info(f"API로부터 받은 ETH/USD 가격: {price}")
        return price

    @task
    def get_usd_krw_fx_rate(logical_date: str) -> float:
        """Frankfurter.app API에서 특정 날짜의 USD/KRW 종가 환율을 가져옵니다."""
        logging.info(f"Extract 태스크 시작: USD/KRW 환율 조회를 시작합니다. (날짜: {logical_date})")
        date_str = pendulum.parse(logical_date).to_date_string()
        url = f"https://api.frankfurter.app/{date_str}?from=USD&to=KRW"

        response = requests.get(url)
        response.raise_for_status()

        rate = response.json()['rates']['KRW']
        logging.info(f"API로부터 받은 USD/KRW 환율: {rate}")
        return rate

    # --- 2. TRANSFORM Task ---
    @task
    def transform_price_data(price_usd: float, fx_rate: float, logical_date: str) -> dict:
        """추출된 데이터를 조합하여 최종 레코드를 만듭니다."""
        logging.info(f"Transform 태스크 시작: price_usd={price_usd}, fx_rate={fx_rate}")
        price_krw = price_usd * fx_rate
        date_str = pendulum.parse(logical_date).to_date_string()
        final_record = {
            "price_date": date_str,
            "price_usd": round(price_usd, 4),
            "price_krw": round(price_krw, 4)
        }
        logging.info(f"최종 변환된 레코드: {final_record}")
        return final_record

    # --- 3. LOAD Tasks ---
    @task
    def load_to_redshift(record: dict):
        """
        처리된 레코드를 Redshift 테이블에 적재합니다.
        - 만약 동일한 price_date의 데이터가 이미 있다면, 기존 데이터를 삭제하고 새로운 데이터로 교체합니다.
        """
        logging.info(f"Load 태스크 시작: Redshift 테이블에 레코드를 적재합니다. Record: {record}")
        hook = RedshiftSQLHook(redshift_conn_id="RedshiftConn")
        
        conn = hook.get_conn()
        
        try:
            with conn.cursor() as cursor:
                # 1. 기존 데이터 삭제 (단일 명령)
                delete_sql = "DELETE FROM analytics.dim_daily_prices WHERE price_date = %s;"
                cursor.execute(delete_sql, (record['price_date'],))
                logging.info(f"기존 데이터 삭제 시도: price_date = {record['price_date']}")

                # 2. 새로운 데이터 삽입 (단일 명령)
                insert_sql = "INSERT INTO analytics.dim_daily_prices (price_date, price_usd, price_krw) VALUES (%s, %s, %s);"
                cursor.execute(insert_sql, (record['price_date'], record['price_usd'], record['price_krw']))
                logging.info(f"새로운 데이터 삽입: {record}")

            # 3. 모든 작업이 성공했을 때 트랜잭션을 영구 저장(COMMIT)합니다.
            conn.commit()
            logging.info("Redshift Load 성공: 트랜잭션이 성공적으로 COMMIT되었습니다.")

        except Exception as e:
            # 4. 작업 중 하나라도 실패하면 모든 변경 사항을 취소(ROLLBACK)합니다.
            logging.error(f"트랜잭션 실패: {e}")
            conn.rollback()
            raise # 에러를 다시 발생시켜 Airflow 태스크를 실패 상태로 만듭니다.

        finally:
            conn.close()


    # --- DAG 실행 순서 정의 ---
    logical_date_str = "{{ ds }}"
    
    # 1. Extract (두 태스크가 병렬로 실행됨)
    usd_price = get_eth_usd_price(logical_date=logical_date_str)
    krw_rate = get_usd_krw_fx_rate(logical_date=logical_date_str)

    # 2. Transform (usd_price와 krw_rate의 결과를 입력으로 받음)
    final_data_record = transform_price_data(
        price_usd=usd_price, 
        fx_rate=krw_rate, 
        logical_date=logical_date_str
    )

    # 3. Load (transform의 결과를 입력으로 받아 두 태스크가 병렬로 실행됨)
    load_to_redshift(record=final_data_record)

daily_eth_price_etl()