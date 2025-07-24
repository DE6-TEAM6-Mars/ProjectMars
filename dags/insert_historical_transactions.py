from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import psycopg2

def build_dynamic_where_clause(start_date: datetime, end_date: datetime) -> str:
    current = start_date
    conditions = []
    while current <= end_date:
        year = current.strftime("%Y")
        month = str(int(current.strftime("%m")))  # 앞의 0 제거
        day = str(int(current.strftime("%d")))    # 앞의 0 제거
        conditions.append(f"(year = '{year}' AND month = '{month}' AND day = '{day}')")
        current += timedelta(days=1)
    return " OR\n".join(conditions)


# 날짜 범위 지정
START_DATE = datetime(2025, 6, 1)
END_DATE = datetime(2025, 6, 30)

def insert_transactions_to_redshift():
    # Airflow Connection에서 Redshift 연결 정보 가져오기
    conn = BaseHook.get_connection('RedshiftConn')

    # psycopg2 연결 생성
    pg_conn = psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    # 동적 WHERE 절 생성
    where_clause = build_dynamic_where_clause(START_DATE, END_DATE)

    # SQL 쿼리 실행
    query = f"""
        INSERT INTO tb_eth_historical_transactions (
            transactionhash,
            blocknumber,
            transaction_from,
            transaction_to,
            value,
            transaction_status,
            key_year,
            key_month,
            key_day,
            key_hour,
            transaction_timestamp
        )
        SELECT
            transactionhash,
            blocknumber,
            "from" AS transaction_from,
            "to" AS transaction_to,
            value,
            status AS transaction_status,
            year AS key_year,
            month AS key_month,
            day AS key_day,
            hour AS key_hour,
            timestamp AS transaction_timestamp
        FROM spectrum.eth_transactions_parquet
        WHERE {where_clause};
    """
    cur.execute(query)
    cur.close()
    pg_conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='insert_historical_transactions',
    default_args=default_args,
    description='Insert Spectrum ETH tx into Redshift (via psycopg2 + Airflow Connection)',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    insert_dynamic = PythonOperator(
        task_id='insert_historical_transactions',
        python_callable=insert_transactions_to_redshift
    )
