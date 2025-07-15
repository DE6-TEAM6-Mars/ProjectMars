from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

def build_dynamic_where_clause(start_date: datetime, end_date: datetime) -> str:
    current = start_date
    conditions = []
    while current <= end_date:
        year = current.strftime("%Y")
        month = current.strftime("%m")
        day = current.strftime("%d")
        conditions.append(f"(year = '{year}' AND month = '{month}' AND day = '{day}')")
        current += timedelta(days=1)
    return " OR\n".join(conditions)

# 날짜 범위 지정
START_DATE = datetime(2015, 7, 1)
END_DATE = datetime(2016, 7, 1)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='insert_historical_transactions',
    default_args=default_args,
    description='Insert Spectrum ETH tx into Redshift over dynamic date range',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    where_clause = build_dynamic_where_clause(START_DATE, END_DATE)

    insert_dynamic = PostgresOperator(
        task_id='insert_historical_transactions',
        postgres_conn_id='redshift_conn_id', # Redshift 연결 ID
        sql=f"""
            INSERT INTO raw_data.test_historical_transactions
            SELECT
                transactionhash,
                blocknumber,
                "from",
                "to",
                value,
                status,
                timestamp
            FROM spectrum.tb_eth_transactions_parquet
            WHERE {where_clause};
        """
    )
