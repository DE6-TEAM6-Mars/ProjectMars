from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from utils import collect_raw_transactions, upload_to_s3
import logging


@dag(
    dag_id='ethereum_block_collector_auto',
    start_date=datetime(2025, 6, 30, 15, 5, 0),
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['ethereum', 'block', 'collector']
)
def ethereum_block_collector_auto():
    @task()
    def collect_and_save() -> None:
        context = get_current_context()
        execution_date: datetime = context["execution_date"]

        # UTC 기준의 execution_date를 사용 (Airflow 내부 시간 기준)
        from_ts = execution_date - timedelta(hours=1)
        to_ts = execution_date

        logging.info(f"[COLLECT] Target time range: {from_ts} → {to_ts}")
        txs = collect_raw_transactions(from_ts, to_ts)
        logging.info(f"[COLLECT] Total collected txs: {len(txs)}")

        upload_to_s3(txs, execution_date)

    collect_and_save()


ethereum_block_collector_auto()
