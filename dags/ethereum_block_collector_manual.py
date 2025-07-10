from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from typing import List, Dict, Any
from utils import collect_raw_transactions, upload_to_s3
import logging


@dag(
    dag_id='ethereum_block_collector_manual',
    start_date=datetime(2025, 6, 30, 15, 5, 0),
    schedule_interval='@hourly',
    catchup=True,
    max_active_runs=1,
    default_args={
        'owner': 'jeongin',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['ethereum', 'block', 'collector']
)
def ethereum_block_collector_manual():
    @task()
    def collect_task() -> List[Dict[str, Any]]:
        context = get_current_context()
        execution_date: datetime = context["execution_date"]

        from_ts = execution_date - timedelta(hours=1)
        to_ts = execution_date

        logging.info(f"[1] Collecting transactions from {from_ts} to {to_ts}")
        return collect_raw_transactions(from_ts, to_ts)

    @task()
    def save_to_s3(txs: List[Dict[str, Any]]) -> None:
        context = get_current_context()
        execution_date: datetime = context["execution_date"]
        upload_to_s3(txs, execution_date)

    raw_txs = collect_task()
    save_to_s3(raw_txs)


ethereum_block_collector_manual()
