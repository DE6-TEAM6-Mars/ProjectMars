from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from utils import collect_raw_transactions, upload_to_s3, load_partition_and_table, slack_callback
import logging


@dag(
    dag_id='ethereum_block_collector_auto',
    start_date=datetime(2025, 7, 1, 1, 5),
    schedule_interval='5 * * * *',
    catchup=True,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': slack_callback,
    },
    tags=['ethereum', 'block', 'collector']
)
def ethereum_block_collector_auto():
    @task()
    def collect_and_save() -> None:
        context = get_current_context()
        execution_date: datetime = context["execution_date"]

        # UTC 기준의 execution_date를 사용 (Airflow 내부 시간 기준)
        aligned_execution = execution_date.replace(minute=0, second=0, microsecond=0)
        from_ts = aligned_execution - timedelta(hours=1)
        to_ts = aligned_execution

        logging.info(f"[COLLECT] Target time range: {from_ts} → {to_ts}")
        txs = collect_raw_transactions(from_ts, to_ts)
        logging.info(f"[COLLECT] Total collected txs: {len(txs)}")

        upload_to_s3(txs, from_ts)
        if txs:  # ✅ 데이터가 존재할 때만 Glue & Redshift 처리
            load_partition_and_table(from_ts)
        else:
            logging.warning(f"[SKIP] No transactions collected — skipping Glue & Redshift load for {from_ts}")

    collect_and_save()


ethereum_block_collector_auto()
