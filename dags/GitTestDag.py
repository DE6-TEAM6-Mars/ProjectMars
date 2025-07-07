from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='print_korea_time_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # 매 5분마다 실행
    start_date=datetime(2025, 7, 6),
    catchup=False,
    tags=['test'],
) as dag:

    print_time = BashOperator(
        task_id='print_kst_time',
        bash_command='TZ=Asia/Seoul date'
    )