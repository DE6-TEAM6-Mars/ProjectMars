from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    dag_id='copy_eth_data_to_redshift',
    default_args=default_args,
    start_date=datetime(2015, 1, 1),
    schedule_interval=None,  # 필요시 '@daily' 등으로 설정
    catchup=False
)

def copy_parquet_to_redshift(**kwargs):
    from datetime import datetime, timedelta

    # 날짜 구간
    start_date = datetime(2015, 7, 8)
    end_date = datetime(2015, 7, 9)

    bucket = "de6-team6-bucket"
    s3_prefix = "output/eth_transactions_parquet"
    iam_role = "arn:aws:iam::862327261051:role/de6-team6-airflow-s3-access"#이거 안되면 s3 access key랑 secret key로 바꿔야함
    target_table = "raw_data.tb_eth_transactions_test_parquet" # Redshift 원하는 타겟 테이블

    conn = psycopg2.connect(
        dbname="dev",
        user="jhw",
        password="******",
        host="de4mars.duckdns.org",
        port=5439
    )
    cursor = conn.cursor()

    current = start_date
    while current <= end_date:
        year = current.year
        month = current.month
        day = current.day

        s3_path = f"s3://{bucket}/{s3_prefix}/year={year}/month={month}/day={day}/"
        copy_sql = f"""
        COPY {target_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
        """
        try:
            print(f"📦 COPY {current.strftime('%Y-%m-%d')}")
            cursor.execute(copy_sql)
            conn.commit()
        except Exception as e:
            print(f"❌ Failed on {current.strftime('%Y-%m-%d')}: {e}")
            conn.rollback()
        current += timedelta(days=1)

    cursor.close()
    conn.close()

# DAG 태스크 정의
copy_task = PythonOperator(
    task_id='copy_to_redshift',
    python_callable=copy_parquet_to_redshift,
    dag=dag
)
