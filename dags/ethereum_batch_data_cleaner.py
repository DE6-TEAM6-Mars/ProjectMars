from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3

# 기본 설정
bucket = "de6-team6-bucket"
glue_db = "de6-team6-testdb"
glue_table = "tb_eth_batch_transactions"
region = "ap-northeast-2"

# KST 기준 범위
START = datetime(2025, 6, 3, 0)
END = datetime(2025, 6, 16, 5)

def delete_all_batch_data():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # S3 & Glue 클라이언트 생성
    access_key = Variable.get("AWS_ACCESS_KEY_ID")
    secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client("s3", region_name=region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    glue = boto3.client("glue", region_name=region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)

    redshift = PostgresHook(postgres_conn_id="RedshiftConn")

    current = START
    while current <= END:
        year = current.strftime("%Y")
        month = current.strftime("%m")
        day = current.strftime("%d")
        hour = current.strftime("%H")
        prefix = f"eth/batch/year={year}/month={month}/day={day}/hour={hour}/"
        base_filename = f"ETH_{year}{month}{day}_{hour}"

        # 1. S3 파일 삭제 (.parquet, .empty)
        for ext in [".parquet", ".empty"]:
            key = f"{prefix}{base_filename}{ext}"
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                print(f"✅ S3 삭제됨: {key}")
            except s3.exceptions.NoSuchKey:
                print(f"⛔ S3 파일 없음: {key}")
            except Exception as e:
                print(f"❌ S3 삭제 오류: {key}, error={e}")

        # 2. Glue 파티션 삭제
        try:
            glue.delete_partition(
                DatabaseName=glue_db,
                TableName=glue_table,
                PartitionValues=[year, month, day, hour]
            )
            print(f"✅ Glue 파티션 삭제됨: {year}-{month}-{day} {hour}")
        except glue.exceptions.EntityNotFoundException:
            print(f"⛔ Glue 파티션 없음: {year}-{month}-{day} {hour}")
        except Exception as e:
            print(f"❌ Glue 삭제 오류: {e}")

        # 3. Redshift 레코드 삭제
        try:
            sql = f"""
                DELETE FROM raw_data.tb_eth_batch_transactions
                WHERE key_year='{year}' AND key_month='{month}'
                AND key_day='{day}' AND key_hour='{hour}';
            """
            redshift.run(sql)
            print(f"✅ Redshift 레코드 삭제됨: {year}-{month}-{day} {hour}")
        except Exception as e:
            print(f"❌ Redshift 삭제 오류: {e}")

        current += timedelta(hours=1)


with DAG(
    dag_id="ethereum_batch_data_cleaner",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
    tags=["eth", "cleanup", "s3", "glue", "redshift"]
) as dag:

    cleanup_task = PythonOperator(
        task_id="delete_all_batch_data",
        python_callable=delete_all_batch_data
    )
