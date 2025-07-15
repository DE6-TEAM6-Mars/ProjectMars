import logging
from datetime import datetime, timedelta, timezone

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

KST = timezone(timedelta(hours=9))

@dag(
    dag_id='ethereum_realtime_batch_processor',
    start_date=datetime(2025, 7, 1),
    schedule_interval='15 * * * *',
    catchup=True,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
    },
    tags=['ethereum', 'redshift', 'batch'],
    doc_md="""
    ### Ethereum Realtime Batch Processor
    - S3에 저장된 Ethereum 트랜잭션 Parquet 파일을 Redshift `raw_data.realtime_transaction` 테이블에 적재합니다.
    """
)
def ethereum_realtime_batch_processor():
    """
    S3의 Raw Transaction 데이터를 Redshift에 배치 처리하는 DAG
    """

    @task
    def load_s3_to_redshift(logical_date: str):
        """
        S3 Parquet 파일을 Redshift 테이블에 COPY 합니다.
        - Staging Table을 사용하여 트랜잭션의 원자성을 보장합니다.
        - 동일한 시간대의 데이터가 존재하면, 기존 데이터를 삭제하고 새로운 데이터로 교체합니다.
        """
        execution_date = datetime.fromisoformat(logical_date)
        logging.info(f"Load to Redshift 태스크 시작. Execution Date: {execution_date}")

        # --- 1. S3 경로 및 변수 설정 ---
        s3_bucket = Variable.get("S3_BUCKET_NAME")
        redshift_conn_id = "RedshiftConn"
        aws_conn_id = "S3Conn" 

        kst_dt = execution_date.replace(tzinfo=timezone.utc).astimezone(KST)
        year, month, day = kst_dt.strftime('%Y'), kst_dt.strftime('%m'), kst_dt.strftime('%d')
        
        base_filename = f"ETH_{kst_dt.strftime('%Y%m%d_%H')}"
        parquet_filename = f"{base_filename}.parquet"
        empty_filename = f"{base_filename}.empty"

        s3_parquet_key = f"eth/batch/{year}/{month}/{day}/{parquet_filename}"
        s3_empty_key = f"eth/batch/{year}/{month}/{day}/{empty_filename}"
        s3_full_path = f"s3://{s3_bucket}/{s3_parquet_key}"

        # --- 2. S3 파일 존재 여부 확인 ---
        s3_client = boto3.client('s3')

        # 데이터가 없는 경우(.empty 파일) 처리
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=s3_empty_key)
            logging.info(f"데이터 없음(.empty) 파일 발견: {s3_empty_key}. 작업을 건너뜁니다.")
            return
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] != "404":
                raise

        # 데이터 파일이 없는 경우 에러 처리
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=s3_parquet_key)
            logging.info(f"처리할 데이터 파일 발견: {s3_full_path}")
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logging.error(f"S3에 데이터 파일이 없습니다: {s3_full_path}")
                raise FileNotFoundError(f"Source file not found at {s3_full_path}")
            raise

        # --- 3. Redshift에 데이터 적재 ---
        redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        credentials = s3_hook.get_credentials()
        access_key, secret_key = credentials.access_key, credentials.secret_key
        
        target_table = "raw_data.realtime_transaction"
        staging_table_name = "staging_realtime_transaction_temp"
        
        create_staging_sql = f"""
        CREATE TEMP TABLE {staging_table_name} (
                transactionHash         VARCHAR(256),
            transactionIndex        BIGINT,
            blockHash               VARCHAR(256),
            blockNumber             BIGINT,
            "from"                  VARCHAR(256),
            "to"                    VARCHAR(256),
            value                   DOUBLE PRECISION,
            input                   SUPER,
            functionSelector        VARCHAR(256),
            nonce                   BIGINT,
            gas                     BIGINT,
            gasPrice                BIGINT,
            maxFeePerGas            DOUBLE PRECISION,
            maxPriorityFeePerGas    DOUBLE PRECISION,
            gasUsed                 BIGINT,
            cumulativeGasUsed       BIGINT,
            effectGasPrice          BIGINT,
            contractAddress         VARCHAR(256),
            type                    BIGINT,
            status                  BIGINT,
            logsBloom               VARCHAR(8192),
            timestamp               BIGINT,
            decodedInput            SUPER,
            accessList              SUPER,
            authorizationList       SUPER,
            logs                    SUPER
        );
        """

        # 스키마가 정확히 일치하지 않아도 VARCHAR로 다 받기 때문에 옵션이 필요 없습니다.
        copy_sql = f"""
        COPY {staging_table_name}
        FROM '{s3_full_path}'
        CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
        FORMAT AS PARQUET
        SERIALIZETOJSON
        TIMEFORMAT 'epochmicros'; 
        """
        
        # 멱등성을 위한 DELETE (최종 테이블 대상)
        delete_sql = f"""
        DELETE FROM {target_table}
        WHERE "timestamp" >= TO_TIMESTAMP('{execution_date - timedelta(hours=1)}', 'YYYY-MM-DD HH24:MI:SS')
          AND "timestamp" < TO_TIMESTAMP('{execution_date}', 'YYYY-MM-DD HH24:MI:SS');
        """

        insert_sql = f"""
        INSERT INTO {target_table} ("timestamp", "value", "from", "to", "blockNumber", "status")
        SELECT
            TIMESTAMP 'epoch' + ("timestamp" / 1000000) * INTERVAL '1 second',
            "value",
            "from",
            "to",
            "blockNumber",
            CASE
                WHEN status = '0x1' THEN '1'
                ELSE '0'
            END AS status
        FROM {staging_table_name};
        """

        conn = redshift_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                logging.info("1. Staging 테이블 생성...")
                cursor.execute(create_staging_sql)
                
                logging.info(f"2. Staging 테이블로 데이터 COPY...")
                cursor.execute(copy_sql)
                
                logging.info("3. Target 테이블의 기존 데이터 삭제...")
                cursor.execute(delete_sql)
                
                logging.info("4. Target 테이블로 변환하며 데이터 INSERT...")
                cursor.execute(insert_sql)

            logging.info("모든 작업 성공. 트랜잭션을 COMMIT 합니다.")
            conn.commit()

        except Exception as e:
            logging.error(f"Redshift 작업 중 오류 발생: {e}")
            conn.rollback()
            raise
        finally:
            logging.info("Redshift Connection을 종료합니다.")
            conn.close()
    load_s3_to_redshift(logical_date="{{ logical_date }}")
ethereum_realtime_batch_processor()