from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import logging

# 추가함
from datetime import timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook
import boto3

KST = timezone(timedelta(hours=9))

#신규 파일을 기존 테이블에 파티션으로 추가하는 함수.
def load_partition_and_table(from_ts: datetime) -> None:

    table_columns = [
        {"Name": "transactionhash", "Type": "string"},
        {"Name": "transactionindex", "Type": "string"},
        {"Name": "blockhash", "Type": "string"},
        {"Name": "blocknumber", "Type": "bigint"},
        {"Name": "from", "Type": "string"},
        {"Name": "to", "Type": "string"},
        {"Name": "value","Type": "string"},
        {"Name": "input","Type": "string"},
        {"Name": "functionselector","Type": "string"},
        {"Name": "nonce","Type": "string"},
        {"Name": "gas","Type": "string"},
        {"Name": "gasprice","Type": "string"},
        {"Name": "maxfeepergas","Type": "string"},
        {"Name": "maxpriorityfeepergas","Type": "string"},
        {"Name": "gasused","Type": "string"},
        {"Name": "cumulativegasused","Type": "string"},
        {"Name": "effectgasprice","Type": "string"},
        {"Name": "contractaddress","Type": "string"},
        {"Name": "type","Type": "string"},
        {"Name": "status","Type": "string"},
        {"Name": "logsbloom","Type": "string"},
        {"Name": "timestamp","Type": "string"},
        {"Name": "decodedinput","Type": "string"},
        {"Name": "accesslist","Type": "array<struct<address:string,storageKeys:array<string>>>"},
        {"Name": "authorizationlist","Type": "array<struct<chainId:string,nonce:string,address:string,yParity:string,r:string,s:string>>"},
        {"Name": "logs","Type": "array<struct<contractAddress:string,transactionHash:string,transactionIndex:bigint,blockHash:string,blockNumber:bigint,data:string,logIndex:bigint,removed:boolean,topics:array<string>,decodedLog:struct<name:string,eventFragment:string,signature:string,eventHash:string,args:array<struct<name:string,type:string,value:string>>>>>"}
	]

    kst_dt = from_ts.replace(tzinfo=timezone.utc).astimezone(KST)
    year = kst_dt.strftime('%Y')
    month = kst_dt.strftime('%m')
    day = kst_dt.strftime('%d')
    hour = kst_dt.strftime('%H')
    database_name = 'de6-team6-testdb'
    table_name = 'tb_eth_batch_transactions'
    partition_s3_path = f's3://de6-team6-bucket/eth/batch/year={year}/month={month}/day={day}/hour={hour}/'

    values = [year, month, day, hour]

    client = boto3.client('glue', region_name='ap-northeast-2')

    partition_exists = False
    try:
        # 파티션 존재 여부 확인
        client.get_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=values
        )
        print(f"[INFO] Partition already exists. : {values}")
        partition_exists = True
    except client.exceptions.EntityNotFoundException:
        # 파티션이 없다면 파티션 추가
        client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput={
                'Values': values,
                'StorageDescriptor': {
                    'Columns': table_columns,
                    'Location': partition_s3_path,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    }
                }
            }
        )

        print(f"[INFO] Partition {year}-{month}-{day} {hour} registration complete.")
    
    # Redshift에 데이터 로드
    hook = PostgresHook(postgres_conn_id='RedshiftConn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # 파티션이 존재하면 기존 데이터를 삭제하고 재 적재
    if(partition_exists):
        delete_sql = f"""
            DELETE FROM raw_data.tb_eth_batch_transactions
            WHERE key_year= '{year}' and key_month='{month}' and key_day = '{day}' and key_hour = '{hour}'
        """
        cursor.execute(delete_sql)
        print(f"[INFO] Existing data deleted for partition {year}-{month}-{day} {hour}.")

    # Spactrum에서 필요한 컬럼만 추려서 Redshift 테이블에 적재
    insert_sql = f"""
        INSERT INTO raw_data.tb_eth_batch_transactions (
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
            "year" AS key_year,
            "month" AS key_month,
            "day" AS key_day,
            "hour" AS key_hour,
            to_timestamp("timestamp", 'YYYY-MM-DD HH24:MI:SS') AS transaction_timestamp
        FROM spectrum.tb_eth_batch_transactions
        WHERE year = '{year}' AND month = '{month}' AND day = '{day}' AND hour = '{hour}'
    """
    cursor.execute(insert_sql)
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"[INFO] Data loaded into {database_name}.{table_name} for partition {year}-{month}-{day} {hour}.")



@dag(
    dag_id='ethereum_block_partition_load_test',
    start_date=datetime(2025, 7, 1, 1, 5),
    schedule_interval='5 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['ethereum', 'block', 'collector']
)
def ethereum_block_partition_load_test():
    @task()
    def load_test() -> None:
        context = get_current_context()
        execution_date: datetime = context["execution_date"]

        # UTC 기준의 execution_date를 사용 (Airflow 내부 시간 기준)
        aligned_execution = execution_date.replace(minute=0, second=0, microsecond=0)
        from_ts = aligned_execution - timedelta(hours=1)
        to_ts = aligned_execution

        load_partition_and_table(from_ts)

    load_test()


dag_instance = ethereum_block_partition_load_test()