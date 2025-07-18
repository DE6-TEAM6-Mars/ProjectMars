import requests
import pandas as pd
import json
import boto3
import io
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from itertools import cycle
from airflow.models import Variable
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from airflow.providers.postgres.hooks.postgres import PostgresHook

ETHERSCAN_API_KEY = Variable.get("ETHERSCAN_API_KEY")
NODIT_API_KEYS = json.loads(Variable.get("NODIT_API_KEYS", default_var="[]"))
KST = timezone(timedelta(hours=9))


def find_block_by_timestamp(target_ts: int, closest: str = "before") -> Optional[int]:
    url = "https://api.etherscan.io/api"
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": target_ts,
        "closest": closest,
        "apikey": ETHERSCAN_API_KEY
    }

    try:
        res = requests.get(url, params=params, timeout=10).json()
        print(f"[DEBUG] Etherscan ts={target_ts}, closest={closest} → res={res}")
        if res.get("status") == "1":
            return int(res["result"])
        else:
            print(f"[!] Etherscan Error: {res.get('message')}")
    except Exception as e:
        print(f"[!] Etherscan request failed: {e}")
    return None


def collect_raw_transactions(from_ts: datetime, to_ts: datetime) -> List[Dict[str, Any]]:
    start_unix = int(from_ts.timestamp())
    end_unix = int(to_ts.timestamp())

    start_block = find_block_by_timestamp(start_unix, closest="before")
    end_block = find_block_by_timestamp(end_unix, closest="before")

    if start_block is None or end_block is None:
        raise ValueError(f"❌ Block not found. start_block={start_block}, end_block={end_block}")

    if not NODIT_API_KEYS:
        raise ValueError("❌ Airflow Variable 'NODIT_API_KEYS' is not configured or empty")

    print(f"[INFO] Collecting from block {start_block} to {end_block} ({end_block - start_block + 1} blocks)")

    api_cycle = cycle(NODIT_API_KEYS)
    raw_txs: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(fetch_block, blk, api_cycle, start_unix, end_unix)
            for blk in range(start_block, end_block + 1)
        ]
        for future in as_completed(futures):
            raw_txs.extend(future.result())

    print(f"[INFO] Total {len(raw_txs)} transactions collected.")
    return raw_txs


def fetch_block(block_number: int, api_cycle, start_unix: int, end_unix: int) -> List[Dict[str, Any]]:
    key = next(api_cycle)
    url = "https://web3.nodit.io/v1/ethereum/mainnet/blockchain/getTransactionsInBlock"
    headers = {
        "X-API-KEY": key,
        "Content-Type": "application/json"
    }

    print(f"[FETCH] Start block={block_number}, API Key=****{key[-4:]}")
    txs_collected = []
    cursor = None
    prev_cursors = set()

    while True:
        payload = {
            "block": block_number,
            "withLogs": True,           # ✅ 반드시 추가
            "withDecode": True,         # ✅ 반드시 추가
            "rpp": 1000
        }
        if cursor:
            payload["cursor"] = cursor

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=10)

            if resp.status_code == 429:
                print(f"[RATE LIMIT] 429 Too Many Requests → block={block_number}, retrying...")
                time.sleep(5)
                continue

            resp.raise_for_status()
            data = resp.json()

            txs = data.get("items", [])
            if not txs:
                print(f"[SKIP] Block {block_number} → No txs found.")
                break

            # ✅ timestamp는 트랜잭션 첫 항목에서 가져옴
            block_ts = txs[0].get("timestamp")
            if block_ts is None or not (start_unix <= block_ts < end_unix):
                print(f"[SKIP] Block {block_number} ts={block_ts} OUT OF RANGE")
                break

            print(f"[FETCH] Block {block_number}, cursor={cursor} → {len(txs)} txs")

            for tx in txs:
                tx["timestamp"] = datetime.fromtimestamp(
                    block_ts, tz=timezone.utc
                ).astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')
            txs_collected.extend(txs)

            cursor = data.get("nextCursor")
            if not cursor:
                break
            if cursor in prev_cursors:
                print(f"[!] Cursor loop detected: block={block_number}, cursor={cursor}")
                break
            prev_cursors.add(cursor)

        except Exception as e:
            print(f"[ERROR] Block {block_number} fetch failed: {e}")
            break

    print(f"[FETCH-END] block={block_number}, total txs={len(txs_collected)}")
    return txs_collected


def upload_to_s3(txs: List[Dict[str, Any]], from_ts: datetime) -> None:
    kst_dt = from_ts.replace(tzinfo=timezone.utc).astimezone(KST)
    year = kst_dt.strftime('%Y')
    month = kst_dt.strftime('%m')
    day = kst_dt.strftime('%d')
    hour = kst_dt.strftime('%H')
    base_filename = f"ETH_{kst_dt.strftime('%Y%m%d_%H')}"
    partition_prefix = f"eth/batch/year={year}/month={month}/day={day}/hour={hour}"
    s3_key = f"{partition_prefix}/{base_filename}.parquet"
    empty_key = f"{partition_prefix}/{base_filename}.empty"

    bucket = Variable.get("S3_BUCKET_NAME")
    access_key = Variable.get("AWS_ACCESS_KEY_ID")
    secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # 이미 업로드된 파일이면 skip
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        print(f"[!] File already exists at s3://{bucket}/{s3_key} — skipping upload.")
        return
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] != "404":
            raise

    # 트랜잭션이 없으면 .empty 파일 업로드
    if not txs:
        print("[!] No transactions to upload.")
        try:
            s3.put_object(Bucket=bucket, Key=empty_key, Body=b'')
            print(f"[2] Uploaded empty marker to s3://{bucket}/{empty_key}")
        except Exception as e:
            print(f"[!] Failed to upload empty marker: {e}")
            raise
        return

    # 트랜잭션이 있으면 Parquet 변환 & 업로드 (Arrow 기반)
    try:
        df = pd.DataFrame(txs).fillna("")
        # Arrow 스키마 정의
        arrow_schema = pa.schema([
            ("transactionHash", pa.string()),
            ("transactionIndex", pa.string()),
            ("blockHash", pa.string()),
            ("blockNumber", pa.int64()),
            ("from", pa.string()),
            ("to", pa.string()),
            ("value", pa.string()),
            ("input", pa.string()),
            ("functionSelector", pa.string()),
            ("nonce", pa.string()),
            ("gas", pa.string()),
            ("gasPrice", pa.string()),
            ("maxFeePerGas", pa.string()),
            ("maxPriorityFeePerGas", pa.string()),
            ("gasUsed", pa.string()),
            ("cumulativeGasUsed", pa.string()),
            ("effectGasPrice", pa.string()),
            ("contractAddress", pa.string()),
            ("type", pa.string()),
            ("status", pa.string()),
            ("logsBloom", pa.string()),
            ("timestamp", pa.string()),
            ("decodedInput", pa.string()),
            ("accessList", pa.list_(
                pa.struct([
                    ("address", pa.string()),
                    ("storageKeys", pa.list_(pa.string()))
                ])
            )),
            ("authorizationList", pa.list_(
                pa.struct([
                    ("chainId", pa.string()),
                    ("nonce", pa.string()),
                    ("address", pa.string()),
                    ("yParity", pa.string()),
                    ("r", pa.string()),
                    ("s", pa.string()),
                ])
            )),
            ("logs", pa.list_(
                pa.struct([
                    ("contractAddress", pa.string()),
                    ("transactionHash", pa.string()),
                    ("transactionIndex", pa.int64()),
                    ("blockHash", pa.string()),
                    ("blockNumber", pa.int64()),
                    ("data", pa.string()),
                    ("logIndex", pa.int64()),
                    ("removed", pa.bool_()),
                    ("topics", pa.list_(pa.string())),
                    ("decodedLog", pa.struct([
                        ("name", pa.string()),
                        ("eventFragment", pa.string()),
                        ("signature", pa.string()),
                        ("eventHash", pa.string()),
                        ("args", pa.list_(
                            pa.struct([
                                ("name", pa.string()),
                                ("type", pa.string()),
                                ("value", pa.string())
                            ])
                        )),
                    ]))
                ])
            ))
        ])

        # 누락된 컬럼 채우기
        for field in arrow_schema:
            if field.name not in df.columns:
                df[field.name] = None

        # decodedInput 직렬화
        if "decodedInput" in df.columns:
            df["decodedInput"] = df["decodedInput"].apply(lambda v: json.dumps(v, ensure_ascii=False))

        # Arrow 테이블로 변환
        table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        # 업로드
        s3.upload_fileobj(buffer, bucket, s3_key)
        print(f"[2] Uploaded to s3://{bucket}/{s3_key}")

        # empty marker 제거 시도
        try:
            s3.delete_object(Bucket=bucket, Key=empty_key)
            print(f"[CLEANUP] Removed empty marker s3://{bucket}/{empty_key}")
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                print(f"[!] Failed to delete empty marker: {e}")

    except Exception as e:
        print(f"[!] Failed to upload parquet to S3: {e}")
        raise

def load_partition_and_table(from_ts: datetime) -> None:
    table_columns = [
        {"Name": "transactionhash", "Type": "string"},
        {"Name": "transactionindex", "Type": "string"},
        {"Name": "blockhash", "Type": "string"},
        {"Name": "blocknumber", "Type": "bigint"},
        {"Name": "from", "Type": "string"},
        {"Name": "to", "Type": "string"},
        {"Name": "value", "Type": "string"},
        {"Name": "input", "Type": "string"},
        {"Name": "functionselector", "Type": "string"},
        {"Name": "nonce", "Type": "string"},
        {"Name": "gas", "Type": "string"},
        {"Name": "gasprice", "Type": "string"},
        {"Name": "maxfeepergas", "Type": "string"},
        {"Name": "maxpriorityfeepergas", "Type": "string"},
        {"Name": "gasused", "Type": "string"},
        {"Name": "cumulativegasused", "Type": "string"},
        {"Name": "effectgasprice", "Type": "string"},
        {"Name": "contractaddress", "Type": "string"},
        {"Name": "type", "Type": "string"},
        {"Name": "status", "Type": "string"},
        {"Name": "logsbloom", "Type": "string"},
        {"Name": "timestamp", "Type": "string"},
        {"Name": "decodedinput", "Type": "string"},
        {"Name": "accesslist", "Type": "array<struct<address:string,storageKeys:array<string>>>"},
        {"Name": "authorizationlist", "Type": "array<struct<chainId:string,nonce:string,address:string,yParity:string,r:string,s:string>>"},
        {"Name": "logs", "Type": "array<struct<contractAddress:string,transactionHash:string,transactionIndex:bigint,blockHash:string,blockNumber:bigint,data:string,logIndex:bigint,removed:boolean,topics:array<string>,decodedLog:struct<name:string,eventFragment:string,signature:string,eventHash:string,args:array<struct<name:string,type:string,value:string>>>>>"}
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

    access_key = Variable.get("AWS_ACCESS_KEY_ID")
    secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError("❌ AWS 자격 증명이 Airflow Variables에 없습니다.")

    client = boto3.client('glue', region_name='ap-northeast-2')

    partition_exists = False
    try:
        client.get_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=values
        )
        print(f"[INFO] Partition already exists. : {values}")
        partition_exists = True
    except client.exceptions.EntityNotFoundException:
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
                        'Parameters': {'serialization.format': '1'}
                    }
                }
            }
        )
        print(f"[INFO] Partition {year}-{month}-{day} {hour} registration complete.")

    # Redshift 적재
    hook = PostgresHook(postgres_conn_id='RedshiftConn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    if partition_exists:
        delete_sql = f"""
            DELETE FROM raw_data.tb_eth_batch_transactions
            WHERE key_year= '{year}' and key_month='{month}' and key_day = '{day}' and key_hour = '{hour}'
        """
        cursor.execute(delete_sql)
        print(f"[INFO] Existing data deleted for partition {year}-{month}-{day} {hour}.")

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