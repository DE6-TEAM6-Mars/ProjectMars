import requests
import pandas as pd
import json
import boto3
import io
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from airflow.models import Variable
import pyarrow as pa
import pyarrow.parquet as pq


# 환경 변수 불러오기
NODEREAL_RPC = Variable.get("NODEREAL_RPC")
ETHERSCAN_API_KEY = Variable.get("ETHERSCAN_API_KEY")
KST = timezone(timedelta(hours=9))


# RPC 호출
def call_rpc(method: str, params: List[Any]) -> Optional[Dict[str, Any]]:
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    }
    try:
        res = requests.post(NODEREAL_RPC, json=payload, timeout=10)
        return res.json().get("result")
    except Exception as e:
        print(f"[!] RPC call failed for method={method}, params={params}, error={e}")
        return None


# 타임스탬프 → KST 문자열
def convert_hex_timestamp_to_kst_str(hex_timestamp: str) -> Optional[str]:
    if not hex_timestamp:
        return None
    try:
        utc_dt = datetime.fromtimestamp(int(hex_timestamp, 16), tz=timezone.utc)
        return utc_dt.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return None


# 블록 조회
def get_block_by_number(block_number: int) -> Optional[Dict[str, Any]]:
    hex_num = hex(block_number)
    return call_rpc("eth_getBlockByNumber", [hex_num, True])


# 타임스탬프 → 블록 번호 (Etherscan)
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
        print(f"[DEBUG] Etherscan request for ts={target_ts}, closest={closest} → res={res}")

        if res.get("status") == "1":
            return int(res["result"])
        else:
            print(f"[!] Etherscan Error: {res.get('message')}, full response: {res}")
    except Exception as e:
        print(f"[!] Etherscan request failed: {e}")

    return None


# 트랜잭션 필드 정제 + 타임스탬프 포함
def transform_transaction(tx: Dict[str, Any], block_ts_hex: str) -> Dict[str, Any]:
    numeric_fields = [
        'blockNumber', 'gas', 'gasPrice', 'maxFeePerGas',
        'maxPriorityFeePerGas', 'nonce', 'transactionIndex', 'value'
    ]
    transformed_tx = tx.copy()
    for field in numeric_fields:
        if field in transformed_tx and transformed_tx[field]:
            try:
                transformed_tx[field] = str(int(transformed_tx[field], 16))
            except Exception:
                pass
    transformed_tx["timestamp"] = convert_hex_timestamp_to_kst_str(block_ts_hex)
    return transformed_tx


# 시간 범위 내의 정제된 트랜잭션 수집
def collect_raw_transactions(from_ts: datetime, to_ts: datetime) -> List[Dict[str, Any]]:
    start_unix = int(from_ts.timestamp())
    end_unix = int(to_ts.timestamp())

    start_block = find_block_by_timestamp(start_unix, closest="before")
    end_block = find_block_by_timestamp(end_unix, closest="before")

    if start_block is None or end_block is None:
        print(f"[!] Block not found. start_block={start_block}, end_block={end_block}")
        return []

    print(f"[INFO] Collecting from block {start_block} to {end_block} ({end_block - start_block + 1} blocks)")

    raw_txs: List[Dict[str, Any]] = []
    for block_number in range(start_block, end_block + 1):
        block = get_block_by_number(block_number)
        if not block:
            print(f"[!] Block {block_number} 조회 실패")
            continue
        block_ts_hex = block["timestamp"]
        block_ts_int = int(block_ts_hex, 16)
        if start_unix <= block_ts_int < end_unix:
            for tx in block.get("transactions", []):
                tx["blockNumber"] = block["number"]
                transformed = transform_transaction(tx, block_ts_hex)
                raw_txs.append(transformed)

    return raw_txs


# S3 업로드
def upload_to_s3(txs: List[Dict[str, Any]], execution_date: datetime) -> None:
    # KST 기준 경로 구성
    kst_dt = execution_date.replace(tzinfo=timezone.utc).astimezone(KST)
    year = kst_dt.strftime('%Y')
    month = kst_dt.strftime('%m')
    day = kst_dt.strftime('%d')
    base_filename = f"ETH_{kst_dt.strftime('%Y%m%d_%H')}"
    jsonl_path = f"/tmp/{base_filename}.jsonl"
    parquet_filename = f"{base_filename}.parquet"
    empty_filename = f"{base_filename}.empty"
    s3_key = f"eth/{year}/{month}/{day}/{parquet_filename}"
    empty_key = f"eth/{year}/{month}/{day}/{empty_filename}"

    # S3 설정
    bucket = Variable.get("S3_BUCKET_NAME")
    access_key = Variable.get("AWS_ACCESS_KEY_ID")
    secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # ✅ 중복 방지: S3에 파일이 이미 존재하는 경우 생략
    try:
        s3.head_object(Bucket=bucket, Key=s3_key)
        print(f"[!] File already exists at s3://{bucket}/{s3_key} — skipping upload.")
        return
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] != "404":
            raise

    # ✅ txs가 비어 있을 경우 .empty 파일로 기록 후 종료
    if not txs:
        print("[!] No transactions to upload.")
        try:
            s3.put_object(Bucket=bucket, Key=empty_key, Body=b'')
            print(f"[2] Uploaded empty marker to s3://{bucket}/{empty_key}")
        except Exception as e:
            print(f"[!] Failed to upload empty marker: {e}")
        return

    try:
        # 1. JSONL 파일로 저장
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for tx in txs:
                f.write(json.dumps(tx, ensure_ascii=False) + '\n')
        print(f"[1] JSONL 저장 완료: {jsonl_path}")

        # 2. JSONL → DataFrame
        df = pd.read_json(jsonl_path, lines=True)

        # 3. DataFrame → Parquet
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)

        # 4. Parquet → S3 업로드
        s3.upload_fileobj(buffer, bucket, s3_key)
        print(f"[2] Uploaded to s3://{bucket}/{s3_key}")

    except Exception as e:
        print(f"[!] Failed to upload parquet to S3: {e}")
