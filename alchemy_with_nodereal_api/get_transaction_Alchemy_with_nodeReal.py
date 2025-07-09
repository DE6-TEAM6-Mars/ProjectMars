import os
import json
import time
import datetime
from multiprocessing import Pool, cpu_count
from typing import List, Sequence

import boto3
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from dotenv import load_dotenv

# -------------------------- 0. 기본 상수 --------------------------
BLOCK_DIGITS = 8
CHUNK_SIZE = 5000
FETCH_TIMEOUT = 20
SLEEP_BETWEEN_CYCLES = 5
MAX_RETRIES_PER_BLOCK = 3
MAX_RECOVERY_ROUNDS = 5
INITIAL_START_BLOCK = 1
INITIAL_END_BLOCK = 5_700_000
KST = datetime.timezone(datetime.timedelta(hours=9))


# --------------------------------------------------------------------------- #
# 1. 환경 변수 로드
# --------------------------------------------------------------------------- #

# API 키 및 S3 설정 로드
load_dotenv()
ALCHEMY_NODEREAL_KEYS: Sequence[str] = [k.strip() for k in os.getenv("ALCHEMY_NODEREAL_API_KEYS", "").split(",") if k.strip()]
KEY_COUNT = len(ALCHEMY_NODEREAL_KEYS)
if KEY_COUNT == 0:
    raise ValueError("ALCHEMY_NODEREAL_API_KEYS가 .env에 정의되지 않았습니다.")
MAX_WORKERS = KEY_COUNT * 4

S3_CONFIG = {
    "bucket_name": "de6-team6-bucket",
    "target_prefix": "eth/historical/",
    "access_key": os.getenv("S3_ACCESS_KEY"),
    "secret_key": os.getenv("S3_SECRET_KEY")
}


# -------------------------- 2. 경로 --------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMPRESSED_DIR = os.path.join(BASE_DIR, "../compressed_data")
STATE_FILE = os.path.join(BASE_DIR, "eth_processing_state.json")
FAILED_LOG = os.path.join(BASE_DIR, "failed_blocks.log")
os.makedirs(COMPRESSED_DIR, exist_ok=True)

# -------------------------- 3. 상태 관리 --------------------------
def save_state(next_block: int) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"next_fetch_block": next_block}, f)

def load_state(initial_block: int) -> int:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return int(json.load(f).get("next_fetch_block"))
        except Exception:
            print("[경고] 상태 파일 손상 → 초기화")
    save_state(initial_block)
    return initial_block

# -------------------------- 4. 유틸 --------------------------
def padding_for_filename(num: int) -> str:
    return f"{num:0{BLOCK_DIGITS}d}"

def hex_ts_to_kst_str(hex_ts: str) -> str:
    if not hex_ts:
        return ""
    utc_dt = datetime.datetime.fromtimestamp(int(hex_ts, 16), tz=datetime.timezone.utc)
    return utc_dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

def transform_tx(tx: dict, block_ts_hex: str) -> dict:
    numeric_fields = [
        "blockNumber", "gas", "gasPrice", "maxFeePerGas", "maxPriorityFeePerGas",
        "nonce", "transactionIndex", "value",
    ]
    new_tx = tx.copy()
    for f in numeric_fields:
        if f in tx and tx[f]:
            new_tx[f] = str(int(tx[f], 16))
    new_tx["timestamp"] = hex_ts_to_kst_str(block_ts_hex)
    return new_tx


def select_api_key(block_num: int, attempt: int = 0) -> str:
    idx = (block_num + attempt) % KEY_COUNT
    return ALCHEMY_NODEREAL_KEYS[idx]

# -------------------------- 5. 블록 다운로드 --------------------------

def download_block(block_num: int) -> tuple[int, List[dict]] | None:
    for attempt in range(MAX_RETRIES_PER_BLOCK):
        try:
            url = select_api_key(block_num, attempt)
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_getBlockByNumber", "params": [hex(block_num), True],
            }
            res = requests.post(url, json=payload, timeout=FETCH_TIMEOUT)
            res.raise_for_status()
            block = res.json().get("result", {})
            ts_hex = block.get("timestamp", "0x0")
            return block_num, [transform_tx(tx, ts_hex) for tx in block.get("transactions", [])]
        except Exception as e:
            print(f"[ERROR] 블록 {block_num} 실패: {e}")
            time.sleep(1)
    return None


def _parallel_download_with_recovery(start_blk: int, end_blk: int) -> List[dict]:
    blocks = list(range(start_blk, end_blk + 1))
    missing = blocks[:]
    tx_data = []

    for round_i in range(1, MAX_RECOVERY_ROUNDS + 1):
        print(f"[복구] 라운드 {round_i}: 블록 {len(missing)}개 요청")
        with Pool(processes=MAX_WORKERS) as pool:
            results = list(tqdm(pool.imap_unordered(download_block, missing), total=len(missing), desc=f"[다운로드: 라운드 {round_i} ]"))

        next_missing = []
        for result in results:
            if result is None:
                continue
            blk_num, txs = result
            tx_data.extend(txs)
            missing.remove(blk_num)

        if not missing:
            break

    if missing:
        with open(FAILED_LOG, "a", encoding="utf-8") as f:
            for blk in missing:
                f.write(f"{blk}\n")
        raise RuntimeError(f"[치명] 블록 {len(missing)}개 누락: {missing} → 로그 기록 완료")
    print(f"[완료] {len(tx_data)}개의 트랜잭션 다운로드 완료 ({len(missing)}개 누락)")
    return tx_data

# -------------------------- 6. Parquet 저장 + S3 업로드 --------------------------
def compress_parquet_and_upload(start_blk: int, end_blk: int, tx_data: List[dict]) -> None:
    pq_name = f"{padding_for_filename(start_blk)}_{padding_for_filename(end_blk)}.parquet"
    pq_path = os.path.join(COMPRESSED_DIR, pq_name)
    if tx_data:
        df = pd.DataFrae(tx_data)
        is_empty_flag="false"
    else:
        print("[주의] 저장할 트랜잭션 없음 — 빈 Parquet 파일 생성")
        df = pd.DaaFrame([], columns=["blockNumber"])    
        is_empty_flag="true"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, pq_path, compression='snappy')

    print(f"[압축] {pq_name} 생성 완료")

    s3_key = os.path.join(S3_CONFIG["target_prefix"], pq_name)
    boto3.client("s3",aws_access_key_id=S3_CONFIG["access_key"],aws_secret_access_key=S3_CONFIG["secret_key"]).upload_file(pq_path, S3_CONFIG["bucket_name"], s3_key,ExtraaArgs={"Metadata": {"is_empty": is_empty_flag}})
    print(f"[업로드] s3://{S3_CONFIG['bucket_name']}/{s3_key}")

    os.remove(pq_path) #필요없긴해
    print("[정리] 로컬 parquet 삭제 완료")

# -------------------------- 7. 메인 루프 --------------------------
def main():
    print("--- Lossless Downloader with Parquet + Recovery ---")
    start_block = load_state(INITIAL_START_BLOCK)
    while start_block <= INITIAL_END_BLOCK:
        end_block = min(start_block + CHUNK_SIZE - 1, INITIAL_END_BLOCK)
        print(f"\n[사이클] {start_block} → {end_block}")

        try:
            tx_data = _parallel_download_with_recovery(start_block, end_block)
            compress_parquet_and_upload(start_block, end_block, tx_data)
        except Exception as e:
            print(f"[오류] 처리 중단: {e}")
            break

        next_start = end_block + 1
        save_state(next_start)
        start_block = next_start

        print("[휴식] 다음 루프까지 5초…")
        time.sleep(SLEEP_BETWEEN_CYCLES)

    print("프로그램 종료.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[중단] 사용자가 종료했습니다. 진행 상황은 저장되었습니다.")
