"""
Nodit Lossless Ethereum Block Downloader (Asyncio + Parquet + S3)
===============================================================
- 비동기 aiohttp + aiolimiter 로 Nodit 2 req/s 제한 극복
- 블록당 페이지(cursor) 병렬 처리
- 상태 파일로 중단 시 재개
- Parquet + Snappy 압축 후 S3 업로드

필수 환경변수 (.env)
--------------------
NODEIT_API_KEYS="k1,k2,k3,..."
S3_ACCESS_KEY="..."
S3_SECRET_KEY="..."
BUCKET_NAME="de6-team6-bucket"
TARGET_PREFIX="eth/historical/"

"""

import os, json, time, asyncio, datetime, logging, math
from typing import List, Dict, Optional, Sequence, Tuple, Any

import aiohttp
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm
from dotenv import load_dotenv

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3

# ─────────────────────────────────────────────────────────────
# 0. 기본 상수 & 설정
# ─────────────────────────────────────────────────────────────
BLOCK_DIGITS = 8
CHUNK_SIZE   = 1000                 # 블록 배치 크기
FETCH_TIMEOUT= 20
MAX_RETRIES  = 1
KST          = datetime.timezone(datetime.timedelta(hours=9))

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(BASE_DIR, "../compressed_data")
STATE_FILE   = os.path.join(BASE_DIR, "eth_processing_state.json")
FAILED_LOG   = os.path.join(BASE_DIR, "failed_blocks.log")
# ────────────────────────────────
# PyArrow 스키마
# ────────────────────────────────
schema = pa.schema([
    ("transactionHash", pa.string()),
    ("transactionIndex", pa.string()),
    ("blockHash", pa.string()),
    ("blockNumber", pa.string()),
    ("from", pa.string()),
    ("to", pa.string()),
    ("value", pa.string()),
    ("input", pa.string()),
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
    ("accessList", pa.list_(pa.struct([
        ("address", pa.string()),
        ("storageKeys", pa.list_(pa.string()))
    ]))),
    ("logs", pa.list_(pa.struct([
        ("contractAddress", pa.string()),
        ("transactionHash", pa.string()),
        ("transactionIndex", pa.int64()),
        ("blockHash", pa.string()),
        ("blockNumber", pa.int64()),
        ("data", pa.string()),
        ("logIndex", pa.int64()),
        ("removed", pa.bool_()),
        ("topics", pa.list_(pa.string()))
    ])))
])

URL_NodeReal = "https://eth-mainnet.nodereal.io/v1/"
URL_Alchemy = "https://eth-mainnet.g.alchemy.com/v2/"


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────
# 1. 환경 변수
# ─────────────────────────────────────────────────────────────
load_dotenv()
NODEREAL_API_KEYS: Sequence[str] = [k.strip() for k in os.getenv("NODEREAL_API_KEYS", "").split(",") if k.strip()]
if not NODEREAL_API_KEYS:
    raise ValueError("NODEREAL_API_KEYS not set in .env")
KEY_COUNT = len(NODEREAL_API_KEYS)
# Alchemy 및 NodeReal 키 로드 및 리미터 설정
ALCHEMY_API_KEYS: Sequence[str] = [k.strip() for k in os.getenv("ALCHEMY_API_KEYS", "").split(",") if k.strip()]
if not ALCHEMY_API_KEYS:
    raise ValueError("ALCHEMY_API_KEYS not set in .env")
print(len(ALCHEMY_API_KEYS), "Alchemy API keys loaded")
alchemy_limiters = {k: AsyncLimiter(6, 1) for k in ALCHEMY_API_KEYS}  # 5 req/sec
nodereal_limiters = {k: AsyncLimiter(6, 1) for k in NODEREAL_API_KEYS}  # 6 req/sec

from itertools import cycle
alchemy_key_iter = cycle(ALCHEMY_API_KEYS)
nodereal_key_iter = cycle(NODEREAL_API_KEYS)

S3_CONFIG = {
    "bucket_name": os.getenv("BUCKET_NAME", "de6-team6-bucket"),
    "target_prefix": os.getenv("TARGET_PREFIX", "eth/historical/"),
    "access_key": os.getenv("S3_ACCESS_KEY"),
    "secret_key": os.getenv("S3_SECRET_KEY"),
}

os.makedirs(DATA_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# 2. 유틸 함수
# ─────────────────────────────────────────────────────────────

def pad(num: int) -> str:
    return f"{num:0{BLOCK_DIGITS}d}"

def hex_ts_to_kst_str(hex_ts: str) -> str:
    if not hex_ts:
        return ""
    utc_dt = datetime.datetime.fromtimestamp(int(hex_ts, 16), tz=datetime.timezone.utc)
    return utc_dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

def hex_to_str(value: Optional[str]) -> str: #10진수로 변환후 string으로 변환
    if not value:
        return ""
    return str(int(value, 16))
def convert_logs(logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def safe_int(hex_str):
        try:
            return int(hex_str, 16)
        except:
            return None

    converted = []
    for log in logs:
        converted.append({
            "contractAddress": log.get("address"),
            "transactionHash": log.get("transactionHash"),
            "transactionIndex": safe_int(log.get("transactionIndex")),
            "blockHash": log.get("blockHash"),
            "blockNumber": safe_int(log.get("blockNumber")),
            "data": log.get("data"),
            "logIndex": safe_int(log.get("logIndex")),
            "removed": log.get("removed", False),
            "topics": log.get("topics", [])
        })
    return converted


## 반환하는 형식 변환함수인데 이거 조정 필요
def transform_tx(tx: Dict, blk_ts: int|str):
    new_tx = tx.copy()
    new_tx["timestamp"] = hex_ts_to_kst_str(blk_ts)
    return new_tx

# ─────────────────────────────────────────────────────────────
# 3. 상태 저장/로드
# ─────────────────────────────────────────────────────────────

def save_state(next_block: int):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"next_fetch_block": next_block}, f)

def load_state(initial: int) -> int:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return int(json.load(f).get("next_fetch_block", initial))
        except Exception:
            logging.warning("State file corrupted. Resetting.")
    save_state(initial)
    return initial

# ─────────────────────────────────────────────────────────────
# 4. Nodit API 호출 (비동기)
# ─────────────────────────────────────────────────────────────

async def _post(session: aiohttp.ClientSession, key: str, payload: Dict[str, Any], url: str, limiter_dict: Dict[str, AsyncLimiter]) -> Optional[Dict]:
    for attempt in range(1, MAX_RETRIES+1):
        try:
            async with limiter_dict[key]:
                hdr = {"Content-Type": "application/json"}
                async with session.post(url+key, headers=hdr, json=payload, timeout=FETCH_TIMEOUT) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except Exception as e:
            logging.warning(f"[Retry {attempt}] API call failed: {e}")
            if attempt == MAX_RETRIES:
                return None 
            await asyncio.sleep(1)



# ────────────────────────────────
# Block + Receipt 수집
# ────────────────────────────────


async def get_block(session: aiohttp.ClientSession, key: str, block_number: int) -> Dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": [hex(block_number), True]}
    res = await _post(session, key, payload, URL_NodeReal, nodereal_limiters)
    if not res or "result" not in res:
        return {}
    return res["result"]



async def get_receipt(session: aiohttp.ClientSession, key: str, block_number: int) -> Tuple[Optional[Dict], bool]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockReceipts", "params": [hex(block_number)]}
    try:
        res = await _post(session, key, payload, URL_Alchemy, alchemy_limiters)
        return res.get("result", {}), True
    except Exception:
        return None, False


# fetch_block_and_receipts 내부 receipt 수집 부분 수정
async def fetch_block_and_receipts(session: aiohttp.ClientSession, block_number: int,  nodereal_key: str, alchemy_key: str) -> Tuple[int, Optional[List[Dict[str, Any]]]]:
    try:
        block_resp = await get_block(session, nodereal_key, block_number)
        if not block_resp:
            logging.warning(f"Block {block_number} get_block failed.")
            return block_number, None  # API 호출 실패만 None 처리
        block_ts = block_resp.get("timestamp", "0x0")
        timestamp_str = hex_ts_to_kst_str(block_ts)
        transactions = block_resp.get("transactions", [])
        receipts, success = await get_receipt(session, alchemy_key, block_number)
        if not success:
            logging.warning(f"Block {block_number} get_receipt failed.")
            return block_number, None
        # 1. receipt 목록을 해시 기준으로 dict화
        receipt_map = {r["transactionHash"]: r for r in receipts}
        # 2. 블록의 각 트랜잭션과 receipt를 매칭해서 병합
        results = []
        for tx in transactions:
            tx_hash = tx.get("hash")
            receipt = receipt_map.get(tx_hash)
            row = {
                "transactionHash": tx_hash,
                "transactionIndex": hex_to_str(tx.get("transactionIndex")),
                "blockHash": tx.get("blockHash"),
                "blockNumber": hex_to_str(tx.get("blockNumber")),
                "from": tx.get("from"),
                "to": tx.get("to"),
                "value": hex_to_str(tx.get("value")),
                "input": tx.get("input"),
                "nonce": hex_to_str(tx.get("nonce")),
                "gas": hex_to_str(tx.get("gas")),
                "gasPrice": hex_to_str(tx.get("gasPrice")),
                "maxFeePerGas": hex_to_str(tx.get("maxFeePerGas")),
                "maxPriorityFeePerGas": hex_to_str(tx.get("maxPriorityFeePerGas")),
                "type": tx.get("type"),
                "timestamp": timestamp_str,
                "accessList": tx.get("accessList") or [],
                "gasUsed": hex_to_str(receipt.get("gasUsed")),
                "cumulativeGasUsed": hex_to_str(receipt.get("cumulativeGasUsed")),
                "effectGasPrice": hex_to_str(receipt.get("effectiveGasPrice")),
                "contractAddress": receipt.get("contractAddress"),
                "status": hex_to_str(receipt.get("status")),
                "logsBloom": receipt.get("logsBloom"),
                "logs": convert_logs(receipt.get("logs", [])),
            }
            results.append(row)
        return block_number, results  # 비어 있어도 API 성공이면 무조건 반환
    except Exception as e:
        logging.error(f"Exception while fetching block {block_number}: {e}")
        return block_number,None



# ─────────────────────────────────────────────────────────────
# 5. 블록 범위 비동기 수집
# ─────────────────────────────────────────────────────────────

async def collect_range(start_or_list, end_blk=None) -> Tuple[List[Dict], List[int]]:
    if isinstance(start_or_list, list):
        block_list = start_or_list
    else:
        block_list = list(range(start_or_list, end_blk + 1))

    sem = asyncio.Semaphore(len(ALCHEMY_API_KEYS)*3)#  # Alchemy 키당 3개 동시 실행
    results, failed_blocks = [], []

    async with aiohttp.ClientSession() as session:
        async def _wrapper(block_number: int, idx: int):
            async with sem:
                nodereal_key=next(nodereal_key_iter)
                alchemy_key = next(alchemy_key_iter)
                return await fetch_block_and_receipts(session, block_number, nodereal_key, alchemy_key)
        tasks = [_wrapper(b, i) for i, b in enumerate(block_list)]

        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"{block_list[0]}-{block_list[-1]}"):
            try: 
                block_number, txs = await coro
                if txs is None:
                    failed_blocks.append(block_number)
                else:
                    results.append((block_number, txs))
            except Exception as e:
                logging.warning(f"Unexpected error at block {block_number}: {e}")
                failed_blocks.append(block_number)


    results.sort(key=lambda x: x[0])
    merged = [tx for _, txs in results for tx in txs]
    return merged, failed_blocks



# ─────────────────────────────────────────────────────────────
# 6. Parquet 저장 + S3 업로드 (sync, thread offload)
# ─────────────────────────────────────────────────────────────

def upload_to_s3(file_path: str, key_name: str):
    s3 = boto3.client("s3", aws_access_key_id=S3_CONFIG["access_key"],aws_secret_access_key=S3_CONFIG["secret_key"])
    s3.upload_file(file_path, S3_CONFIG["bucket_name"], key_name)

def parquet_and_upload(start_blk: int, end_blk: int, tx_data: List[Dict]):
    file_prefix = f"{pad(start_blk)}_{pad(end_blk)}"

    if tx_data:
        df = pd.DataFrame(tx_data).fillna("")
        # Arrow 스키마 정의

        # 누락된 컬럼을 None으로 채움
        for field in schema:
            if field.name not in df.columns:
                df[field.name] = None

        # Arrow 테이블 생성
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # 저장 및 업로드
        pq_path = os.path.join(DATA_DIR, f"{file_prefix}.parquet")
        pq.write_table(table, pq_path, compression="snappy")
        
        upload_to_s3(pq_path, os.path.join(S3_CONFIG["target_prefix"], os.path.basename(pq_path)))
        os.remove(pq_path)
        logging.info(f"Uploaded {file_prefix}.parquet ({len(tx_data)} tx)")
    
    else:
        # 빈 파일 처리
        empty_path = os.path.join(DATA_DIR, f"{file_prefix}.empty")
        with open(empty_path, "w") as f:
            f.write("")
        upload_to_s3(empty_path, os.path.join(S3_CONFIG["target_prefix"], os.path.basename(empty_path)))
        os.remove(empty_path)
        logging.info(f"Uploaded empty file {file_prefix}.empty")


# ─────────────────────────────────────────────────────────────
# 7. 메인 루프
# ─────────────────────────────────────────────────────────────

INITIAL_START_BLOCK = 1           # 2025년 1월 1일 0시부터 시작 21523206
INITIAL_END_BLOCK   = 22603459 #  # 2025년 5월 31일 23시 59분 59초 이전블록 번호
SLEEP_BETWEEN_CYCLES= 5

async def main():
    start_blk = load_state(INITIAL_START_BLOCK)
    while start_blk <= INITIAL_END_BLOCK:
        remain = start_blk % CHUNK_SIZE #수집하는 블록 단위 조절할때 보기 편하게 정리하는 용도?
        if remain == 1:
            end_blk = min(start_blk + CHUNK_SIZE - 1, INITIAL_END_BLOCK)
        else:
            end_blk = start_blk + CHUNK_SIZE - remain
        logging.info(f"▶ Collecting blocks {start_blk} ~ {end_blk}")
        range_start_time = time.time()

        # 1차 수집
        tx_data, missing_blocks = await collect_range(start_blk, end_blk)

        # missing_blocks가 남아있다면 반복 수집
        while missing_blocks:
            logging.warning(f"🔁 Retrying missing blocks: {len(missing_blocks)}개 시도")
            retry_tx, new_missing = await collect_range(missing_blocks)
            tx_data.extend(retry_tx)
            # 남아있는 실패 블록 필터링
            succeeded_blocks=set(missing_blocks) - set(new_missing)
            missing_blocks = new_missing
            await asyncio.sleep(1)  # 살짝 딜레이 주는 게 Nodit에 안정적

        # 시간 측정 종료
        range_end_time = time.time()
        total_time = range_end_time - range_start_time
        logging.info(f"Finished collecting {len(tx_data)} txs from blocks {start_blk} ~ {end_blk} in {total_time:.2f} seconds → {len(tx_data)/total_time:.2f} txs/sec")

        # 모두 성공한 경우에만 업로드
        await asyncio.to_thread(parquet_and_upload, start_blk, end_blk, tx_data)

        # 다음 블록으로 진행
        next_start = end_blk + 1
        save_state(next_start)
        start_blk = next_start
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Progress saved.")
