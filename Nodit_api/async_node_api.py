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

파라미터는 코드 상단 상수로 조정하세요.
"""

import os, json, time, asyncio, datetime, logging, math
from typing import List, Dict, Sequence, Tuple

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
CHUNK_SIZE   = 5_000                 # 블록 배치 크기
FIRST_RPP    = 1000                  # Nodit 최대 1000
FETCH_TIMEOUT= 20
MAX_RETRIES  = 3
KST          = datetime.timezone(datetime.timedelta(hours=9))

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(BASE_DIR, "../compressed_data")
STATE_FILE   = os.path.join(BASE_DIR, "eth_processing_state.json")
FAILED_LOG   = os.path.join(BASE_DIR, "failed_blocks.log")

URL = "https://web3.nodit.io/v1/ethereum/mainnet/blockchain/getTransactionsInBlock"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

# ─────────────────────────────────────────────────────────────
# 1. 환경 변수
# ─────────────────────────────────────────────────────────────
load_dotenv()
NODIT_API_KEYS: Sequence[str] = [k.strip() for k in os.getenv("NODIT_API_KEYS", "").split(",") if k.strip()]
if not NODIT_API_KEYS:
    raise ValueError("NODEIT_API_KEYS not set in .env")
KEY_COUNT = len(NODIT_API_KEYS)
LIMITERS  = {k: AsyncLimiter(2, 1) for k in NODIT_API_KEYS}  # 2 req / sec per key

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

def utc_to_kst(ts: int|str) -> str:
    try:
        ts_int = int(ts)
    except Exception:
        return ""
    utc_dt = datetime.datetime.fromtimestamp(ts_int, tz=datetime.timezone.utc)
    return utc_dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

def transform_tx(tx: Dict, blk_ts: int|str):
    new_tx = tx.copy()
    new_tx["timestamp"] = utc_to_kst(blk_ts)
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

async def _post(session: aiohttp.ClientSession, key: str, payload: Dict) -> Dict:
    for attempt in range(1, MAX_RETRIES+1):
        try:
            async with LIMITERS[key]:
                hdr = {"X-API-KEY": key,
                       "accept": "application/json",
                       "content-type": "application/json"}
                async with session.post(URL, headers=hdr, json=payload, timeout=FETCH_TIMEOUT) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(1*attempt)
    # never here

async def fetch_block(block_num: int, key_offset: int=0) -> List[Dict]:
    """모든 페이지를 병렬로 수집해 트랜잭션 리스트 반환"""
    txs: List[Dict] = []
    key_idx = key_offset % KEY_COUNT
    key     = NODIT_API_KEYS[key_idx]

    async with aiohttp.ClientSession() as session:
        # 첫 페이지로 count 파악
        first_payload = {
            "block": str(block_num),
            "rpp"  : FIRST_RPP,
            "withCount": True,
            "withLogs" : True,
            "withDecode": True,
        }
        first = await _post(session, key, first_payload)
        block_ts = first.get("items", [{}])[0].get("timestamp", 0) if first.get("items") else 0
        txs.extend(transform_tx(tx, block_ts) for tx in first.get("items", []))
        total_cnt = first.get("count", len(txs))

        # 남은 페이지 수 계산
        pages_left = math.ceil(total_cnt / FIRST_RPP) - 1
        cursor = first.get("cursor")
        tasks = []
        while pages_left and cursor:
            key_idx += 1
            key = NODIT_API_KEYS[key_idx % KEY_COUNT]
            payload = {"block": str(block_num),
                       "rpp": FIRST_RPP,
                       "cursor": cursor,
                       "withDecode": True}
            tasks.append(_post(session, key, payload))
            pages_left -= 1
            cursor = None  # cursor chaining은 응답에서 다시 얻음

        # 비동기 페이지 응답 수집 (중첩 cursor 대응)
        while tasks:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            tasks = list(_)
            for d in done:
                try:
                    page = d.result()
                except Exception as e:
                    logging.warning(f"Block {block_num} page error: {e}")
                    continue
                txs.extend(transform_tx(tx, block_ts) for tx in page.get("items", []))
                next_cursor = page.get("cursor")
                if next_cursor:
                    key_idx += 1
                    key = NODIT_API_KEYS[key_idx % KEY_COUNT]
                    payload = {"block": str(block_num), "rpp": FIRST_RPP, "cursor": next_cursor, "withDecode": True}
                    tasks.append(_post(session, key, payload))

    return txs

# ─────────────────────────────────────────────────────────────
# 5. 블록 범위 비동기 수집
# ─────────────────────────────────────────────────────────────

async def collect_range(start_blk: int, end_blk: int) -> List[Dict]:
    """start_blk~end_blk inclusive 트랜잭션 전부 수집"""
    sem = asyncio.Semaphore(KEY_COUNT*4)  # 동시 블록 다운로드 제한

    async def _wrapper(b, idx):
        async with sem:
            return b, await fetch_block(b, idx)

    tasks = [_wrapper(b, i) for i, b in enumerate(range(start_blk, end_blk+1))]
    results: List[Tuple[int,List[Dict]]] = []
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Range {start_blk}-{end_blk}"):
        blk, tx_list = await coro
        results.append((blk, tx_list))
    # 정렬 보장 & 병합
    results.sort(key=lambda x: x[0])
    merged: List[Dict] = []
    for _, txl in results:
        merged.extend(txl)
    return merged

# ─────────────────────────────────────────────────────────────
# 6. Parquet 저장 + S3 업로드 (sync, thread offload)
# ─────────────────────────────────────────────────────────────

def upload_to_s3(file_path: str, key_name: str):
    s3 = boto3.client("s3", aws_access_key_id=S3_CONFIG["access_key"],aws_secret_access_key=S3_CONFIG["secret_key"])
    s3.upload_file(file_path, S3_CONFIG["bucket_name"], key_name)

def parquet_and_upload(start_blk:int, end_blk:int, tx_data:List[Dict]):
    file_prefix = f"{pad(start_blk)}_{pad(end_blk)}"
    if tx_data:
        df = pd.DataFrame(tx_data).fillna("")
        table = pa.Table.from_pandas(df)
        pq_path = os.path.join(DATA_DIR, f"{file_prefix}.parquet")
        pq.write_table(table, pq_path, compression="snappy")
        upload_to_s3(pq_path, os.path.join(S3_CONFIG["target_prefix"], os.path.basename(pq_path)))
        os.remove(pq_path)
        logging.info(f"Uploaded {file_prefix}.parquet ({len(tx_data)} tx)")
    else:
        # 빈 블록 범위 → .empty
        empty_path = os.path.join(DATA_DIR, f"{file_prefix}.empty")
        with open(empty_path, "w") as f:
            f.write("")
        upload_to_s3(empty_path, os.path.join(S3_CONFIG["target_prefix"], os.path.basename(empty_path)))
        os.remove(empty_path)
        logging.info(f"Uploaded empty file {file_prefix}.empty")

# ─────────────────────────────────────────────────────────────
# 7. 메인 루프
# ─────────────────────────────────────────────────────────────

INITIAL_START_BLOCK = 1
INITIAL_END_BLOCK   = 5_700_000
SLEEP_BETWEEN_CYCLES= 5

async def main():
    start_blk = load_state(INITIAL_START_BLOCK)
    while start_blk <= INITIAL_END_BLOCK:
        end_blk = min(start_blk + CHUNK_SIZE - 1, INITIAL_END_BLOCK)
        logging.info(f"Collecting blocks {start_blk} ~ {end_blk}")
        try:
            tx_data = await collect_range(start_blk, end_blk)
            await asyncio.to_thread(parquet_and_upload, start_blk, end_blk, tx_data)
        except Exception as e:
            logging.error(f"Error during range {start_blk}-{end_blk}: {e}")
            with open(FAILED_LOG, "a") as f:
                f.write(f"{start_blk}-{end_blk}: {e}\n")
            break
        next_start = end_blk + 1
        save_state(next_start)
        start_blk = next_start
        logging.info("Sleeping before next cycle…")
        await asyncio.sleep(SLEEP_BETWEEN_CYCLES)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Progress saved.")
