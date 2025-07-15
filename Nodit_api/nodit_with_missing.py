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
MAX_RETRIES  = 1
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
print(KEY_COUNT)
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
            await asyncio.sleep(1)

async def fetch_block(block_num: int, key_offset: int=0) -> Tuple[int, List[Dict]]:
    txs: List[Dict] = []
    key_idx = key_offset % KEY_COUNT
    key     = NODIT_API_KEYS[key_idx]

    async with aiohttp.ClientSession() as session:
        try:
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
                cursor = None

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
            return block_num, txs

        except Exception as e:
            logging.debug(f"Err {block_num}: {e}")
            return block_num, None                      # 네트워크·타임아웃도 실패

# ─────────────────────────────────────────────────────────────
# 5. 블록 범위 비동기 수집
# ─────────────────────────────────────────────────────────────

async def collect_range(start_or_list, end_blk=None) -> Tuple[List[Dict], List[int]]:
    if isinstance(start_or_list, list):  # missing block list로 호출된 경우
        block_list = start_or_list
    else:
        block_list = list(range(start_or_list, end_blk + 1))

    sem = asyncio.Semaphore(KEY_COUNT * 4)
    results, failed = [], []

    async def _wrapper(b, idx):
        async with sem:
            b , txs=await fetch_block(b,idx)
            return b, txs

    tasks = [_wrapper(b, i) for i, b in enumerate(block_list)]
    for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"{block_list[0]}-{block_list[-1]}"):
        try:
            blk, txs = await coro
            if txs is None:
                failed.append(blk)
            else:
                results.append((blk, txs))
        except Exception as e:
            failed.append(blk)
            logging.warning(f"Unexpected error at block {blk}: {e}")


    results.sort(key=lambda x: x[0])
    merged = [tx for _, txs in results for tx in txs]
    return merged, failed


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

        # "decodedInput"은 JSON 문자열로 직렬화
        if "decodedInput" in df.columns:
            df["decodedInput"] = df["decodedInput"].apply(lambda v: json.dumps(v, ensure_ascii=False))

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

        # 누락된 컬럼을 None으로 채움
        for field in arrow_schema:
            if field.name not in df.columns:
                df[field.name] = None

        # Arrow 테이블 생성
        table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)

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

INITIAL_START_BLOCK = 1
INITIAL_END_BLOCK   = 5_700_000
SLEEP_BETWEEN_CYCLES= 5

async def main():
    start_blk = load_state(INITIAL_START_BLOCK)

    while start_blk <= INITIAL_END_BLOCK:
        end_blk = min(start_blk + CHUNK_SIZE - 1, INITIAL_END_BLOCK)
        logging.info(f"▶ Collecting blocks {start_blk} ~ {end_blk}")

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
