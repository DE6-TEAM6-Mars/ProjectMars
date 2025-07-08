import os
import json
import gzip
import glob
import time
import datetime
from multiprocessing import Pool, cpu_count
from typing import List, Sequence

import boto3
import requests
from tqdm import tqdm
from dotenv import load_dotenv


# -------------------------- 0. 기본 상수 --------------------------
BLOCK_DIGITS= 8 #그냥 자릿수 8로 고정
CHUNK_SIZE = 20_000 
FETCH_TIMEOUT = 20
SLEEP_BETWEEN_CYCLES = 5
MAX_RETRIES_PER_BLOCK = 3          # 각 블록 요청 재시도 횟수
MAX_RECOVERY_ROUNDS = 5            # 누락 블록 재수집 라운드 수
INITIAL_START_BLOCK = 1  #초기 시작 블록 번호 설정
INITIAL_END_BLOCK = 4_563_598 #초기 종료 블록 번호 설정
KST = datetime.timezone(datetime.timedelta(hours=9))

# Alchemy api+ nodeReal api 키 
load_dotenv()
ALCHEMY_KEYS: Sequence[str] =[k.strip() for k in os.getenv("ALCHEMY_NODEREAL_API_KEYS", "").split(",") if k.strip()] 
KEY_COUNT = len(ALCHEMY_KEYS)
MAX_WORKERS = KEY_COUNT*4
S3_CONFIG = {
    "bucket_name": "de6-team6-bucket",
    "target_prefix": "eth/historical/",
    "access_key" : os.getenv("S3_ACCESS_KEY"),
    "secret_key" : os.getenv("S3_SECRET_KEY")
}
# -------------------------- 1. 경로 & 설정 ------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_RAW = os.path.join(BASE_DIR, "../raw_block_data")
DIR_COMPRESSED = os.path.join(BASE_DIR, "../compressed_data")
STATE_FILE = os.path.join(BASE_DIR, "eth_processing_state.json")
FAILED_LOG = os.path.join(BASE_DIR, "failed_blocks.log")
os.makedirs(DIR_RAW, exist_ok=True)

# -------------------------- 2. 진행 상태 관리 --------------------------
def save_state(next_block: int) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"next_fetch_block": next_block}, f)


def load_or_init_state(initial_block: int) -> int:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return int(json.load(f).get("next_fetch_block"))
        except Exception:
            print("[경고] 상태 파일 손상 → 초기화")
    save_state(initial_block)
    return initial_block

# -------------------------- 3. 유틸 함수 --------------------------
def pad_for_filename(num: int) -> str:
    return f"{num:0{BLOCK_DIGITS}d}"          # 0으로 왼쪽 채움

def hex_ts_to_kst_str(hex_ts: str) -> str:
    if not hex_ts:
        return ""
    dt = datetime.datetime.fromtimestamp(int(hex_ts, 16), tz=datetime.timezone.utc)
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def transform_tx(tx: dict, block_ts_hex: str) -> dict:
    numeric_fields = [
        "blockNumber", "gas", "gasPrice", "maxFeePerGas", "maxPriorityFeePerGas",
        "nonce", "transactionIndex", "value",
    ]
    tx = tx.copy()
    for f in numeric_fields:
        if f in tx and tx[f]:
            tx[f] = str(int(tx[f], 16))
    tx["timestamp"] = hex_ts_to_kst_str(block_ts_hex)
    return tx

# -------------------------- 4. 블록 다운로드 -----------------------
def _write_empty(file_path: str):
    # 블록은 존재하나 트랜잭션 없을 때 표식으로 빈 파일 생성
    open(file_path, "w", encoding="utf-8").close()

def _alchemy_url_for(block_num: int, attempt: int = 0) -> str:
    """블록 번호+재시도 횟수 조합으로 라운드로빈 URL 선택"""
    idx = (block_num + attempt) % KEY_COUNT
    return ALCHEMY_KEYS[idx]

def download_block(block_num: int) -> int | None:
    """단일 블록 요청&파일 생성 / 실패 시 None 반환"""
    fp = os.path.join(DIR_RAW, f"{block_num}.jsonl")
    if os.path.exists(fp):
        return block_num

    for attempt in range(MAX_RETRIES_PER_BLOCK):
        try:
            url = _alchemy_url_for(block_num, attempt)
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "eth_getBlockByNumber", "params": [hex(block_num), True],
            }
            res=requests.post(url, json=payload, timeout=FETCH_TIMEOUT)
            res.raise_for_status()
            data = res.json().get("result", {})
            with open(fp, "w", encoding="utf-8") as f:
                if data and data.get("transactions"):
                    ts_hex = data.get("timestamp", "0x0")
                    for tx in data["transactions"]:
                        f.write(json.dumps(transform_tx(tx, ts_hex), ensure_ascii=False) + "\n")
                else:
                    ##
                    pass  # 빈 파일이 이미 작성 완료
                    
            return block_num
        except Exception:
            time.sleep(1)
    return None  # 모든 재시도 실패

# -------------------------- 5. 범위 다운로드 + 복구 ----------------

def fetch_range_with_recovery(start_blk: int, end_blk: int) -> None:
    """청크 범위 다운로드 후 누락 블록 복구"""
    # 1차 병렬 다운로드
    _parallel_download(range(start_blk, end_blk + 1))

    # 누락 블록 반복 복구
    for round_i in range(1, MAX_RECOVERY_ROUNDS + 1):
        missing = _missing_blocks(start_blk, end_blk)
        if not missing:
            break
        print(f"[복구] 라운드 {round_i}: 누락 {len(missing)}개 재요청")
        _parallel_download(missing)

    # 최종 누락 검사 후 실패 로그 기록
    final_missing = _missing_blocks(start_blk, end_blk)
    if final_missing:
        with open(FAILED_LOG, "a", encoding="utf-8") as f:
            for blk in final_missing:
                f.write(f"{blk}\n")
        raise RuntimeError(f"[치명] 청크 내 누락 블록 {len(final_missing)}개. 로그 기록 완료")


def _parallel_download(iterable):
    with Pool(processes=MAX_WORKERS) as pool:
        for _ in tqdm(pool.imap_unordered(download_block, iterable), total=len(iterable), desc="[다운로드]"):
            pass


def _missing_blocks(start_blk: int, end_blk: int) -> List[int]:
    """청크 범위에서 파일 없는 블록 목록 반환"""
    missing: List[int] = []
    for blk in range(start_blk, end_blk + 1):
        if not os.path.exists(os.path.join(DIR_RAW, f"{blk}.jsonl")):
            missing.append(blk)
    return missing

# -------------------------- 6. 압축 & S3 ---------------------------

def compress_and_upload(start_blk: int, end_blk: int) -> None:
    files = [os.path.join(DIR_RAW, f"{blk}.jsonl") for blk in range(start_blk, end_blk + 1)]

    # 안전 검사: 누락 없어야 함
    missing = [p for p in files if not os.path.exists(p)]
    if missing:
        raise RuntimeError(f"[오류] 압축 시도 전 누락 파일 {len(missing)}개 발견")

    os.makedirs(DIR_COMPRESSED, exist_ok=True)
    gz_name = f"{pad_for_filename(start_blk)}_{pad_for_filename(end_blk)}.jsonl.gz"
    gz_path = os.path.join(DIR_COMPRESSED, gz_name)

    with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
        for p in files:
            with open(p, "r", encoding="utf-8") as src:
                gz.write(src.read())
    print(f"[압축] {gz_name} 생성 완료")

    s3_key = os.path.join(S3_CONFIG["target_prefix"], gz_name)
    boto3.client("s3",aws_access_key_id=S3_CONFIG["access_key"], aws_secret_access_key=S3_CONFIG["secret_key"]).upload_file(gz_path, S3_CONFIG["bucket_name"], s3_key)
    print(f"[업로드] s3://{S3_CONFIG['bucket_name']}/{s3_key}")

    # 로컬 정리
    os.remove(gz_path)
    for p in files:
        os.remove(p)
    print("[정리] raw + gzip 삭제 완료")

# -------------------------- 7. 메인 루프 --------------------------

def main():
    print("--- Loss‑less Chunk Downloader ---")\
    
    start_block = load_or_init_state(INITIAL_START_BLOCK)
    while start_block <=  INITIAL_END_BLOCK:
        end_block = min(start_block + CHUNK_SIZE - 1, INITIAL_END_BLOCK)
        print(f"\n[사이클] {start_block} → {end_block}")

        # 1) 다운로드 & 복구
        try:
            fetch_range_with_recovery(start_block, end_block)
        except RuntimeError as e:
            print(e)
            print("[중단] 청크 누락이 해결되지 않아 프로세스 중단. 재시작 시 동일 위치부터 재시도합니다.")
            break

        # 2) 압축 & 업로드 (누락 검사 포함)
        compress_and_upload(start_block, end_block)

        # 3) 다음 청크
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
