import os
import requests
import json
import gzip
import boto3
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import datetime
import glob

# --- 1. 설정 ---
# Alchemy API 설정
ALCHEMY_CONFIG = {
    "url": "https://eth-mainnet.g.alchemy.com/v2/KudUpSM8lqKQ7mduKdJza8_I3BFgG3JV",
    "timeout": 20
}

# S3 버킷 설정
S3_CONFIG = {
    "bucket_name": "de6-team6-bucket",
    "target_prefix": "eth/historical/"
}

# 처리 및 실행 설정
PROCESSING_CONFIG = {
    "state_file": "eth_processing_state.json", # 상태 파일 이름 
    "local_raw_dir": "./raw_block_data",         # 추출된 개별 블록 파일 저장소
    "local_compressed_dir": "./compressed_data", # 압축 파일 임시 저장소
    "compression_chunk_size": 20000,             # 몇 개의 블록을 하나의 파일로 압축할지 결정
    "fetch_batch_size": 1000,                    # 한 번에 병렬로 가져올 블록 수
    "num_workers": min(cpu_count(), 12)
}

KST = datetime.timezone(datetime.timedelta(hours=9))

# --- 2. 상태 관리 ---
def save_state(next_fetch_block):
    """다음 '추출'할 블록 번호를 파일에 저장합니다."""
    state = {"next_fetch_block": next_fetch_block}
    with open(PROCESSING_CONFIG["state_file"], 'w') as f:
        json.dump(state, f)

def load_or_initialize_state():
    """상태 파일을 로드하거나, 최신 블록부터 시작하도록 초기화합니다."""
    state_file = PROCESSING_CONFIG["state_file"]
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                start_block = state.get("next_fetch_block")
                if start_block is not None:
                    print(f"[상태 로드] 다음 추출할 블록: {start_block}")
                    return start_block
        except (json.JSONDecodeError, KeyError):
            print(f"[경고] '{state_file}' 파일이 손상되어 새로 시작합니다.")
            pass
    
    print("[상태 초기화] 최신 블록 번호를 가져와서 시작합니다.")
    latest_block = get_latest_block_number()
    if latest_block is not None:
        save_state(latest_block)
        print(f"[상태 저장] 다음 추출할 블록: {latest_block}")
    return latest_block

def get_latest_block_number():
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"}
    try:
        response = requests.post(ALCHEMY_CONFIG["url"], json=payload, timeout=ALCHEMY_CONFIG["timeout"])
        response.raise_for_status()
        return int(response.json()['result'], 16)
    except requests.RequestException as e:
        print(f"[에러] 최신 블록 번호 조회 실패: {e}")
        return None

# --- 3. STAGE 1: 데이터 추출 및 로컬 저장 로직 ---
def convert_hex_timestamp_to_kst_str(hex_timestamp):
    if not hex_timestamp: return None
    try:
        utc_dt = datetime.datetime.fromtimestamp(int(hex_timestamp, 16), tz=datetime.timezone.utc)
        return utc_dt.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError): return None

def transform_transaction(tx, block_timestamp):
    if not isinstance(tx, dict): return None
    numeric_fields = ['blockNumber', 'gas', 'gasPrice', 'maxFeePerGas',
                      'maxPriorityFeePerGas', 'nonce', 'transactionIndex', 'value']
    transformed_tx = tx.copy()
    for field in numeric_fields:
        if field in transformed_tx:
            transformed_tx[field] = str(int(transformed_tx[field], 16)) if transformed_tx.get(field) else None
    transformed_tx['timestamp'] = convert_hex_timestamp_to_kst_str(block_timestamp)
    return transformed_tx

def fetch_and_save_single_block(block_num):
    """단일 블록 데이터를 가져와 개별 파일로 저장합니다. 성공 시 block_num 반환."""
    raw_dir = PROCESSING_CONFIG["local_raw_dir"]
    file_path = os.path.join(raw_dir, f"{block_num}.jsonl")

    if os.path.exists(file_path):
        return block_num

    hex_block_num = hex(block_num)
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": [hex_block_num, True]}
    
    try:
        response = requests.post(ALCHEMY_CONFIG["url"], json=payload, timeout=ALCHEMY_CONFIG["timeout"])
        response.raise_for_status()
        result = response.json().get('result')

        if result and result.get('transactions'):
            block_timestamp = result.get('timestamp')
            with open(file_path, 'w', encoding='utf-8') as f:
                for tx in result['transactions']:
                    transformed = transform_transaction(tx, block_timestamp)
                    if transformed:
                        f.write(json.dumps(transformed, ensure_ascii=False) + '\n')
        else:
            open(file_path, 'w').close()
        return block_num
    except requests.RequestException:
        return None

def fetcher(start_block, num_blocks_to_fetch):
    """지정된 수만큼 블록을 병렬로 가져와 로컬에 저장하고, 다음 시작 지점을 반환합니다."""
    print(f"\n--- 데이터 추출 시작 (총 {num_blocks_to_fetch}개) ---")
    
    end_block = max(0, start_block - num_blocks_to_fetch + 1)
    tasks = range(start_block, end_block - 1, -1)
    
    successful_blocks = []
    with Pool(processes=PROCESSING_CONFIG["num_workers"]) as pool:
        results = tqdm(pool.imap_unordered(fetch_and_save_single_block, tasks), total=len(tasks), desc="[데이터 추출]")
        for res in results:
            if res is not None:
                successful_blocks.append(res)
    
    if not successful_blocks:
        print("[경고] 이번 배치에서 성공적으로 가져온 블록이 없습니다. 같은 위치에서 재시도합니다.")
        return start_block

    # 다음 시작 지점
    next_start = min(successful_blocks) - 1
    return next_start


# --- 4. STAGE 2: 압축, 업로드, 정리 로직 ---
def compress_and_upload():
    """로컬의 raw 파일들을 확인하여, 청크가 완성되면 압축/업로드/삭제를 수행합니다."""
    print("\n--- 압축/업로드 작업 확인 ---")
    
    raw_dir = PROCESSING_CONFIG["local_raw_dir"]
    chunk_size = PROCESSING_CONFIG["compression_chunk_size"]
    
    # 1. 로컬 파일 스캔 및 청크 단위로 그룹화
    try:
        raw_files = glob.glob(os.path.join(raw_dir, "*.jsonl"))
    except FileNotFoundError:
        print("Raw 데이터 폴더가 없습니다. 추출을 먼저 진행합니다.")
        return

    chunks = {}
    for f_path in raw_files:
        try:
            block_num = int(os.path.basename(f_path).split('.')[0])
            chunk_id = block_num // chunk_size
            if chunk_id not in chunks:
                chunks[chunk_id] = []
            chunks[chunk_id].append(f_path)
        except (ValueError, IndexError):
            continue

    # 2. 완성된 청크 처리
    completed_chunks = 0
    for chunk_id, files in chunks.items():
        if len(files) == chunk_size:
            completed_chunks += 1
            start_block = chunk_id * chunk_size + chunk_size - 1
            end_block = chunk_id * chunk_size
            
            print(f"\n[처리 시작] 완성된 청크 발견: {start_block} ~ {end_block}")
            
            # 3. 압축
            compressed_file_path = os.path.join(PROCESSING_CONFIG["local_compressed_dir"], f"{start_block}_{end_block}.jsonl.gz")
            os.makedirs(PROCESSING_CONFIG["local_compressed_dir"], exist_ok=True)
            
            try:
                with gzip.open(compressed_file_path, 'wt', encoding='utf-8') as f_gz:
                    for raw_file in sorted(files, reverse=True): # 블록번호 순으로 정렬
                        with open(raw_file, 'r', encoding='utf-8') as f_in:
                            f_gz.write(f_in.read())
                print(f"[압축 성공] -> {compressed_file_path}")
            except Exception as e:
                print(f"[에러] 압축 실패: {e}. 다음 사이클에 재시도합니다.")
                continue

            # 4. S3 업로드
            s3_key = os.path.join(S3_CONFIG["target_prefix"], os.path.basename(compressed_file_path))
            try:
                s3_client = boto3.client('s3')
                s3_client.upload_file(compressed_file_path, S3_CONFIG['bucket_name'], s3_key)
                print(f"[업로드 성공] -> s3://{S3_CONFIG['bucket_name']}/{s3_key}")
            except Exception as e:
                print(f"[에러] S3 업로드 실패: {e}. 다음 사이클에 재시도합니다.")
                os.remove(compressed_file_path) # 실패 시 임시 압축 파일 삭제
                continue

            # 5. 로컬 원본 파일 및 임시 압축 파일 삭제
            try:
                os.remove(compressed_file_path)
                for f in files:
                    os.remove(f)
                print("[정리 완료] 로컬 raw 파일 및 압축 파일 삭제")
            except Exception as e:
                print(f"[경고] 정리 작업 실패: {e}")

    if completed_chunks == 0:
        print("처리할 완성된 청크가 없습니다.")

# --- 5. 메인 실행부 ---
def main():
    """메인 실행 함수: 추출기와 패커를 번갈아 실행"""
    print("--- Ethereum Low-Memory/High-Resilience Processor ---")
    os.makedirs(PROCESSING_CONFIG["local_raw_dir"], exist_ok=True)

    try:
        while True:
            next_block_to_fetch = load_or_initialize_state()
            if next_block_to_fetch is None or next_block_to_fetch <= 0:
                print("모든 블록 처리가 완료된 것 같습니다. 종료합니다.")
                break
            next_start_point = fetcher(next_block_to_fetch, PROCESSING_CONFIG["fetch_batch_size"])
            save_state(next_start_point) # 매 배치마다 진행상황 저장
            compress_and_upload()
            
            print("\n--- 사이클 완료, 5초 후 다음 작업 시작 ---")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n[사용자 중단] 프로그램을 종료합니다. 현재까지의 진행 상황은 안전하게 저장되었습니다.")
    except Exception as e:
        print(f"\n\n[치명적 오류] 예상치 못한 오류로 프로그램을 종료합니다: {e}")

if __name__ == "__main__":
    main()