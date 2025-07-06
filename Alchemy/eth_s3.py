import os
import requests
import json
import gzip
import boto3
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import datetime
from dotenv import load_dotenv

# --- 1. 설정 ---
# Alchemy API 설정
load_dotenv()
ALCHEMY_API_URL = os.getenv("ALCHEMY_API_URL")

if not ALCHEMY_API_URL:
    print("[오류] .env파일에 ALCHEMY_API_URL이 설정되지 않았습니다!")
    print("종료합니다")
    exit()

# S3 버킷 설정
S3_CONFIG = {
    "bucket_name": "de6-team6-bucket",
    "target_prefix": "eth/historical/"
}

# 처리 및 실행 설정
PROCESSING_CONFIG = {
    "MY_START_BLOCK": 22758002,                  # 작업 시작 블록
    "MY_END_BLOCK":   18254393,                  # 작업 종료 블록 
    "state_file": "eth_processing_state.json",   # 상태 파일 이름 
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
    my_start = PROCESSING_CONFIG["MY_START_BLOCK"]
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
    print(f"[상태 초기화] 담당 시작 블록인 {my_start}부터 시작합니다.")
    save_state(my_start)
    return my_start

# --- 3. 데이터 추출 및 로컬 저장 로직 ---
def convert_hex_timestamp_to_kst_str(hex_timestamp):
    if not hex_timestamp: return None
    try:
        utc_dt = datetime.datetime.fromtimestamp(int(hex_timestamp, 16), tz=datetime.timezone.utc)
        return utc_dt.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return None

def transform_transaction(tx, block_timestamp):
    if not isinstance(tx, dict): return None
    numeric_fields = ['blockNumber', 'gas', 'gasPrice', 'maxFeePerGas',
                      'maxPriorityFeePerGas', 'nonce', 'transactionIndex', 'value']
    transformed_tx = tx.copy()
    for field in numeric_fields:
        if field in transformed_tx:
            if transformed_tx.get(field):
                transformed_tx[field] = str(int(transformed_tx[field], 16))

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
        response = requests.post(ALCHEMY_API_URL, json=payload, timeout=20)
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

def fetch_multiple_blocks(start_block, num_blocks_to_fetch):
    """지정된 수만큼 블록을 병렬로 가져와 로컬에 저장하고, 다음 시작 지점을 반환합니다."""
    my_end_block = PROCESSING_CONFIG["MY_END_BLOCK"]
    print(f"\n--- 데이터 추출 시작 (총 {num_blocks_to_fetch}개) ---")
    
    end_block = max(my_end_block, start_block - num_blocks_to_fetch + 1)
    tasks = range(start_block, end_block - 1, -1)
    if not tasks:
        print("추출할 블록이 더 이상 없습니다.")
        return start_block
    
    successful_blocks = []
    with Pool(processes=PROCESSING_CONFIG["num_workers"]) as pool:
        results = tqdm(pool.imap_unordered(fetch_and_save_single_block, tasks), total=len(tasks), desc="[데이터 추출]")
        for res in results:
            if res is not None:
                successful_blocks.append(res)
    
    if not successful_blocks:
        print("[경고] 이번 배치에서 성공적으로 가져온 블록이 없습니다. 같은 위치에서 재시도합니다.")
        return start_block

    return min(successful_blocks) - 1



# --- 4. 압축, 업로드, 정리 로직 ---
def check_chunk_is_complete(start, end, raw_dir):
    """주어진 범위(start ~ end)의 모든 파일이 로컬에 있는지 확인"""
    for block_num in range(start, end - 1, -1):
        if not os.path.exists(os.path.join(raw_dir, f"{block_num}.jsonl")):
            return False
    return True

def process_chunk(start_block, end_block, raw_dir):
    """완성된 청크를 압축, 업로드, 삭제하는 함수"""
    print(f"\n[처리 시작] 연속된 청크 발견: {start_block} ~ {end_block}")
    
    compressed_filename = f"{end_block}_{start_block}.jsonl.gz"
    compressed_file_path = os.path.join(PROCESSING_CONFIG["local_compressed_dir"], compressed_filename)
    os.makedirs(PROCESSING_CONFIG["local_compressed_dir"], exist_ok=True)
    
    files_to_process = [os.path.join(raw_dir, f"{i}.jsonl") for i in range(start_block, end_block - 1, -1)]
    
    try:
        with gzip.open(compressed_file_path, 'wt', encoding='utf-8') as f_gz:
            for raw_file in files_to_process:
                with open(raw_file, 'r', encoding='utf-8') as f_in: f_gz.write(f_in.read())
        print(f"[압축 성공] -> {compressed_file_path}")
    except Exception as e:
        print(f"[에러] 압축 실패: {e}"); return
    
    s3_key = os.path.join(S3_CONFIG["target_prefix"], compressed_filename)
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(compressed_file_path, S3_CONFIG['bucket_name'], s3_key)
        print(f"[업로드 성공] -> s3://{S3_CONFIG['bucket_name']}/{s3_key}")
    except Exception as e:
        print(f"[에러] S3 업로드 실패: {e}"); os.remove(compressed_file_path); return
    
    try:
        os.remove(compressed_file_path)
        for f in files_to_process:
            if os.path.exists(f): os.remove(f)
        print(f"[정리 완료] 처리된 raw 파일 {len(files_to_process)}개 및 압축 파일 삭제")
    except Exception as e:
        print(f"[경고] 정리 작업 실패: {e}")

def run_packer_and_uploader():
    """절대 그리드를 기준으로 자신의 담당 범위 내에서 완성된 청크를 찾아 처리합니다."""
    print("\n--- 압축/업로드 작업 확인 ---")
    
    my_start_block = PROCESSING_CONFIG["MY_START_BLOCK"]
    my_end_block = PROCESSING_CONFIG["MY_END_BLOCK"]
    chunk_size = PROCESSING_CONFIG["compression_chunk_size"]
    raw_dir = PROCESSING_CONFIG["local_raw_dir"]

    current_block = my_start_block
    
    while current_block >= my_end_block:
        chunk_id = current_block // chunk_size
        ideal_chunk_start = (chunk_id * chunk_size) + chunk_size - 1
        ideal_chunk_end = chunk_id * chunk_size
        
        actual_chunk_start = min(current_block, ideal_chunk_start)
        actual_chunk_end = max(my_end_block, ideal_chunk_end)

        if check_chunk_is_complete(actual_chunk_start, actual_chunk_end, raw_dir):
            process_chunk(actual_chunk_start, actual_chunk_end, raw_dir)
        # 미완성일 경우 아무것도 출력하지 않아 로그를 깨끗하게 유지할 수 있습니다.
        
        current_block = actual_chunk_end - 1



# --- 5. 메인 실행부 ---
def main():
    print("--- Ethereum Data Processor ---")
    my_start_block = PROCESSING_CONFIG["MY_START_BLOCK"]
    my_end_block = PROCESSING_CONFIG["MY_END_BLOCK"]
    print(f"담당 작업 범위: {my_start_block} ~ {my_end_block}")
    
    os.makedirs(PROCESSING_CONFIG["local_raw_dir"], exist_ok=True)
    try:
        while True:
            next_block_to_fetch = load_or_initialize_state()
            
            if next_block_to_fetch < my_end_block:
                print("\n담당 범위의 모든 블록 추출을 완료했습니다. 마지막 압축/업로드만 확인합니다.")
                run_packer_and_uploader()
                print("모든 작업 완료. 프로그램을 종료합니다.")
                break
            
            next_start_point = fetch_multiple_blocks(next_block_to_fetch, PROCESSING_CONFIG["fetch_batch_size"])
            save_state(next_start_point)
            run_packer_and_uploader()
            
            print("\n--- 사이클 완료, 5초 후 다음 작업 시작 ---")
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n\n[사용자 중단] 프로그램을 종료합니다. 현재까지의 진행 상황은 안전하게 저장되었습니다.")
    except Exception as e:
        print(f"\n\n[치명적 오류] 예상치 못한 오류로 프로그램을 종료합니다: {e}")

if __name__ == "__main__":
    main()