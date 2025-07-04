import requests
import json
import os
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

ALCHEMY_URL = "https://eth-mainnet.g.alchemy.com/v2/KudUpSM8lqKQ7mduKdJza8_I3BFgG3JV"
LOCAL_DATA_DIR = "./ethereum_historical_data"
BLOCK_NUM = 500000  # 한 번 실행할 때마다 가져올 블록 수

NUM_WORKERS = min(cpu_count(), 8)
STATE_FILE = "state_historical_extractor.json" # 상태 파일 이름 변경

def get_latest_block_number():
    """최신 블록 번호를 가져옵니다."""
    print("가장 최근 블록 번호를 가져옵니다...")
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"}
    try:
        response = requests.post(ALCHEMY_URL, json=payload, timeout=10)
        response.raise_for_status()
        latest_block_num = int(response.json()['result'], 16)
        print(f"최근 블록 번호는 {latest_block_num}입니다.")
        return latest_block_num
    except requests.exceptions.RequestException as e:
        print(f"가장 최근 블록 번호를 가져오는데 에러 발생: {e}")
        return None

def save_state(next_start_block):
    """다음 실행 시 시작할 블록 번호를 파일에 저장합니다."""
    with open(STATE_FILE, 'w') as f:
        json.dump({"next_start_block": next_start_block}, f)
    print(f"\n다음 실행 시 시작할 블록 번호는 {next_start_block}입니다.")

def load_or_initialize_state():
    """
    상태 파일을 로드하거나, 파일이 없으면 초기화합니다.
    다음 작업의 시작 블록 번호를 반환합니다.
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                return json.load(f).get("next_start_block")
            except json.JSONDecodeError:
                pass
    print("상태 파일이 없습니다. 가장 최근 블록 번호부터 시작합니다")
    latest_block = get_latest_block_number()
    if latest_block is not None:
        save_state(latest_block)
    return latest_block

def hex_to_int_str(hex_val):
    if hex_val is None: return None
    try:
        return str(int(hex_val, 16))
    except (ValueError, TypeError): 
        return hex_val

def transform_transaction(tx):
    if not isinstance(tx, dict): 
        return tx
    numeric_fields = ['blockNumber', 'gas', 'gasPrice', 'maxFeePerGas', 
                      'maxPriorityFeePerGas', 'nonce', 'transactionIndex', 'value']
    transformed_tx = tx.copy()
    for field in numeric_fields:
        if field in transformed_tx:
            transformed_tx[field] = hex_to_int_str(transformed_tx[field])
    return transformed_tx
    
def fetch_and_save_block(block_num):
    sub_folder_name = f"{block_num // 10000 * 10000}_{block_num // 10000 * 10000 + 9999}"
    sub_folder_path = os.path.join(LOCAL_DATA_DIR, sub_folder_name)
    os.makedirs(sub_folder_path, exist_ok=True)
    
    file_path = os.path.join(sub_folder_path, f"block_{block_num}.jsonl")

    if os.path.exists(file_path):
        return
        
    hex_block_num = hex(block_num)
    payload = {"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": [hex_block_num, True]}
    
    try:
        response = requests.post(ALCHEMY_URL, json=payload, timeout=15)
        response.raise_for_status()
        result = response.json().get('result')
        if result and result.get('transactions'):
            with open(file_path, 'w', encoding='utf-8') as f:
                for tx in result['transactions']:
                    f.write(json.dumps(transform_transaction(tx)) + '\n')
    except requests.exceptions.RequestException as e:
        pass

if __name__ == "__main__":
    start_block_of_job = load_or_initialize_state()
    
    if start_block_of_job is None:
        print("시작 블럭번호가 없습니다. 상태파일을 확인해주세요.")
    else:
        end_block_of_job = start_block_of_job - BLOCK_NUM
        
        os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

        print(f"\n--- Historical Ethereum Extractor ---")
        print(f"{BLOCK_NUM}개의 블럭을 가져옵니다.")
        print(f"작업 범위는 {start_block_of_job}번부터 {end_block_of_job}입니다.")
        print(f"작업한 데이터는 '{LOCAL_DATA_DIR}'에 저장됩니다.")
        
        tasks = range(start_block_of_job, end_block_of_job - 1, -1)
        
        if not list(tasks):
            print("작업할 분량이 없습니다. 세팅을 확인해주세요.")
        else:
            try:
                with Pool(processes=NUM_WORKERS) as pool:
                    list(tqdm(pool.imap_unordered(fetch_and_save_block, tasks), total=len(list(tasks))))

                # 작업이 성공적으로 완료되었을 때만 다음 시작 지점을 업데이트
                next_start_point = end_block_of_job
                print(f"\n--- 작업 완료! ---")
            except KeyboardInterrupt:
                next_start_point = tasks
                print("\n--- 작업이 중단되었습니다. ---")
            except Exception as e:
                next_start_point = tasks
                print(f"\n--- 에러 발생: {e}. 작업을 중단합니다. ---")
            save_state(next_start_point)