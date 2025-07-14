# nodit_with_missing.py 사용법

1. .env파일 형식
    NODIT_API_KEYS=key1,key2,key3, ... ,key100(따옴표 없음)
   
    S3_ACCESS_KEY="..."
    S3_SECRET_KEY="..."

3. 메인 루프 블록 범위 설정
    INITIAL_START_BLOCK = 1         #시작 블록 번호 => 1으로 해놔도 eth_procession_state.json내에 저장된 블록번호부터 수집 시작.
   
    INITIAL_END_BLOCK   = 5_700_000 #마지막 블록 번호

5. eth_processing_state.json 
    {"next_fetch_block": 5001} #5001부터 수집 시작

