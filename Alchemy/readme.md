# eth_s3.py 사용법

1. 최상단 설정에서 API 주소를 알맞게 변경해주세요!

2. 최상단 설정에서 할당된 블록 범위를
    "MY_START_BLOCK": 22758002,                  # 작업 시작 블록
    "MY_END_BLOCK":   18254393,                  # 작업 종료 블록 
에 알맞게 채워주세요!

3. eth_processing_state.json 파일 내에
    {"next_fetch_block": 22758002}
을 MY_START_BLOCK과 동일하게 채워주세요!

CLI에서 python eth_s3.py로 실행후 켜놓으면 범위 내의 블럭을 모두 업로드 할 때까지 반복해서 돌아갑니다!