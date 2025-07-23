![image.png](attachment:f44d8284-2ff1-4871-81ea-6e3e57b1b81f:image.png)

## RAW_DATA

### tb_eth_batch_transactions

| Column | Type | Description |
| --- | --- | --- |
| transaction_hash | VARCHAR | 트랜잭션 해시 |
| transaction_index | VARCHAR | 블록 내 인덱스 |
| block_number | BIGINT | 블록 번호 |
| from_address | VARCHAR | 보내는 주소 |
| to_address | VARCHAR | 받는 주소 |
| value | DECIMAL(38,0) | 전송 ETH (wei 기준) |
| status | VARCHAR | 트랜잭션 상태 |
| timestamp | TIMESTAMP | 발생 시각 |
| key_year | string | 파티션: 연도 (예: 2024) |
| key_month | string | 파티션: 월 (예: 7 또는 07) |
| key_day | string | 파티션: 일 (예: 15) |
| key_hour | string | 파티션: 시 (예: 14) |

### tb_eth_historical_transactions

| Column | Type | Description |
| --- | --- | --- |
| transaction_hash | VARCHAR | 트랜잭션 해시 |
| transaction_index | VARCHAR | 블록 내 인덱스 |
| block_number | BIGINT | 블록 번호 |
| from_address | VARCHAR | 보내는 주소 |
| to_address | VARCHAR | 받는 주소 |
| value | DECIMAL(38,0) | 전송 ETH (wei 기준) |
| status | VARCHAR | 트랜잭션 상태 |
| timestamp | TIMESTAMP | 발생 시각 |
| key_year | string | 파티션: 연도 (예: 2024) |
| key_month | string | 파티션: 월 (예: 7 또는 07) |
| key_day | string | 파티션: 일 (예: 15) |
| key_hour | string | 파티션: 시 (예: 14) |

## eth_top_holders 10000등

| Column | Type | Description |
| --- | --- | --- |
| address | VARCHAR | 고래 지갑 주소 |
| eth_balance | VARCHAR | ETH 잔액 |
| percentage | VARCHAR | 전체 중 비율 |
| inserted_at | TIMESTAMP | 적재 시각 |

### eth_whale ⇒ top holder10000+새로운고래 나타날때 추가하도록

| Column | Type | Description |
| --- | --- | --- |
| detected_at | TIMESTAMP | 고래 탐지 시각 |
| address | VARCHAR | 신규 고래 주소 |
| first_seen_block | BIGINT | 최초 등장한 블록 번호 |
| first_seen_timestamp | TIMESTAMP | 최초 등장한 시간 |
| total_tx_count | INT | 총 트랜잭션 수 |
| total_value | BIGINT | 총 전송 ETH (wei 기준) |
| note | VARCHAR | 메모 (탐지 이유 등) |

## SPECTRUM

### tb_eth_batch_transactions

| Column Name | Type | Description |
| --- | --- | --- |
| transactionhash | string | 트랜잭션 해시 |
| transactionindex | string | 블록 내 트랜잭션 인덱스 |
| blockhash | string | 블록 해시 |
| blocknumber | bigint | 블록 번호 |
| from | string | 보낸 주소 |
| to | string | 받는 주소 |
| value | string | 전송된 값 (wei 단위) |
| input | string | 트랜잭션 입력값 (data) |
| functionselector | string | 함수 selector (4byte) |
| nonce | string | Nonce 값 |
| gas | string | Gas 한도 |
| gasprice | string | Gas 가격 |
| maxfeepergas | string | 최대 Fee Per Gas |
| maxpriorityfeepergas | string | 최대 우선순위 Fee Per Gas |
| gasused | string | 사용된 gas 양 |
| cumulativegasused | string | 누적 사용된 gas 양 |
| effectgasprice | string | 실제 사용된 gas price |
| contractaddress | string | 생성된 컨트랙트 주소 |
| type | string | 트랜잭션 타입 |
| status | string | 성공 여부 (1=성공, 0=실패) |
| logsbloom | string | Logs Bloom 필터 |
| timestamp | string | 트랜잭션 발생 시각 |
| decodedinput | string | 디코딩된 input 정보 (JSON) |
| accesslist | array<struct<...>> | Access List 정보 |
| authorizationlist | array<struct<...>> | 인증 정보 |
| logs | array<struct<...>> | 로그 정보 |
| year | string | 파티션: 연도 (예: 2024) |
| month | string | 파티션: 월 (예: 7 또는 07) |
| day | string | 파티션: 일 (예: 15) |
| hour | string | 파티션: 시 (예: 14) |

### tb_eth_transactions_parquet

| Column Name | Type | Description |
| --- | --- | --- |
| transactionhash | string | 트랜잭션 해시 |
| transactionindex | string | 블록 내 트랜잭션 인덱스 |
| blockhash | string | 블록 해시 |
| blocknumber | bigint | 블록 번호 |
| from | string | 보내는 주소 |
| to | string | 받는 주소 |
| value | string | 전송된 값 (wei 단위) |
| input | string | 트랜잭션 입력값 (data 필드) |
| functionselector | string | 함수 selector (4바이트) |
| nonce | string | Nonce 값 |
| gas | string | Gas 한도 |
| gasprice | string | Gas 가격 |
| maxfeepergas | string | 최대 fee per gas |
| maxpriorityfeepergas | string | 최대 우선순위 fee per gas |
| gasused | string | 실제 사용된 gas 양 |
| cumulativegasused | string | 누적 사용된 gas 양 |
| effectgasprice | string | 효과적 gas price |
| contractaddress | string | 생성된 컨트랙트 주소 |
| type | string | 트랜잭션 타입 |
| status | string | 실행 상태 (성공/실패) |
| logsbloom | string | 로그 블룸 필터 |
| timestamp | string | 트랜잭션 발생 시간 |
| decodedinput | string | 디코딩된 input (JSON 형식) |
| accesslist | array<struct<...>> | EIP-2930 access list 구조 |
| authorizationlist | array<struct<...>> | 서명 인증 구조체 |
| logs | array<struct<...>> | 발생한 이벤트 로그 리스트 |
| year | string (partition key) | 연도 기반 파티션 |
| month | string (partition key) | 월 기반 파티션 |
| day | string (partition key) | 일 기반 파티션 |
| hour | string (partition key) | 시간 기반 파티션 |

## ANALYTICS