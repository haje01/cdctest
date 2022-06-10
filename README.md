# kfktest

Kafka 의 각종 기능을 테스트

필요한 설정:
- 환경 변수 `KFKTEST_SSH_PKEY` 에 AWS 에서 이용할 Private Key 경로를 지정

## Kafka JDBC Source Connector 테스트

- DB 에서 Kafka 로 로그성 데이터 가져오기
- 대상 DBMS 는 MySQL 과 MSSQL

### 인프라 구축

Terraform 으로 AWS 에 필요 인스턴스 생성

- MySQL
  - `kfktest/mysql` 디렉토리로 이동 후
  - `snakemake -f temp/setup.json -j` 로 생성
  - `snakemake -f tmep/destroy -j` 로 제거
- MSSQL
  - `kfktest/mssql` 디렉토리로 이동 후
  - `snakemake -f temp/setup.json -j` 로 생성
  - `snakemake -f tmep/destroy -j` 로 제거

### 테스트 방법
`kfktest/tests` 디렉토리로 이동 후

- MySQL
  - `pytest test_mysql.py::test_ct_local_basic` 실행
- MSSQL
  - `pytest test_mssql.py::test_ct_local_basic` 실행

### 주의할 점
- 테스트 실행전 기존 Kafka 토픽을 이용하고 있는 클라이언트는 모두 종료해야 한다.
  - kafka-console-consumer 는 delete_topic 이 호출된 뒤 실행해야 한다.
- Kafka JDBC Connector 는 confluentinc-kafka-connect-jdbc-10.5.0 이상을 써야한다 (MSSQL 에서 READ_UNCOMMITTED 를 지원).
