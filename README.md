# kfktest

다양한 상황에서 Kafka 의 기능을 테스트

## 사전 작업
- 환경 변수 `KFKTEST_SSH_PKEY` 에 AWS 에서 이용할 Private Key 경로를 지정
- `refers` 디렉토리에 아래의 두 파일 (가능한 최신 버전) 이 있어야 한다
  - `confluentinc-kafka-connect-jdbc-10.5.0.zip`
    - [confluent 의 JDBC 커넥터 페이지](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc?_ga=2.129728655.246901732.1655082179-1759829787.1651627548&_gac=1.126341503.1655171481.Cj0KCQjwwJuVBhCAARIsAOPwGASjitveKrkPlHSvd6FzJtL8sQZu-c1mrjjhFPBgtc4_f_fGhCBZHx8aAseAEALw_wcB) 에서 Download 를 클릭하여 받음
  - `mysql-connector-java_8.0.29-1ubuntu21.10_all.deb`
    - [MySQL 의 커넥터 다운로드 페이지](https://dev.mysql.com/downloads/connector/j/) 에서 OS 에 맞는 파일을 받음


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
    - 로컬에서 DB insert / select 실행
  - `pytest test_mysql.py::test_ct_remote_basic` 실행
    - 원격 장비에서 DB insert / select 실행
- MSSQL
  - `pytest test_mssql.py::test_ct_local_basic` 실행
    - 로컬에서 DB insert / select 실행
  - `pytest test_mssql.py::test_ct_local_basic` 실행
    - 원격 장비에서 DB insert / select 실행

### 주의할 점
- 테스트 실행전 기존 Kafka 토픽을 이용하고 있는 클라이언트는 모두 종료해야 한다.
  - `kafka-console-consumer` 는 테스트 과정의 delete_topic 이 호출된 뒤 실행해야 한다.
- Kafka JDBC Connector 는 `confluentinc-kafka-connect-jdbc-10.5.0` 이상을 써야한다 (MSSQL 에서 `READ_UNCOMMITTED` 를 지원).
