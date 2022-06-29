# kfktest

다양한 측면 (프로파일, profile) 에서 Kafka 의 기능을 테스트

- Terraform 을 사용해 AWS 에 측면별 인프라를 기술
- Snakemake 를 통한 Terraform 호출로 인프라 생성/파괴
- pytest 로 시나리오별 테스트 수행

## 프로파일 공통 작업

여기서는 모든 프로파일에 공통적으로 필요한 작업을 소개한다.

### 사전 작업
- 내려 받을 Kafka 의 URL 확보
  - 예: `https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz`
- 환경 변수 `KFKTEST_SSH_PKEY` 에 AWS 에서 이용할 Private Key 경로를 지정
- `refers` 디렉토리에 아래의 파일들 (가능한 최신 버전) 이 있어야 한다
  - `confluentinc-kafka-connect-jdbc-10.5.0.zip`
    - [Confluent 의 JDBC 커넥터 다운로드](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc?_ga=2.129728655.246901732.1655082179-1759829787.1651627548&_gac=1.126341503.1655171481.Cj0KCQjwwJuVBhCAARIsAOPwGASjitveKrkPlHSvd6FzJtL8sQZu-c1mrjjhFPBgtc4_f_fGhCBZHx8aAseAEALw_wcB) 에서 Download 클릭하여 받음
  - `mysql-connector-java_8.0.29-1ubuntu21.10_all.deb`
    - [MySQL 의 커넥터 다운로드 페이지](https://dev.mysql.com/downloads/connector/j/) 에서 OS 에 맞는 파일을 받음
  - `debezium-connector-mysql-1.9.4.Final-plugin.tar` 및  `debezium-connector-sqlserver-1.9.4.Final-plugin.tar`
    - [Debezium 커넥터 다운로드](https://debezium.io/documentation/reference/stable/install.html) 에서 MySQL 및 MSSQL 을 위한 Debezium 커넥트를 각각 내려 받음
  - `confluent-hub install confluentinc/kafka-connect-replicator:7.1.1.zip`
    - [Confluent 의 Kafka Replicator 다운로드](https://www7.confluent.io/hub/confluentinc/kafka-connect-replicator) 에서 Download 클릭하여 받음
- `test.tfvars` 파일 만들기
  - 각 설정별  `deploy` 폴더 아래의 `variables.tf` 파일을 참고하여 아래와 같은 형식의 `test.tfvars` 파일을 만들어 준다.
  ```
  name = "kfktest-mysql"
  ubuntu_ami = "ami-063454de5fe8eba79"
  mysql_instance_type = "m5.large"
  kafka_instance_type = "t3.medium"
  insel_instance_type = "t3.small"
  key_pair_name = "AWS-KEY-PAIR-NAME"
  work_cidr = [
      "11.22.33.44/32"      # my pc ip
      ]
  db_user = "DB-USER"
  db_port = "DB-SERVER-PORT"
  kafka_url = "https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz"
  kafka_jdbc_connector = "../../refers/confluentinc-kafka-connect-jdbc-10.5.0.zip"
  mysql_jdbc_driver = "../../refers/mysql-connector-java_8.0.29-1ubuntu21.10_all.deb"
  mysql_dbzm_connector = "../../refers/debezium-connector-mysql-1.9.4.Final-plugin.tar"
  mssql_dbzm_connector = "../../refers/debezium-connector-sqlserver-1.9.4.Final-plugin.tar"
  tags = {
      Owner = "MY-EMAIL-ADDRESS"
  }
  ```

### 인프라 구축

Terraform 으로 프로파일별 AWS 인프라를 생성 및 삭제하는데, Terraform 을 직접 호출하지 않고 Snakemake 의 빌드 타겟으로 실행한다. 프로파일별 인프라 생성 및 파괴는 아래와 같은 식이다:

인프라 생성: `snakemake -f temp/{profile}/setup.json -j`
인프라 삭제: `snakemake -f temp/{profile}/destroy -j`

프로파일별 최초 인프라 구축시에는 아래와 같이 Terraform 모듈 설치가 필요하다.
```
cd deploy/{profile}
terraform init
```

예를 들어 `mysql` 프로파일을 위해서는

`snakemake -f temp/mysql/setup.json -j` 로 인프라를 생성하고
`snakemake -f temp/mysql/destroy -j` 로 인프라 삭제한다

### 테스트 방법

`kfktest/tests` 디렉토리로 이동 후, 원하는 프로파일의 테스트를 수행한다. 예를 들어 `mssql` 프로파일 테스트는

`pytest test_mssql.py`

와 같이 한다.

### 주의할 점
- 테스트 종료 후에는 꼭 `snakemake -f temp/{profile}/destroy -j` 를 호출해 필요 없게된 인프라를 제거 한다.
- 테스트 실행전 기존 Kafka 토픽을 이용하고 있는 클라이언트는 모두 종료해야 한다.
  - `kafka-console-consumer` 는 테스트 과정의 delete_topic 이 호출된 뒤 실행해야 한다.
- Kafka JDBC Connector 는 `confluentinc-kafka-connect-jdbc-10.5.0` 이상을 써야한다 (MSSQL 에서 `READ_UNCOMMITTED` 를 지원).

## 프로파일 소개

여기서는 큰 분류별로 테스트 프로파일의 목적과 기술적 세부 사항에 관해 소개한다.

### 기본적인 Kafka 기능 관련

`minimal` - 브로커, 프로듀서 및 컨슈머로 기본적인 기능을 테스트한다.

### Kafka JDBC Source Connector 관련

- Kafka JDBC Source Connector 를 이용해 CT (Change Tracking) 방식으로 DB 에서 Kafka 로 로그성 데이터 가져오기를 테스트한다.
- 대상 DBMS 별로 `mysql` 과 `mssql` 프로파일이 있다.


