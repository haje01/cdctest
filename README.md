# cdctest

CDC (Change Data Capture) Test
- Debezium
- Kafka JDBC Connector


## 주의할 점
- 테스트 실행전 기존 Kafka 토픽을 이용하고 있는 클라이언트는 모두 종료해야 한다.
  - kafka-console-consumer 는 delete_topic 이 호출된 뒤 실행해야 한다.
- Kafka JDBC Connector 는 confluentinc-kafka-connect-jdbc-10.5.0 이상을 써야 MSSQL 에서 READ_UNCOMMITTED 를 지원한다.