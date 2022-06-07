"""

    테스트 공통.

"""
import os
import json

import paramiko
import pytest

SSH_PKEY = os.environ['CDCTEST_SSH_PKEY']
SETUP_PATH = '../mysql_jdbc/temp/setup.json'
# 내장 토픽 이름
INTERNAL_TOPICS = ['__consumer_offsets', 'connect-configs', 'connect-offsets', 'connect-status']


@pytest.fixture(scope="session")
def setup():
    """인프라 설치 정보."""
    assert os.path.isfile(SETUP_PATH)
    with open(SETUP_PATH, 'rt') as f:
        return json.loads(f.read())


def SSH(host):
    """Paramiko SSH 접속 생성."""
    print("Connect via SSH")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(SSH_PKEY)
    ssh.connect(host, username='ubuntu', pkey=pkey)
    return ssh


def _exec(ssh, cmd, env=None):
    """SSH 로 명령 실행"""
    _, stdout, stderr = ssh.exec_command(". ~/.myenv && " + cmd, environment=env)
    es = stdout.channel.recv_exit_status()
    out = stdout.read().decode('utf8')
    err = stderr.read().decode('utf8')
    if es != 0:
        if err == '':
            err = out
        ssh_addr = ssh.get_transport().getpeername()[0]
        raise Exception(f'[@{ssh_addr}] {cmd} <--- {err}')
    return out


def list_topics(node_ssh, kafka_addr, skip_internal=True):
    """토픽 리스팅.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 주소
        skip_internal: 내부 토픽 생략 여부. 기본 True

    Returns:
        list: 토픽 이름 리스트

    """
    ret = _exec(node_ssh, f'kafka-topics.sh --list --bootstrap-server {kafka_addr}:9092')
    topics = ret.strip().split('\n')
    if skip_internal:
        topics = [t for t in topics if t not in INTERNAL_TOPICS]
    return topics


def create_topic(node_ssh, kafka_addr, topic, partitions=12, replications=1):
    """토픽 생성.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 주소
        topic (str): 생성할 토픽 이름

    """
    return _exec(node_ssh, f'kafka-topics.sh --create --topic {topic} --bootstrap-server {kafka_addr}:9092 --partitions {partitions} --replication-factor {replications}')


def claim_topic(node_ssh, kafka_addr, topic, partitions=12, replications=1):
    """토픽이 없는 경우만 생성.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 주소
        topic (str): 생성할 토픽 이름

    """
    topics = list_topics(node_ssh, kafka_addr)
    if topic not in topics:
        return _exec(node_ssh, f'kafka-topics.sh --create --topic {topic} --bootstrap-server {kafka_addr}:9092 --partitions {partitions} --replication-factor {replications}')


def describe_topic(node_ssh, kafka_addr, topic):
    """토픽 정보 얻기.

    Returns:
        tuple: 토픽 정보, 토픽 파티션들 정보

    """
    ret = _exec(node_ssh, f'kafka-topics.sh --describe --topic {topic} --bootstrap-server {kafka_addr}:9092')
    items = ret.strip().split('\n')
    items = [item.split('\t') for item in items]
    return items[0], items[1:]


def delete_topic(node_ssh, kafka_addr, topic):
    """토픽 삭제.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 주소
        topic (str): 삭제할 토픽 이름

    """
    return _exec(node_ssh, f'kafka-topics.sh --delete --topic {topic}  --bootstrap-server {kafka_addr}:9092')


def delete_all_topics(node_ssh, kafka_addr):
    """내장 토픽을 제외한 모든 토픽 제거."""
    topics = list_topics(node_ssh, kafka_addr)
    for topic in topics:
        delete_topic(node_ssh, kafka_addr, topic)


def reset_topic(node_ssh, kafka_addr, topic, partitions=12, replications=1):
    """특정 토픽 초기화."""
    delete_topic(node_ssh, kafka_addr, topic)
    create_topic(node_ssh, kafka_addr, topic, partitions, replications)


def register_src_conn(node_ssh, kafka_addr, conn_name, db_type, db_addr, db_port, db_user, db_passwd, tables, topic_prefix, poll_interval=5000):
    """카프카 JDBC Source 커넥터 등록.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 주소
        conn_name (str): 커넥터 이름
        db_type (str): 커넥션 URL 용 DBMS 타입. mysql 또는 sqlserver
        db_addr (str): DB 주소
        db_port (int): DB 포트
        db_user (str): DB 유저
        db_passwd (str): DB 유저 암호
        tables (str): 대상 테이블 이름. 하나 이상인 경우 ',' 로 구분
        topic_prefix (str): 테이블을 넣을 토픽의 접두사
        poll_interval (int): ms 단위 폴링 간격. 기본값 5000

    """
    stmt = f'''
curl -vs -X POST 'http://{kafka_addr}:8083/connectors' \
    -H 'Content-Type: application/json' \
    --data-raw '{{ \
    "name" : "{conn_name}", \
    "config" : {{ \
        "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector", \
        "connection.url":"jdbc:{db_type}://{db_addr}:{db_port};databaseName=test;encrypt=true;trustServerCertificate=true;", \
        "connection.user":"{db_user}", \
        "connection.password":"{db_passwd}", \
        "mode": "incrementing", \
        "incrementing.column.name" : "id", \
        "table.whitelist": "{tables}", \
        "topic.prefix" : "{topic_prefix}", \
        "poll.interval.ms": "{poll_interval}", \
        "tasks.max" : "1" \
    }} \
}}'
'''
    ret = _exec(node_ssh, stmt)
    return json.loads(ret)


def list_src_conns(node_ssh, kafka_addr):
    """등록된 카프카 커넥터 리스트.

    Returns:
        list: 등록된 커넥터 이름 리스트

    """
    stmt = f'''curl http://{kafka_addr}:8083/connectors'''
    ret = _exec(node_ssh, stmt)
    conns = json.loads(ret)
    return conns


def unregister_src_conn(node_ssh, kafka_addr, conn_name):
    """카프카 JDBC Source 커넥터 해제.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 주소
        conn_name (str): 커넥터 이름

    """
    stmt = f'''curl -X DELETE http://{kafka_addr}:8083/connectors/{conn_name}'''
    _exec(node_ssh, stmt)


def unregister_all_src_conns(node_ssh, kafka_addr):
    """등록된 모든 카프카 Source 커넥터 해제."""
    for sconn in list_src_conns(node_ssh, kafka_addr):
        unregister_src_conn(node_ssh, kafka_addr, sconn)