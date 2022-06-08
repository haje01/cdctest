"""

    테스트 공통.

"""
import os
import json
import pdb
import re
import binascii

import paramiko
import pytest
from kafka import KafkaConsumer

SSH_PKEY = os.environ['CDCTEST_SSH_PKEY']
# 내장 토픽 이름
INTERNAL_TOPICS = ['__consumer_offsets', 'connect-configs', 'connect-offsets', 'connect-status']


def SSH(host):
    """Paramiko SSH 접속 생성."""
    print("Connect via SSH")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(SSH_PKEY)
    ssh.connect(host, username='ubuntu', pkey=pkey)
    return ssh


def _exec(ssh, cmd, kafka_env=True, stderr_type="stderr", ignore_err=False):
    """SSH 로 명령 실행

    Args:
        ssh: 명령을 실행할 Paramiko SSH 객체
        cmd (str): 명령
        kafka_env (bool): 카프카 환경 변수 설정 여부. 기본 True
        stderr_type (str): 표준 에러 출력을 어떻게 다룰 것인지
            - stderr: 에러 출력 (기본)
            - stdout: 표준 출력
            - ignore: 무시
        ignore_err (bool): 에러가 있어도 무시

    """
    env = ". ~/.myenv && " if kafka_env else ""
    _, stdout, stderr = ssh.exec_command(env + cmd)
    es = stdout.channel.recv_exit_status()
    out = stdout.read().decode('utf8')
    err = stderr.read().decode('utf8')
    if stderr_type == "stdout":
        out = err
        err = ''
    elif stderr_type == "ignore":
        err = ''
    elif es != 0 and err == '':
        err = out
    if not ignore_err:
        if err != '':
            ssh_addr = ssh.get_transport().getpeername()[0]
            raise Exception(f'[@{ssh_addr}] {cmd} <--- {err}')
    return out


def list_topics(node_ssh, kafka_addr, skip_internal=True):
    """토픽 리스팅.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 Private 주소
        skip_internal: 내부 토픽 생략 여부. 기본 True

    Returns:
        list: 토픽 이름 리스트

    """
    print("list_topics")
    ret = _exec(node_ssh, f'kafka-topics.sh --list --bootstrap-server {kafka_addr}:9092')
    topics = ret.strip().split('\n')
    if skip_internal:
        topics = [t for t in topics if t not in INTERNAL_TOPICS]
    return topics


def create_topic(node_ssh, kafka_addr, topic, partitions=12, replications=1):
    """토픽 생성.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 Private 주소
        topic (str): 생성할 토픽 이름

    """
    return _exec(node_ssh, f'kafka-topics.sh --create --topic {topic} --bootstrap-server {kafka_addr}:9092 --partitions {partitions} --replication-factor {replications}')


def claim_topic(node_ssh, kafka_addr, topic, partitions=12, replications=1):
    """토픽이 없는 경우만 생성.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 Private 주소
        topic (str): 생성할 토픽 이름

    """
    print(f"claim_topic: {topic}")
    topics = list_topics(node_ssh, kafka_addr)
    if topic not in topics:
        print("  create topic")
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
        kafka_addr (str): 카프카 브로커 Private 주소
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


def count_topic_message(node_ssh, kafka_addr, topic, from_begin=True, timeout=10000):
    """토픽의 메시지 수를 카운팅.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 Private 주소
        topic (str): 토픽명
        from_begin (bool): 토픽의 첫 메시지부터
        timeout (int): 컨슘 타임아웃

    """
    print(f"count_topic_message: {topic}")
    cmd = f'''kafka-console-consumer.sh --bootstrap-server {kafka_addr}:9092 --topic {topic} --timeout-ms {timeout}'''
    if from_begin:
        cmd += ' --from-beginning'
    cmd += ' | tail -n 10 | grep Processed'
    ret = _exec(node_ssh, cmd, stderr_type="stdout")
    msg = ret.strip().split('\n')[-1]
    match = re.search(r'Processed a total of (\d+) messages', msg)
    if match is not None:
        cnt = int(match.groups()[0])
    else:
        raise Exception(f"Not matching result: {msg}")
    return cnt


def register_sconn(node_ssh, kafka_addr, db_type, db_addr,
        db_port, db_user, db_passwd, db_name, tables, topic_prefix,
        poll_interval=5000):
    """카프카 JDBC Source 커넥터 등록.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 Private 주소
        db_type (str): 커넥션 URL 용 DBMS 타입. mysql 또는 sqlserver
        db_addr (str): DB 주소 (Private IP)
        db_port (int): DB 포트
        db_name (str): 사용할 DB 이름
        db_user (str): DB 유저
        db_passwd (str): DB 유저 암호
        tables (str): 대상 테이블 이름. 하나 이상인 경우 ',' 로 구분
        topic_prefix (str): 테이블을 넣을 토픽의 접두사
        poll_interval (int): ms 단위 폴링 간격. 기본값 5000

    """
    assert db_type in ('mysql', 'sqlserver')
    if db_type == 'sqlserver':
        db_url = f"jdbc:sqlserver://{db_addr}:{db_port};databaseName={db_name};encrypt=true;trustServerCertificate=true;"
    else:
        db_url = f"jdbc:mysql://{db_addr}:{db_port}/{db_name}"

    hash = binascii.hexlify(os.urandom(3)).decode('utf8')
    conn_name = f'my-sconn-{hash}'
    print(f"register_sconn {conn_name}")

    cmd = f'''
curl -vs -X POST 'http://{kafka_addr}:8083/connectors' \
    -H 'Content-Type: application/json' \
    --data-raw '{{ \
    "name" : "{conn_name}", \
    "config" : {{ \
        "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector", \
        "connection.url":"{db_url}", \
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
    ret = _exec(node_ssh, cmd, stderr_type="ignore")
    return json.loads(ret)


def list_sconns(node_ssh, kafka_addr):
    """등록된 카프카 커넥터 리스트.

    Returns:
        list: 등록된 커넥터 이름 리스트

    """
    cmd = f'''curl -s http://{kafka_addr}:8083/connectors'''
    ret = _exec(node_ssh, cmd)
    conns = json.loads(ret)
    return conns


def unregister_sconn(node_ssh, kafka_addr, conn_name):
    """카프카 JDBC Source 커넥터 해제.

    Args:
        node_ssh: 명령을 실행할 노드로의 Paramiko SSH 객체
        kafka_addr (str): 카프카 브로커 Private 주소
        conn_name (str): 커넥터 이름

    """
    print(f"unregister_sconn {conn_name}")
    cmd = f'''curl -X DELETE http://{kafka_addr}:8083/connectors/{conn_name}'''
    _exec(node_ssh, cmd, ignore_err=True)


def unregister_all_sconns(node_ssh, kafka_addr):
    """등록된 모든 카프카 Source 커넥터 해제."""
    print("unregister_all_sconns")
    for sconn in list_sconns(node_ssh, kafka_addr):
        unregister_sconn(node_ssh, kafka_addr, sconn)



@pytest.fixture
def topic(setup):
    """테스트용 카프카 토픽 초기화."""
    consumer_ip = setup['consumer_public_ip']['value']
    kafka_ip = setup['kafka_private_ip']['value']
    ssh = SSH(consumer_ip)
    claim_topic(ssh, kafka_ip, "my-topic-person")
    yield
    delete_topic(ssh, kafka_ip, "my-topic-person")


def remote_insert_fake(ins_ssh, pid, epoch, batch):
    """원격 인서트 노드에서 가짜 데이터 insert"""
    cmd = f"cd cdctest/mssql && python3 inserter.py temp/setup.json {pid} {epoch} {batch}"
    return _exec(ins_ssh, cmd, False)