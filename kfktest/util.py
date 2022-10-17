"""

공용 유틸리티

"""
import io
from cgitb import enable
import random
import os
import json
from json import JSONDecodeError
import re
from datetime import datetime
from dateutil.parser import parse as parse_dt
import time
import binascii
import subprocess
from multiprocessing import Process, Queue
import gzip

import pymssql
from mysql.connector import connect
from faker import Faker
from faker.providers import internet, date_time, company, phone_number
import paramiko
import boto3
import pytest
from retry import retry
import pandas as pd
import boto3

# Insert / Select 프로세스 수
NUM_INS_PROCS = 10  # 10 초과이면 sshd 세션수 문제(?)로 Insert가 안되는 문제 발생
                    # 10 일때 CT 에서 이따금씩(?) 1~4 개 정도 메시지 손실 발생
NUM_SEL_PROCS = 4

KFKTEST_S3_BUCKET = os.environ.get('KFKTEST_S3_BUCKET')
KFKTEST_S3_DIR = os.environ.get('KFKTEST_S3_DIR')

# 빠른 테스트를 위해서는 EPOCH 와 BATCH 수를 10 정도로 줄여 테스트

# 테스트 시작전 미리 행 입력 : 40 (epoch) x 100 (batch) x 10 (process)
# 성능 테스트시 테스트 함수에 아래 데코레이터 추가
# @pytest.mark.parametrize('xtable', [(DB_PRE_EPOCH, DB_PRE_BATCH, None)], indirect=True)
DB_PRE_EPOCH = 40  # DB 초기화시 Insert 에포크 수
DB_PRE_BATCH = 100  # DB 초기화시 Insert 에포크당 행수
DB_PRE_ROWS = DB_PRE_EPOCH * DB_PRE_BATCH * NUM_INS_PROCS  #  DB 초기화시 Insert 된 행수

DB_EPOCH = 10  # DB Insert 에포크 수
DB_BATCH = 1000  # DB Insert 에포크당 행수
DB_ROWS = DB_EPOCH * DB_BATCH * NUM_INS_PROCS  # DB Insert 된 행수

TOPIC_PARTITIONS = 12  # 토픽 기본 파티션 수
TOPIC_REPLICATIONS = 1     # 토픽 기본 복제 수

# Kafka 내장 토픽 이름
INTERNAL_TOPICS = ['__consumer_offsets', 'connect-configs', 'connect-offsets',
    'connect-status', '__transaction_state',
    '_confluent-ksql-default__command_topic', 'default_ksql_processing_log',
    ]
DB_PORTS = {'mysql': 3306, 'mssql': 1433}
HOME = os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def linfo(msg):
    now = datetime.now().strftime('%H:%M:%S')
    print(f'{now} {msg}')


def db_port(profile):
    return DB_PORTS[profile]


def gen_fake_data(count):
    """Fake 데이터 생성."""
    fake = Faker()
    fake.add_provider(internet)
    fake.add_provider(date_time)
    fake.add_provider(company)
    fake.add_provider(phone_number)

    for i in range(count):
        data = {
            'id': i + 1,
            'name': fake.name(),
            'address': fake.address(),
            'ip': fake.ipv4_public(),
            'birth': fake.date(),
            'company': fake.company(),
            'phone': fake.phone_number()
        }
        yield data


def insert_fake(conn, cursor, epoch, batch, pid, profile, table='person', dt=None, show=False):
    """Fake 데이터를 DB insert."""
    assert profile in ('mysql', 'mssql')
    linfo(f"[ ] insert_fake {epoch} {batch} {table}")
    fake = Faker()
    fake.add_provider(internet)
    fake.add_provider(date_time)
    fake.add_provider(company)
    fake.add_provider(phone_number)

    if dt is None:
        sql = f"INSERT INTO {table}(pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
    else:
        sql = f"INSERT INTO {table}(regdt, pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    # if profile == 'mysql':
    #     sql = "INSERT INTO person(pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
    # else:
    #     sql = "INSERT INTO person(pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    for j in range(epoch):
        if batch == 1:
            if j % 20 == 0:
                linfo(f"Inserter {pid} epoch: {j+1}")
        else:
            linfo(f"Inserter {pid} epoch: {j+1}")
        rows = []
        for i in range(batch):
            row = [
                pid,
                j * batch + i,
                fake.name(),
                fake.address(),
                fake.ipv4_public(),
                fake.date(),
                fake.company(),
                fake.phone_number()
            ]
            if dt is not None:
                row.insert(0, dt)
            rows.append(tuple(row))
            if show:
                linfo(row)
        cursor.executemany(sql, rows)
        conn.commit()
    linfo(f"[v] insert_fake {epoch} {batch} {table}")


def insert_fake_tmp(profile, epoch, batch):
    """임시 테이블에 가짜 데이터 인서트."""
    con, cur = db_concur(profile)
    insert_fake(con, cur, epoch, batch, 1, profile, 'fake_tmp')


@retry(RuntimeError, tries=10, delay=3)
def SSH(host, name=None):
    """Paramiko SSH 접속 생성."""
    name = host if name is None else f'{name} ({host})'
    linfo(f"[ ] ssh connect {name}")
    ssh_pkey = os.environ['KFKTEST_SSH_PKEY']
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(ssh_pkey)
    try:
        ssh.connect(host, username='ubuntu', pkey=pkey)
    except (paramiko.ssh_exception.NoValidConnectionsError, paramiko.ssh_exception.SSHException) as e:
        raise RuntimeError(f"Can not connect to {host}")
    linfo(f"[v] ssh connect {name}")
    return ssh


def ssh_exec(ssh, cmd, kafka_env=True, stderr_type="stderr", ignore_err=False):
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

    Return:
        string: stdout

    """
    env = "source ~/.kenv && " if kafka_env else ""
    cmd = f'{env}{cmd}'
    # linfo(f"[ ] ssh_exec {cmd}")
    _, stdout, stderr = ssh.exec_command(cmd)
    es = stdout.channel.recv_exit_status()
    out = stdout.read().decode('utf8')
    err = stderr.read().decode('utf8')
    # linfo("f[v] ssh_exec {cmd}")
    if stderr_type == "stdout":
        out = err
        err = ''
    elif stderr_type == "ignore":
        err = ''
    elif es != 0 and err == '':
        err = out
    if es != 0 and not ignore_err :
        if err != '':
            ssh_addr = ssh.get_transport().getpeername()[0]
            raise Exception(f'[@{ssh_addr}] {cmd} <--- {err}')
    return out


def local_exec(cmd):
    """로컬에서 쉘 명령 실행."""
    return subprocess.run(cmd, shell=True)


def scp_to_remote(src, dst_addr, dst_dir):
    """로컬 파일을 원격지로 scp.

    Args:
        src (str): 원본 파일 경로
        dst_addr (str): 대상 노드 주소
        dst_dir (str): 대상 디렉토리

    """
    linfo(f'scp_to_remote: {src} to {dst_addr}:{dst_dir}')
    assert not dst_dir.endswith('/')
    ssh_pkey = os.environ['KFKTEST_SSH_PKEY']

    dst_ssh = SSH(dst_addr)
    # 대상 디렉토리 확보
    ssh_exec(dst_ssh, f'mkdir -p {dst_dir}', False)
    # scp
    src = os.path.abspath(src)
    cmd = f'scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '\
        f'-i {ssh_pkey} {src} ubuntu@{dst_addr}:{dst_dir}'
    return local_exec(cmd)


def list_topics(kfk_ssh, skip_internal=True):
    """토픽 리스팅.

    Args:
        profile: 프로파일 명
        skip_internal: 내부 토픽 생략 여부. 기본 True

    Returns:
        list: 토픽 이름 리스트

    """
    linfo("list_topics at kafka")
    ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --list --bootstrap-server localhost:9092')
    topics = ret.strip().split('\n')
    if skip_internal:
        topics = [t for t in topics if t not in INTERNAL_TOPICS]
    return topics


def create_topic(kfk_ssh, topic, partitions=TOPIC_PARTITIONS,
                 replications=TOPIC_REPLICATIONS, config=None):
    """토픽 생성.

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        topic (str): 생성할 토픽 이름

    """
    linfo(f"[ ] create_topic '{topic}'")
    cmd = f'kafka-topics.sh --create --topic {topic} --bootstrap-server localhost:9092 --partitions {partitions} --replication-factor {replications}'
    if config is not None:
        for ck, cv in config.items():
            cmd += f' --config {ck}={cv}'
    ret = ssh_exec(kfk_ssh, cmd)
    linfo(f"[v] create_topic '{topic}'")
    return ret



def claim_topic(kfk_ssh, topic, partitions=TOPIC_PARTITIONS,
                replications=TOPIC_REPLICATIONS, config=None):
    """토픽이 없는 경우만 생성.

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        topic (str): 생성할 토픽 이름

    """
    linfo(f"[ ] claim_topic: {topic} at kafka")
    topics = list_topics(kfk_ssh)
    if topic not in topics:
        create_topic(kfk_ssh, topic, partitions=partitions,
            replications=replications, config=config)
    linfo(f"[v] claim_topic: {topic} at kafka")


def describe_topic(kfk_ssh, topic):
    """토픽 정보 얻기.

    Returns:
        tuple: 토픽 정보, 토픽 파티션들 정보

    """
    ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --describe --topic {topic} --bootstrap-server localhost:9092')
    items = ret.strip().split('\n')
    items = [item.split('\t') for item in items]
    return items[0], items[1:]


def get_kafka_ssh(profile):
    """프로파일에 맞는 Kafka SSH 얻기."""
    setup = load_setup(profile)
    ip = setup['kafka_public_ip']['value']
    ssh = SSH(ip, 'kafka')
    return ssh


def get_ksqldb_ssh(profile):
    """프로파일에 맞는 ksqlDB SSH 얻기."""
    setup = load_setup(profile)
    ip = setup['ksqldb_public_ip']['value']
    ssh = SSH(ip, 'ksqldb')
    return ssh


@pytest.fixture
def xkfssh(xprofile, xkvmstart):
    linfo("xkfssh")
    ssh = get_kafka_ssh(xprofile)
    yield ssh


def check_topic_exists(kfk_ssh, topic):
    """토픽 존재 여부를 체크."""
    return _check_topic_exists(kfk_ssh, topic)


def _check_topic_exists(kfk_ssh, topic):
    ret = ssh_exec(kfk_ssh, "kafka-topics.sh --list --bootstrap-server localhost:9092")
    topics = ret.strip().split('\n')
    return topic in topics


@retry(RuntimeError, tries=7, delay=3)
def delete_topic(kfk_ssh, topic, ignore_not_exist=False):
    """토픽 삭제.

    토픽이 가끔 삭제되지 않는 이슈:
    - 프로듀서/컨슈머가 이용중인 토픽을 지우려고 할 때 발생하는 듯
    - 가급적 테스트 종료 후 충분한 시간이 지난 뒤 (재시작시 등) 지울 것
    - 문제 발생시 zookeeper shell 로 지워주어야

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        topic (str): 삭제할 토픽 이름
        ignore_not_exist (bool): 토픽이 존재하지 않는 경우 에러 무시
        retry (int): 삭제가 완료될 때까지 재시도 횟수. 기본값 None

    """

    linfo(f"[ ] delete_topic '{topic}'")

    try:
        ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --delete --topic {topic}  --bootstrap-server localhost:9092')
    except Exception as e:
        if 'does not exist' in str(e) and ignore_not_exist:
            linfo(f"   delete_topic '{topic}' - not exist")
            return
        else:
            raise e

    if _check_topic_exists(kfk_ssh, topic):
        raise RuntimeError(f"Topic {topic} still remain.")
    linfo(f"[v] delete_topic '{topic}'")
    return ret


@retry(RuntimeError, tries=10, delay=10)
def delete_ksql_objects(ssh, strtbls):
    """ksqlDB 에서 주어진 순서대로 스트림과 테이블을 삭제.

    - 테스트별 의존성에 맞게 삭제

    Args:
        strtbls: [(객체_타입, 객체_이름), ...] 형태의 리스트
            객체 타입:
                0: 스트림
                1: 테이블
            예: [(1, 'my_table'), (0, 'my_stream'), ...]
    """
    linfo(f"[ ] delete_ksql_objects '{strtbls}'")
    for atype, name in (strtbls):
        assert atype in (0, 1)
        if atype == 0:
            _ksql_exec(ssh, f'DROP STREAM IF EXISTS {name}')
        else:
            _ksql_exec(ssh, f'DROP TABLE IF EXISTS {name}')
    linfo(f"[v] delete_ksql_objects '{strtbls}'")


@retry(RuntimeError, tries=7, delay=3)
def delete_all_topics(kfk_ssh, profile):
    """대상 카프카의 모든 토픽 삭제.

    person* 패턴 토픽 한정

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체

    """
    topics = list_topics(kfk_ssh)
    linfo(f"[ ] delete_all_topics")
    if len(topics) > 0:
        topics = ','.join(topics)
        try:
            ret = ssh_exec(kfk_ssh, f"kafka-topics.sh --delete --topic '{topics}' --bootstrap-server localhost:9092")
        except Exception as e:
            import pdb; pdb.set_trace()
            raise e

        # 남은 토픽 확인
        topics = list_topics(kfk_ssh)
        num_topic = sum(topics)
        if num_topic > 0:
            raise RuntimeError(f"Topics {topics} still remain.")
    linfo(f"[v] delete_all_topics")
    return


# def delete_all_topics(profile):
#     """내장 토픽을 제외한 모든 토픽 제거."""
#     topics = list_topics(profile)
#     for topic in topics:
#         delete_topic(profile, topic)


def reset_topic(ssh, topic, partitions=TOPIC_PARTITIONS,
        replications=TOPIC_REPLICATIONS, config=None):
    """특정 토픽 초기화."""
    linfo(f"[ ] reset_topic '{topic}'")
    delete_topic(ssh, topic, True)
    create_topic(ssh, topic, partitions, replications, config)
    linfo(f"[v] reset_topic '{topic}'")


def count_topic_message(profile, topic, timeout=None):
    linfo("[ ] count_topic_message")
    setup = load_setup(profile)
    kfk_ip = setup['kafka_public_ip']['value']
    timeout = timeout * 1000 if timeout is not None else None
    stimeout = f"--timeout-ms {timeout}" if timeout is not None else ''
    # cmd = f'''kafkacat -b localhost:9092 -t {topic} -C -e -q | wc -l'''
    cmd = f'''kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {topic} {stimeout} --from-beginning | wc -l
'''
    kfk_ssh = SSH(kfk_ip)
    ret = ssh_exec(kfk_ssh, cmd)
    cnt = int(ret.strip())
    linfo(f"[v] count_topic_message {cnt}")
    return cnt


# def count_topic_message(profile, topic, from_begin=True, timeout=10):
#     """토픽의 메시지 수를 카운팅.

#     Args:
#         profile : 프로파일
#         topic (str): 토픽명. (하나 이상이면 ',' 로 구분)
#         from_begin (bool): 토픽의 첫 메시지부터. 기본값 True
#         timeout (int): 컨슘 타임아웃 초. 기본값 10초
#             너무 작은 값이면 카운팅이 끝나기 전에 종료될 수 있다.

#     """
#     linfo(f"[ ] count_topic_message - topic: {topic}, timeout: {timeout}")
#     assert type(topic) is str
#     setup = load_setup(profile)
#     kfk_ip = setup['kafka_public_ip']['value']
#     topics = topic.split(',')
#     procs = []
#     qs = []
#     for topic in topics:
#         q = Queue()
#         p = Process(target=_count_topic_message,
#                     args=(kfk_ip, topic, q, from_begin, timeout))
#         procs.append(p)
#         qs.append(q)
#         p.start()

#     total = 0
#     for i, p in enumerate(procs):
#         linfo(f"[ ] consume proc {i} {p} get queue result")
#         cnt = qs[i].get()
#         linfo(f"[v] consume proc {i} {p} get queue result {cnt}")
#         total += cnt
#         p.join()

#     linfo(f"[v] count_topic_message - topic: {topic}, timeout: {timeout} {total}")
#     return total


# def _count_topic_message(kfk_ip, topic, q, from_begin=True, timeout=10):
#     time.sleep(random.random() * 10)
#     linfo(f"[ ] _count_topic_message - topic: {topic}")
#     # cmd = f'''kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {topic} --timeout-ms {timeout * 1000}'''
#     cmd = f'''kcat -b localhost:9092 --topic {topic} -m {timeout} -q | wc -l'''
#     kfk_ssh = SSH(kfk_ip)

#     assert from_begin == True
#     # 타임아웃까지 기다림
#     ret = ssh_exec(kfk_ssh, cmd)
#     cnt = int(ret.strip())
#     linfo("1")
    # if from_begin:
    #     linfo("2-1")
    #     # cmd = f"""kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server=localhost:9092 --topic {topic} | awk -F ':' '{{sum += $3}} END {{print sum}}'"""
    #     cmd = f'''kcat -'''
    #     ret = ssh_exec(kfk_ssh, cmd)
    #     linfo("2-2")
    #     linfo(f"topic {topic} {ret.strip()}")
    #     cnt = int(ret.strip())
    #     q.put(cnt)
    # else:
    #     linfo("3-1")
    #     cmd += ' | tail -n 10 | grep Processed'
    #     ret = ssh_exec(kfk_ssh, cmd, stderr_type="stdout")
    #     linfo("3-1")
    #     msg = ret.strip().split('\n')[-1]
    #     match = re.search(r'Processed a total of (\d+) messages', msg)
    #     if match is not None:
    #         cnt = int(match.groups()[0])
    #     else:
    #         raise Exception(f"Not matching result: {msg}")
    # linfo(f"[v] _count_topic_message - topic: {topic} {cnt}")
    # return cnt


# def count_msg_from_topic(ssh, topics):
#     total = 0
#     for topic in topics:
#         cmd = f"""kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server=localhost:9092 --topic {topic} | awk -F ':' '{{sum += $3}} END {{print sum}}'"""
#         ret = ssh_exec(ssh, cmd)
#         print(topic, ret)
#         cnt = int(ret.strip())
#         total += cnt
#     return total


@retry(RuntimeError, tries=6, delay=5)
def register_jdbc(kfk_ssh, profile, db_addr, db_port, db_user, db_passwd,
        db_name, topic_prefix, com_hash, params=None):
    """카프카 JDBC Source 커넥터 등록.

    설정에 관한 참조:
        https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector/
        https://docs.confluent.io/4.0.1/connect/references/allconfigs.html

    Args:
        kfk_ssh: 카프카 노드로의 Paramiko SSH 객체
        profile (str): 커넥션 URL 용 DBMS 타입. mysql 또는 mssql
        db_addr (str): DB 주소 (Private IP)
        db_port (int): DB 포트
        db_name (str): 사용할 DB 이름
        db_user (str): DB 유저
        db_passwd (str): DB 유저 암호
        topic_prefix (str): 테이블을 넣을 토픽의 접두사
        com_hash (str): 커넥터 이름에 붙을 해쉬
        params (dict): 추가 인자들
            inc_col: Incremental 컬럼. 기본값 None
            ts_col: Timestamp 컬럼. 기본값 None
            query: 테이블이 아닌 Query Based Ingest 를 위한 쿼리 (tables 는 무시됨)
            query_topic : query 이용시 토픽 명
            tables: 대상 테이블들 이름. 하나 이상인 경우 ',' 로 구분. 기본값 person
                빈 문자열이면 대상 DB 의 모든 테이블
            tasks: 커넥터가 사용하는 테스크 수. 기본값 1
            poll_interval (int): ms 단위 폴링 간격. 기본값 5000
            batch_rows (int): 폴링시 가져올 행수. 기본값 1000
            ts_incl (bool): 타임스탬프 모드에서 쿼리시 기존 값 포함 여부. 기본값 false
            ts_delay (int): 타임 스탬프 기준 트랜잭션이 완성되기를 기다리는 ms 시간. 기본값 0

    """
    assert profile in ('mysql', 'mssql')
    if profile == 'mssql':
        db_url = f"jdbc:sqlserver://{db_addr}:{db_port};databaseName={db_name};encrypt=true;trustServerCertificate=true;"
    else:
        db_url = f"jdbc:mysql://{db_addr}:{db_port}/{db_name}"

    # 반복된 테스트에 문제가 없으려면 커넥터 이름이 매번 달라야 한다.
    assert com_hash is not None
    conn_name = f'jdbc_{profile}_{com_hash}'
    tables = params.get('tables', '')
    inc_col = params.get('inc_col', 'id')
    ts_col = params.get('ts_col')
    query = params.get('query')
    tasks = params.get('tasks', 1)
    poll_interval = params.get('poll_interval', 5000)
    batch_rows = params.get('batch_rows', 1000)
    ts_incl = params.get('ts_incl', False)
    ts_delay = params.get('ts_delay', 0)

    linfo(f"[ ] register_jdbc {conn_name} {inc_col} {ts_col} {tables} {tasks}")
    isolation = 'READ_UNCOMMITTED' if profile == 'mssql' else 'DEFAULT'
    if inc_col is None:
        assert ts_col is not None
        mode = 'timestamp'
    elif ts_col is None:
        mode = 'incrementing'
    else:
        mode = 'timestamp+incrementing'
    # poll.interval.ms 가 작고, batch.max.rows 가 클수록 DB 에서 데이터를 빠르게 가져온다.
    # https://stackoverflow.com/questions/59037507/with-kafka-jdbc-source-connector-getting-only-1000-records-sec-how-to-improve-t
    data = {
        'connector.class' : 'io.confluent.connect.jdbc.JdbcSourceConnector',
        'connection.url': db_url,
        'connection.user':db_user,
        'connection.password':db_passwd,
        'auto.create': False,
        'auto.evolve': False,
        'poll.interval.ms': 5000,
        'offset.flush.interval.ms': 60000,
        'delete.enabled': True,
        'transaction.isolation.mode': isolation,
        'mode': mode,
        'poll.interval.ms': poll_interval,
        'batch.max.rows': batch_rows,
        'tasks.max': tasks,
        'timestamp.inclusive': ts_incl,
        'timestamp.delay.interval.ms': ts_delay,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'value.converter.schemas.enable': False,
        "transforms":"copyFieldToKey,extractKeyFromStruct",
        "transforms.copyFieldToKey.type":"org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.copyFieldToKey.fields":"pid",
        "transforms.extractKeyFromStruct.type":"org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.extractKeyFromStruct.field":"pid"
    }
    if query is None:
        if len(tables) > 0:
            data['table.whitelist'] = tables
        else:
            if profile == 'mssql':
                data['table.blacklist'] = 'trace_xe_event_map,trace_xe_action_map,fake_tmp'
        data['topic.prefix'] = topic_prefix
        data['catalog.pattern'] = 'test'
    else:
        data['query'] = query
        # 주의: query 가 있는 경우 topic.prefix 에 완전한 토픽이름이 지정되어야 한다.
        assert 'query_topic' in params
        data['topic.prefix'] = params['query_topic']

    if inc_col is not None:
       data['incrementing.column.name'] = inc_col
    if ts_col is not None:
        data['timestamp.column.name'] = ts_col

    config = json.dumps(data)
    data = put_connector(kfk_ssh, conn_name, config)
    # 등록된 커넥터 상태 확인
    time.sleep(5)
    status = get_connector_status(kfk_ssh, conn_name)
    if status['tasks'][0]['state'] == 'FAILED':
        import pdb; pdb.set_trace()
        raise RuntimeError(status['tasks'][0]['trace'])
    linfo(f"[v] register_jdbc {conn_name} {inc_col} {ts_col} {tables} {tasks}")
    return data


@retry(RuntimeError, tries=6, delay=5)
def register_s3sink(kfk_ssh, profile, topics, param):
    """카프카 S3 Sink Connector 등록.

    설정에 관한 참조:
        https://data-engineer-tech.tistory.com/34
        https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html

    Args:
        kfk_ssh: 카프카 노드로의 Paramiko SSH 객체
        profile (str): 커넥션 URL 용 DBMS 타입. mysql 또는 mssql
        topics (str): 대상 토픽
        params (dict): 추가 인자들
            s3_bucket (str): 올릴 S3 버킷
            s3_dir (str): 올릴 S3 디렉토리
            s3_region (str): 올릴 S3 리전
            chash (str): 커넥터 이름 해쉬

    """
    chash = param.get('chash', _hash())
    s3_bucket = param.get('s3_bucket', KFKTEST_S3_BUCKET)
    s3_dir = param.get('s3_dir', KFKTEST_S3_DIR)
    s3_region = param.get('s3_region', 'ap-northeast-2')
    # 배치당 메시지 수에 비례해 키울 필요
    flush_size = param.get('flush_size', 5)
    # 테스트 종료전 확인 가능하게 설정 필요
    rot_interval = param.get('rot_interval', 5000)
    conn_name = f's3sink_{profile}_{chash}'
    linfo(f"[ ] register_s3sink {conn_name}")
    assert profile in ('mysql', 'mssql', 'nodb')

    data = {
        "name": conn_name,
        "topics": topics,
        "tasks.max": 1,
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": False,
        "value.converter.schemas.enable": False,
        "flush.size": flush_size,
        "rotate.schedule.interval.ms": rot_interval,
        "s3.bucket.name": s3_bucket,
        "s3.region": s3_region,
        "topics.dir": s3_dir,
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH/'minute'=mm",
        "partition.duration.ms": 60000,
        "timestamp.extractor": "Wallclock",
        "s3.compression.type": "gzip",
        "locale": "ko_KR",
        "timezone": "Asia/Seoul",
    }

    config = json.dumps(data)
    cred = boto3.Session().get_credentials()
    data = put_connector(kfk_ssh, conn_name, config, aws_vars=[cred.access_key, cred.secret_key])
    # 등록된 커넥터 상태 확인
    time.sleep(5)
    status = get_connector_status(kfk_ssh, conn_name)
    if status['tasks'][0]['state'] == 'FAILED':
        raise RuntimeError(status['tasks'][0]['trace'])
    linfo(f"[v] register_s3sink {conn_name}")
    return conn_name


@retry(RuntimeError, tries=6, delay=5)
def register_dbzm(kfk_ssh, profile, svr_name, db_addr, db_port, db_name,
        db_user, db_passwd, name_hash):
    """Debezium MySQL/MSSQL 커넥터 등록

    설정에 관한 참조:
        https://debezium.io/documentation/reference/1.9/connectors/mysql.html
        https://debezium.io/documentation/reference/1.9/connectors/sqlserver.html
        https://debezium.io/blog/2020/09/03/debezium-1-3-beta1-released/

    Args:
        kfk_ssh: 카프카 노드로의 Paramiko SSH 객체
        profile (str): 프로파일 명
        svr_name (str): DB 서버 명
        db_addr (str): DB 주소 (Private IP)
        db_port (int): DB 포트
        db_name (str): 대상 DB 이름
        db_user (str): DB 유저
        db_passwd (str): DB 유저 암호
        name_hash (str): 커넥터 이름에 붙을 해쉬

    """
    conn_name = f'dbzm_{profile}_{name_hash}'
    linfo(f"[ ] register_dbzm {conn_name} for {db_name}")

    # 공통 설정
    config = {
        "database.hostname": db_addr,
        "database.port": f"{db_port}",
        "database.user": db_user,
        "database.password": db_passwd,
        "database.server.name": svr_name,
        "database.history.kafka.bootstrap.servers": "localhost:9092",
        "database.history.kafka.topic": f"{profile}.history.{svr_name}",
        "include.schema.changes": "true",
        "tasks.max": "1"
    }

    if profile == 'mysql':
        cls_name = 'mysql.MySqlConnector'
        config["database.include.list"] = db_name
        config["database.server.id"] = "1"
    else:
        cls_name = 'sqlserver.SqlServerConnector'
        config['database.dbname'] = db_name

    config['connector.class'] = f"io.debezium.connector.{cls_name}"

    data = put_connector(kfk_ssh, conn_name, json.dumps(config))
    # 등록된 커넥터 상태 확인
    time.sleep(5)
    status = get_connector_status(kfk_ssh, conn_name)
    if status['tasks'][0]['state'] == 'FAILED':
        raise RuntimeError(status['tasks'][0]['trace'])
    linfo(f"[v] register_dbzm {conn_name} for {db_name}")
    return data


def get_connector_status(kfk_ssh, conn_name):
    """등록된 카프카 커넥터 상태를 얻음."""
    linfo(f"[ ] get_connector_status {conn_name}")
    cmd = f'''curl -s http://localhost:8083/connectors/{conn_name}/status'''
    ret = ssh_exec(kfk_ssh, cmd)
    status = json.loads(ret)
    try:
        linfo(f"[v] get_connector_status {conn_name} {status['tasks'][0]['state']}")
    except KeyError:
        msg = str(status)
        print(msg)
        raise RuntimeError(msg)
    return status


def put_connector(kfk_ssh, name, config, poll_interval=5000, aws_vars=None):
    """공용 카프카 커넥터 설정

    Args:
        kfk_ssh: Kafka 노드 Paramiko SSH 객체
        name (str): 커넥터 이름
        config (str): 커넥터 설정
            업데이트시는 갱신할 필드만 설정해도 됨
        poll_interval (int): ms 단위 폴링 간격. 기본값 5000

    """
    # curl 용 single-quote escape
    config = config.replace("'", r"'\''")

    # AWS credential 필요한 경우
    if aws_vars is not None:
        cmd = f'''mkdir -p ~/.aws && cat <<EOT > ~/.aws/credentials
[default]
aws_access_key_id = {aws_vars[0]}
aws_secret_access_key = {aws_vars[1]}
EOT
'''
        ret = ssh_exec(kfk_ssh, cmd)
        if 'error_code' in ret:
            raise RuntimeError(ret)

    # PUT 을 이용하면 새로 만들때나 갱신할 때 같은 코드를 쓸 수 있다.
    cmd = f'''
curl -vs -X PUT 'http://localhost:8083/connectors/{name}/config' \
    -H 'Content-Type: application/json' \
    --data-raw '{config}' '''

    ret = ssh_exec(kfk_ssh, cmd)
    if 'error_code' in ret:
        raise RuntimeError(ret)

    try:
        data = json.loads(ret)
    except Exception as e:
        msg = str(e)
        linfo(msg)
        raise RuntimeError(msg)
    return data


@retry(RuntimeError, tries=6, delay=5)
def list_kconn(kfk_ssh):
    """등록된 카프카 커넥터 리스트.

    Returns:
        list: 등록된 커넥터 이름 리스트

    """
    linfo(f"[ ] list_kconn")
    cmd = f'''curl -s http://localhost:8083/connectors'''
    ret = ssh_exec(kfk_ssh, cmd)
    try:
        conns = json.loads(ret)
    except json.decoder.JSONDecodeError as e:
        raise RuntimeError(str(e))
    linfo(f"[v] list_kconn")
    return conns


@retry(RuntimeError, tries=6, delay=5)
def pause_kconn(kfk_ssh, conn_name):
    """카프카 커넥터 정지."""
    linfo(f"[ ] pause_kconn {conn_name}")
    cmd = f'''curl -s -X DELETE http://localhost:8083/connectors/{conn_name}/pause'''
    try:
        ssh_exec(kfk_ssh, cmd, ignore_err=True)
    except json.decoder.JSONDecodeError as e:
        raise RuntimeError(str(e))
    linfo(f"[v] pause_kconn {conn_name}")


@retry(RuntimeError, tries=6, delay=5)
def restart_kconn(kfk_ssh, conn_name):
    """카프카 커넥터 재개."""
    linfo(f"[ ] restart_kconn {conn_name}")
    cmd = f'''curl -s -X DELETE http://localhost:8083/connectors/{conn_name}/restart'''
    try:
        ssh_exec(kfk_ssh, cmd, ignore_err=True)
    except json.decoder.JSONDecodeError as e:
        raise RuntimeError(str(e))
    linfo(f"[v] restart_kconn {conn_name}")


@retry(RuntimeError, tries=6, delay=5)
def unregister_kconn(kfk_ssh, conn_name):
    """카프카 커넥터 등록 해제.

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        conn_name (str): 커넥터 이름

    """
    linfo(f"[ ] unregister_kconn {conn_name}")
    # pause_kconn(kfk_ssh, conn_name)
    # time.sleep(5)
    cmd = f'''curl -s -X DELETE http://localhost:8083/connectors/{conn_name}'''
    try:
        ssh_exec(kfk_ssh, cmd, ignore_err=True)
    except json.decoder.JSONDecodeError as e:
        raise RuntimeError(str(e))
    linfo(f"[v] unregister_kconn {conn_name}")


def unregister_all_kconn(kfk_ssh):
    """등록된 모든 카프카 커넥터 등록 해제."""
    linfo("[ ] unregister_all_kconn")
    for kconn in list_kconn(kfk_ssh):
        unregister_kconn(kfk_ssh, kconn)
    linfo("[v] unregister_all_kconn")


@retry(RuntimeError, tries=6, delay=5)
def start_zookeeper(kfk_ssh):
    """주키퍼 시작."""
    linfo(f"[ ] start_zookeeper")
    ssh_exec(kfk_ssh, "sudo systemctl start zookeeper")
    # 시간 경과 후에 동작 확인
    time.sleep(5)
    # 브로커 시작 확인
    if not is_service_active(kfk_ssh, 'zookeeper'):
        raise RuntimeError('Zookeeper not launched!')
    linfo(f"[v] start_zookeeper")


@retry(RuntimeError, tries=6, delay=5)
def stop_zookeeper(kfk_ssh, ignore_err=False):
    """주키퍼 정지."""
    linfo(f"[ ] stop_zookeeper")
    ssh_exec(kfk_ssh, "sudo systemctl stop zookeeper", ignore_err=ignore_err)
    linfo(f"[v] stop_zookeeper")


@retry(RuntimeError, tries=6, delay=5)
def start_kafka_broker(kfk_ssh):
    """카프카 브로커 시작.

    주: 프로세스 kill 후 몇 번 시도가 필요한 듯.

    """
    linfo(f"[ ] start_kafka_broker")
    ssh_exec(kfk_ssh, "sudo systemctl start kafka")
    # 충분한 시간 경과 후에 동작을 확인해야 한다.
    # kill 시 Zookeeper 의 기존 /brokers/ids/0 겹치는 문제로 기동중 다시 죽을 수 있음
    time.sleep(20)
    # 브로커 시작 확인
    if not is_service_active(kfk_ssh, 'kafka'):
        raise RuntimeError('Broker not launched!')
    linfo(f"[v] start_kafka_broker")


def is_service_active(ssh, svc_name):
    """systemctl 을 이용해 서비스 Active 상태를 얻음."""
    ret = ssh_exec(ssh, f"systemctl status {svc_name} | grep Active | awk '{{print $2}}'")
    stat = ret.strip()
    linfo(f"is_service_active - {svc_name} {stat}")
    return stat == 'active'


def stop_kafka_broker(kfk_ssh, ignore_err=False):
    """카프카 브로커 정지.

    주: 의존성에 따라 Kafka Connect 를 먼저 멈추고 이것을 불러야 한다.

    """
    linfo(f"[ ] stop_kafka_broker")
    ssh_exec(kfk_ssh, "sudo systemctl stop kafka", ignore_err=ignore_err)
    # ssh_exec(kfk_ssh, "sudo $KAFKA_HOME/bin/kafka-server-stop.sh", ignore_err=ignore_err)
    linfo(f"[v] stop_kafka_broker")


@pytest.fixture
def xkvmstart(xprofile):
    """Kafka VM이 정지상태이면 재개."""
    linfo("xkvmstart")
    claim_vm_start(xprofile, 'kafka')
    yield


def claim_vm_start(profile, inst_name):
    """VM 실행 확인."""
    inst = ec2inst_by_name(profile, inst_name)
    state = inst.state['Name']
    if state != 'running':
        vm_start(profile, inst_name)


@pytest.fixture
def xzookeeper(xsetup, xkfssh):
    """주키퍼 동작 확인."""
    linfo("xzookeeper")
    claim_zookeeper(xkfssh)
    yield


@retry(RuntimeError, tries=10, delay=3)
def claim_zookeeper(kfk_ssh):
    """주키퍼 동작 확인."""
    linfo("[ ] claim_zookeeper")
    ret = ssh_exec(kfk_ssh, 'fuser 2181/tcp', stderr_type='stdout')
    # 주키퍼가 떠있지 않으면 시작
    if ret == '':
        start_zookeeper(kfk_ssh)
    linfo("[v] claim_zookeeper")


@pytest.fixture
def xkafka(xsetup, xkfssh, xzookeeper):
    """카프카 브로커 동작 확인."""
    linfo("xkafka")
    claim_kafka(xkfssh)
    yield


def claim_kafka(kfk_ssh):
    """카프카 브로커 동작 확인."""
    linfo("[ ] claim_kafka")
    # 카프카가 떠있지 않으면 시작
    if not is_service_active(kfk_ssh, 'kafka'):
        start_kafka_broker(kfk_ssh)
    linfo("[v] claim_kafka")


@pytest.fixture(params=[{
        'topics': [],      # 명시적으로 리셋할 토픽들
        'partitions': TOPIC_PARTITIONS,
        'replications': TOPIC_REPLICATIONS,
        'cdc': False,      # CDC 여부,
        'topic_cfg': None  # 토픽 생성 설정
    }])
def xtopic(xkfssh, xprofile, request):
    """카프카 토픽 초기화."""
    linfo("xtopic_")

    if request.param.get('cdc', False):
        reset_topic(xkfssh, f"db1")
        scm = 'dbo' if xprofile == 'mssql' else 'test'
        topic = f"db1.{scm}.person"
    else:
        topic = f"{xprofile}_person"
    topics = request.param.get('topics', [])
    if len(topics) == 0:
        topics = [topic]
    partitions = request.param.get('partitions', TOPIC_PARTITIONS)
    replications = request.param.get('replications', TOPIC_REPLICATIONS)
    delete_all_topics(xkfssh, xprofile)
    topic_cfg = request.param.get('topic_cfg', None)
    for _topic in topics:
        create_topic(xkfssh, _topic, partitions, replications, topic_cfg)
    yield topic


def setup_path(profile):
    return f'../temp/{profile}/setup.json'


@pytest.fixture(scope="session")
def xsetup(xprofile):
    """인프라 설치 정보."""
    linfo("xsetup")
    assert os.path.isfile(setup_path(xprofile))
    with open(setup_path(xprofile), 'rt') as f:
        return json.loads(f.read())


@pytest.fixture(scope="session")
def xcp_setup(xprofile, xsetup):
    """확보된 인프라 설치 정보를 원격 노드에 복사."""
    linfo("xcp_setup")
    from kfktest.cpsetup import cp_setup as _cp_setup
    _cp_setup(xprofile)
    yield xsetup


@pytest.fixture
def xdbconcur(xprofile):
    linfo("xdbconcur")
    conn, cursor = db_concur(xprofile)
    yield conn, cursor
    conn.close()


def db_concur(profile):
    """프로파일에 적합한 DB 커넥션과 커서 얻기."""
    setup = load_setup(profile)
    db_addr = setup[f'{profile}_public_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']
    if profile == 'mysql':
        conn = connect(host=db_addr, user=db_user, password=db_passwd, database="test")
    else:
        conn = pymssql.connect(host=db_addr, user=db_user, password=db_passwd, database="test")
    cursor = conn.cursor()
    return conn, cursor


def mysql_exec_many(cursor, stmt):
    """MySQL 용 멀티 라인 쿼리 실행

    주: 결과를 읽어와야 쿼리 실행이 됨

    """
    results = cursor.execute(stmt, multi=True)
    for res in results:
        linfo(res)


def drop_all_tables(profile):
    linfo(f"[ ] drop_all_tables {profile}")
    conn, cursor = db_concur(profile)
    if profile == 'mysql':
        sql = '''
            SELECT concat('DROP TABLE IF EXISTS `', table_name, '`;')
            FROM information_schema.tables
            WHERE table_schema = 'test';
        '''
        cursor.execute(sql)
        results = cursor.fetchall()
        sql = '\n'.join([r[0] for r in results])
        cursor.execute(sql)
    else:
        sql = '''
            EXEC sp_MSforeachtable @command1 = "DROP TABLE ?"
        '''
        cursor.execute(sql)

    conn.commit()
    linfo(f"[v] drop_all_tables {profile}")


def local_produce_proc(profile, pid, msg_cnt, acks=1, duprate=0, lagrate=0,
        withkey=False):
    """로컬 프로듀서 프로세스 함수."""
    from kfktest.producer import produce

    linfo(f"[ ] produce process {pid}")
    produce(profile, messages=msg_cnt, acks=acks, dev=True, duprate=duprate,
        lagrate=lagrate, withkey=withkey)
    linfo(f"[v] produce process {pid}")


def local_consume_proc(profile, pid, q, topic=None, timeout=10):
    """로컬 컨슈머 프로세스 함수."""
    from kfktest.consumer import consume

    linfo(f"[ ] consume process {pid}")
    cnt = consume(profile, dev=True, count_only=True, from_begin=True,
                  timeout=timeout, topic=topic)
    linfo(f"local consumer put cnt {cnt} to queue")
    q.put(cnt)
    linfo(f"[v] consume process {pid} {cnt}")


def remote_produce_proc(profile, setup, pid, msg_cnt):
    """원격 프로듀서 프로세스 함수."""
    from kfktest.producer import produce

    linfo(f"[ ] produce process {pid}")
    pro_ip = setup['producer_public_ip']['value']
    ssh = SSH(pro_ip, 'producer')
    cmd = f"cd kfktest/deploy/{profile} && python3 -m kfktest.producer {profile} -p {pid} -m {msg_cnt}"
    ret = ssh_exec(ssh, cmd, False)
    linfo(ret)
    linfo(f"[v] produce process {pid}")


def remote_consume_proc(profile, setup, pid):
    """원격 컨슈머 프로세스 함수."""
    from kfktest.consumer import consume

    linfo(f"[ ] consumer {pid}")
    pro_ip = setup['consumer_public_ip']['value']
    ssh = SSH(pro_ip, 'consumer')
    cmd = f"cd kfktest/deploy/{profile} && python3 -m kfktest.consumer {profile} -b -c -t 10"
    ret = ssh_exec(ssh, cmd, False)
    linfo(f"[v] consumer {pid}")
    return ret


def local_select_proc(profile, pid):
    """로컬에서 가짜 데이터 셀렉트 프로세스 함수."""
    linfo(f"Select process {pid} start")
    cmd = f"cd ../deploy/{profile} && python -m kfktest.selector {profile} -p {pid} -d "
    local_exec(cmd)
    linfo(f"Select process {pid} done")


def local_insert_proc(profile, pid, epoch=DB_EPOCH, batch=DB_BATCH, hide=False,
        table=None):
    """로컬에서 가짜 데이터 인서트 프로세스 함수."""
    linfo(f"[ ] local insert process {pid} {epoch} {table}")
    hide = '-n' if hide else ''
    cmd = f"cd ../deploy/{profile} && python -m kfktest.inserter {profile} -p {pid} -e {epoch} -b {batch} -d {hide}"
    if table is not None:
        cmd += f' -t {table}'
    local_exec(cmd)
    linfo(f"[v] local insert process {pid} {epoch} {table}")


def remote_select_proc(profile, setup, pid):
    """원격 셀렉트 노드에서 가짜 데이터 셀렉트 (원격 노드에 setup.json 있어야 함)."""
    linfo(f"[ ] select process {pid}")
    sel_ip = setup['selector_public_ip']['value']
    ssh = SSH(sel_ip, 'selector')
    cmd = f"cd kfktest/deploy/{profile} && python3 -m kfktest.selector {profile} -p {pid}"
    ret = ssh_exec(ssh, cmd, False)
    linfo(ret)
    linfo(f"[v] select process {pid}")
    return ret


def remote_insert_proc(profile, setup, pid, epoch=DB_EPOCH, batch=DB_BATCH,
        hide=False, table=None, delay=0):
    """원격 인서트 노드에서 가짜 데이터 인서트 (원격 노드에 setup.json 있어야 함)."""
    linfo(f"[ ] remote insert process {pid}")
    ins_ip = setup['inserter_public_ip']['value']
    hide = '-n' if hide else ''
    ssh = SSH(ins_ip, 'inserter')
    cmd = f"cd kfktest/deploy/{profile} && python3 -m kfktest.inserter {profile} -p {pid} -e {epoch} -b {batch} {hide} --delay {delay}"
    if table is not None:
        cmd += f' -t {table}'
    ret = ssh_exec(ssh, cmd, False)
    linfo(ret)
    linfo(f"[v] remote insert process {pid}")
    return ret


def producer_logger_proc(profile, messages=10000, latency=0):
    """프로듀서에서 로그 파일 생성."""
    linfo(f"[ ] producer_logger_proc")
    setup = load_setup(profile)
    pip = setup['producer_public_ip']['value']
    ssh = SSH(pip, 'producer')
    cmd = f"python3 -m kfktest.logger test.log -m {messages} -l {latency}"
    ret = ssh_exec(ssh, cmd, False)
    linfo(f"[v] producer_logger_proc")
    return ret


def inserter_kill_processes(profile, setup, cmd_ptrn):
    """원격 인서트 장비에서 명령 패턴으로 프로세스들 제거."""
    linfo(f"[ ] inserter_kill_processes")
    ins_ip = setup['inserter_public_ip']['value']
    ssh = SSH(ins_ip, 'delete')
    cmd = f'pkill -f "{cmd_ptrn}"'
    ret = ssh_exec(ssh, cmd, False)
    linfo(f"[v] inserter_kill_processes")
    return ret


@pytest.fixture(params=[
        {
        'pre_epoch': 0, 'pre_batch': 0, 'fix_regdt': None, 'tables': ['person'],
        'skip': False, 'datetime1': False
        }])
def xtable(xprofile, xkafka, request):
    """테스트용 테이블 초기화."""
    if request.param.get('skip', False):
        yield

    linfo("xtable")
    # 기존 테이블 삭제
    drop_all_tables(xprofile)

    # 생성
    for table in request.param.get('tables', ['person']):
        _xtable(xprofile, table, request)

    yield time.time()


def _xtable(xprofile, table, request):
    from kfktest.table import reset_table

    pre_epoch = request.param.get('pre_epoch', 0)
    pre_batch = request.param.get('pre_batch', 0)
    fix_regdt = request.param.get('fix_regdt', None)
    datetime1 = request.param.get('datetime1', False)
    conn, cursor = reset_table(xprofile, table, fix_regdt, datetime1=datetime1)

    from kfktest.inserter import insert
    if pre_epoch > 0 :
        linfo(f"[ ] insert initial data to {table}")
        # Insert 프로세스들 시작
        ins_pros = []
        for pid in range(1, NUM_INS_PROCS + 1):
            # insert 프로세스
            p = Process(target=local_insert_proc, args=(xprofile, pid,
                                                        pre_epoch,
                                                        pre_batch,
                                                        True,
                                                        table))
            ins_pros.append(p)
            p.start()

        for p in ins_pros:
            p.join()
        linfo(f"[v] insert initial data to {table}")


@pytest.fixture
def xconn(xkfssh, xsetup, xkafka):
    """테스트용 카프카 커넥트 기동 요청.

    - 커넥트가 기동되지 않았으면 기동
    - VM 정지 후 재시작하면 Kafka Connect 가 비정상 -> 재시작해야

    """
    linfo("xconn")
    claim_kafka_connect(xkfssh)
    yield


def claim_kafka_connect(kfk_ssh):
    """카프카 커넥트 요청."""
    linfo("[ ] claim_kafka_connect")
    if not is_service_active(kfk_ssh, 'kafka-connect'):
        start_kafka_connect(kfk_ssh)
    linfo("[v] claim_kafka_connect")


@retry(RuntimeError, tries=6, delay=5)
def start_kafka_connect(kfk_ssh):
    """카프카 커넥트 시작."""
    linfo(f"[ ] start_kafka_connect")
    ssh_exec(kfk_ssh, "sudo systemctl start kafka-connect")

    # 잠시 후 동작 확인
    time.sleep(5)
    # 시작 확인
    if not is_service_active(kfk_ssh, 'kafka-connect'):
        raise RuntimeError('Connect not launched!')
    linfo(f"[v] start_kafka_connect")


def restart_kafka_and_connect(profile, kfk_ssh, com_hash, cdc):
    """의존성을 고려해 Kafka 와 Kafka Connect 재시작.

    Args:
        kfk_ssh: 카프카 노드로의 Paramiko SSH 객체
        profile (str): 프로파일명
        com_hash (str): 테스트 공용 해쉬
        cdc (bool): CDC 이용 여부

    - stop_kafka_and_connect 으로 정지된 경우를 위한 재시작
    - 커넥터 재등록도 수행

    """
    start_kafka_broker(kfk_ssh)
    time.sleep(7)
    start_kafka_connect(kfk_ssh)
    setup = load_setup(profile)
    # 커넥터 등록해제한 경우 재등록
    # _xjdbc(profile, setup, kfk_ssh, com_hash)
    # if cdc:
    #     # enable_cdc(profile)  # disable 한 경우
    #     _xdbzm(profile, setup, kfk_ssh, com_hash)


def stop_kafka_and_connect(profile, kfk_ssh, com_hash):
    """의존성을 고려해 Kafka 브로커와 커넥트 정지.

    - 브로커만 정지해도 커넥트까지 정지되나, 그렇게 하면 메시지 손실이 생길 수 있다.
    - 메시지 중복은 괜찮아도, 메시지 손실은 곤란하다.
    - 커넥트를 먼저 정지하고, 브로커를 정지하면 적어도 손실은 없다.

    """
    stop_kafka_connect(profile, kfk_ssh, com_hash)
    stop_kafka_broker(kfk_ssh)


def stop_kafka_connect(profile, kfk_ssh, com_hash):
    """카프카 커넥트 정지.

    - 커넥트는 따로 정지 명령이 없고 kill (-9 는 아님!) 로 정지
    - 커넥트가 정지되면 모든 커넥터도 정지 (= 커넥터 등록 해제까지는 필요 없음)
    - 특정 커넥터만 정지 시켜야할 때는 해당 커넥터만 등록 해제 한다.

    """
    linfo(f"[ ] stop_kafka_connect")
    # 모든 커넥터 등록 해제 (= stop)
    # unregister_all_kconn(kfk_ssh)
    # time.sleep(3)
    # CDC 설정 해제 (실제 서비스에서는 해주는 것이 맞을 것)
    # if is_cdc_enabled(profile):
    #     cap_inst = f'dbo_person_{com_hash}'
    #     disable_cdc(profile, cap_inst)

    ssh_exec(kfk_ssh, "sudo kill $(fuser 8083/tcp)")
    time.sleep(3)
    linfo(f"[v] stop_kafka_connect")


@pytest.fixture
def xrmcons(xkfssh, xconn):
    """등록된 모든 카프카 Connector 등록 해제."""
    linfo("xrmcons")
    unregister_all_kconn(xkfssh)


@pytest.fixture
def xhash():
    """공용 해쉬."""
    linfo("xhash")
    return _hash()


def _hash():
    return binascii.hexlify(os.urandom(3)).decode('utf8')


@pytest.fixture(params=[{
        'inc_col': 'id', 'ts_col': None, 'query': None, 'query_topic': None,
        'tables': "person", "tasks": 1
    }])
def xjdbc(xprofile, xrmcons, xkfssh, xtable, xtopic, xconn, xsetup, xhash, request):
    """CT용 JDBC 소스 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    # 명시된 해쉬가 있으면 그것을 이용
    chash = request.param.get('chash', xhash)
    _xjdbc(xprofile, xsetup, xkfssh, chash, request.param)
    time.sleep(5)
    yield


def _xjdbc(profile, setup, kfssh, chash, params):
    linfo("xjdbc")
    db_addr = setup[f'{profile}_private_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']
    ret = register_jdbc(kfssh, profile, db_addr, DB_PORTS[profile],
        db_user, db_passwd, "test", f"{profile}_", chash,
        params=params)
    if 'error_code' in ret:
        raise RuntimeError(ret['message'])


@pytest.fixture()
def xs3rmdir(request):
    """s3sink 된 s3 디렉토리 제거."""
    s3_bucket = KFKTEST_S3_BUCKET
    s3_dir = KFKTEST_S3_DIR + "/"
    linfo(f"[ ] remove s3 dir")
    s3_region = 'ap-northeast-2'
    s3_rmdir(s3_bucket, s3_dir)
    linfo(f"[v] remove s3 dir")


@pytest.fixture(params=[{'topics': None}])
def xs3sink(xprofile, xrmcons, xkfssh, xhash, xs3rmdir, request):
    """S3 Sink Connector 등록."""
    topics = request.param.get('topics', None)
    topics = f'{xprofile}_person' if topics is None else topics
    yield register_s3sink(xkfssh, xprofile, topics, request.param)


@pytest.fixture
@pytest.mark.parametrize('xtopic', [{'cdc': True}], indirect=True)
def xdbzm(xprofile, xkfssh, xrmcons, xtopic, xconn, xsetup, xcdc, xhash):
    """CDC 용 Debezium Source 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    _xdbzm(xprofile, xsetup, xkfssh, xhash)
    time.sleep(5)
    yield


def _xdbzm(profile, setup, kfssh, com_hash):
    linfo("xdbzm")
    db_addr = setup[f'{profile}_private_ip']['value']
    db_user = setup['db_user']['value']
    db_passwd = setup['db_passwd']['value']['result']
    svr_name = "db1"

    ret = register_dbzm(kfssh, profile, svr_name, db_addr,
        DB_PORTS[profile], "test", db_user, db_passwd, com_hash)
    if 'error_code' in ret:
        raise RuntimeError(ret['message'])


def load_setup(profile):
    """Profile 에 맞는 setup.json 읽기.

    snakemake -f temp/{profile}/setup.json 로 미리 만들어져야 함.

    """
    path = os.path.join(HOME, 'temp', profile, 'setup.json')
    with open(path, 'rt') as f:
        return json.loads(f.read())


def kill_proc_by_port(ssh, port):
    """원격 노드의 프로세스를 열린 포트 번호로 강제 kill.

    Args:
        ssh: 원격 노드의 Paramiko SSH 객체
        port: 대상 포트

    """
    linfo(f"[ ] kill_proc_by_port {port}")
    cmd = f'kill -9 $(fuser {port}/tcp)'
    ssh_exec(ssh, cmd, stderr_type='stdout')
    linfo(f"[v] kill_proc_by_port {port}")


def ec2inst_by_name(profile, inst_name):
    setup = load_setup(profile)
    key = f'{inst_name}_instance_id'
    inst_id = setup[key]['value']
    ec2 = boto3.resource('ec2')
    inst = ec2.Instance(inst_id)
    return inst


def vm_stop(profile, inst_name):
    """VM 정지."""
    linfo(f"[ ] vm_stop {inst_name}")
    inst = ec2inst_by_name(profile, inst_name)
    inst.stop()
    inst.wait_until_stopped()
    # wait_vm_state(profile, inst_name, 'stopped')
    linfo(f"[v] vm_stop {inst_name}")


def vm_hibernate(profile, inst_name):
    """VM Hibernate."""
    linfo(f"[ ] vm_hibernate {inst_name} of {profile}")
    inst = ec2inst_by_name(profile, inst_name)
    inst.stop(Hibernate=True)
    inst.wait_until_stopped()
    # wait_vm_state(profile, inst_name, 'running')
    linfo(f"[v] vm_hibernate {inst_name} of {profile}")


@retry(RuntimeError, tries=10, delay=5)
def vm_start(profile, inst_name):
    """VM 시작."""
    linfo(f"[ ] vm_start {inst_name} of {profile}")
    inst = ec2inst_by_name(profile, inst_name)
    try:
        inst.start()
    except Exception as e:
        if 'is not in a state from' in str(e):
            raise RuntimeError(str(e))
        else:
            raise e
    inst.wait_until_running()
    # wait_vm_state(profile, inst_name, 'running')
    linfo(f"[v] vm_start {inst_name} of {profile}")


@retry(RuntimeError, delay=5)
def wait_vm_state(profile, inst_name, state):
    """원하는 VM 상태가 될 때까지 재시도"""
    inst = ec2inst_by_name(profile, 'kafka')
    _state = inst.state['Name']
    if _state == state:
        linfo(f"[v] wait_vm_state {inst_name} {state}.. (now {_state})")
    else:
        linfo(f"[ ] wait_vm_state {inst_name} {state}.. (now {_state})")
        raise RuntimeError("VM state mismtach.")



def count_rows(db_type, cursor):
    tbl = 'person' if db_type == 'mysql' else '[test].[dbo].[person]'
    cursor.execute(f'''
    SELECT COUNT(*) cnt
    FROM {tbl}
    ''')
    res = cursor.fetchone()
    return res[0]


def count_table_row(profile):
    """테이블 행수 얻기."""
    _, cursor = db_concur(profile)
    return count_rows(profile, cursor)


@pytest.fixture
def xcdc(xprofile, xtable, xhash):
    """CDC 가능 처리.

    MSSQL 에서만 동작

    Returns:
        str: SQL Server 의 Capture Instance 명

    """
    linfo("xcdc")
    if xprofile != 'mssql':
        yield
    else:
        yield enable_cdc(xprofile, xhash)


def enable_cdc(profile, com_hash):
    """MSSQL 에서 CDC 설정."""
    cap_inst = f'dbo_person_{com_hash}'
    linfo(f"[ ] enable_cdc {cap_inst}")
    sql = f'''
USE test;

EXEC sys.sp_cdc_enable_db

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'person',
    @role_name = NULL,
    @capture_instance = {cap_inst},
    @supports_net_changes = 1

COMMIT;
    '''
    _, cursor = db_concur(profile)
    cursor.execute(sql)
    linfo(f"[v] enable_cdc {cap_inst}")
    return cap_inst


def is_cdc_enabled(profile, db_name='test'):
    """MSSQL 에서 CDC 여부 확인."""
    _, cursor = db_concur(profile)

    cursor.execute(f'''
        SELECT is_cdc_enabled FROM sys.databases
        WHERE name = '{db_name}'
    ''')
    res = cursor.fetchone()
    return res[0] == True


def disable_cdc(profile, cap_inst):
    """MSSQL 에서 CDC 설정 해제."""
    _, cursor = db_concur(profile)

    linfo(f"[ ] disable_cdc for {cap_inst}")
    sql = f'''
USE test;

EXEC sys.sp_cdc_disable_table
    @source_schema = N'dbo',
    @source_name = N'person',
    @capture_instance = N'{cap_inst}'

EXEC sys.sp_cdc_disable_db

COMMIT;
    '''
    cursor.execute(sql)
    linfo(f"[v] disable_cdc for {cap_inst}")


def batch_fake_data(profile, start, end):
    """임시 가짜 테이블을 복사해 일별 테이블 생성.

    - fake_tmp 테이블이 생성되어 있어야 함.

    """
    start = parse_dt(str(start))
    end = parse_dt(str(end))
    tables = [dt.strftime('person_%Y%m%d') for dt in pd.date_range(start, end)]
    conn, cursor = db_concur(profile)
    for table in tables:
        sql = f'''
            DROP TABLE IF EXISTS {table};
            SELECT * INTO {table}
            FROM fake_tmp
        '''
        cursor.execute(sql)
        conn.commit()


def valid_location(bucket, path, for_dir=False):
    """S3상의 객체 위치가 유효한지 검증.

    Args:
        bucket (str): S3 버킷명
        path (str): 객체 경로명
        for_dir (bool): 디렉토리를 위한 검증 여부. 기본값 False(파일)

    Returns:
        bool: 유효하면 True, 아니면 False
    """
    bucket = bucket.lower()
    path = path.lower()
    if bucket.startswith('s3:'):
        return False
    if path.startswith('/'):
        return False
    if path.startswith('s3:'):
        return False

    if for_dir and not path.endswith('/'):
        return False

    return True


def s3_rmdir(bucket, adir, with_markfile=False, dry_run=False):
    """S3 경로의 디렉토리 이하를 재귀적으로 지운다.

    Note:
        1000개 이상 객체 제거에는 Paginator를 사용해야 함.

    Args:
        bucket (str): S3 버킷. 's3://'는 필요없음.
        adir (str): 버킷 아래 디렉토리 경로. '/'로 시작하지 않고, 종료는 '/'로 끝나야 함.
        with_markfile (bool): AWS EMR에서 생성하는 ~_$folder$ 파일도 함께 제거 여부.
            기본값: False
        dry_run (bool): 지우지는 않고 대상만 출력. 기본값 False
    """
    assert valid_location(bucket, adir, True)
    linfo("Remove directory: s3://{}/{}".format(bucket, adir))

    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=adir)

    def delete(to_delete):
        if not dry_run:
            client.delete_objects(Bucket=bucket, Delete=to_delete)
        else:
            for td in to_delete['Objects']:
                linfo("  {}".format(td['Key']))

    to_delete = dict(Objects=[])
    total = 0
    if dry_run:
        linfo("Directories to be deleted:")

    for item in pages.search('Contents'):
        if item is None:
            continue
        to_delete['Objects'].append(dict(Key=item['Key']))

        # S3 리미트에 도달하면 Flush
        if len(to_delete['Objects']) >= 1000:
            delete(to_delete)
            total += len(to_delete['Objects'])
            to_delete = dict(Objects=[])

    # 남은 것을 제거
    if len(to_delete['Objects']):
        delete(to_delete)
        total += len(to_delete['Objects'])
        to_delete = dict(Objects=[])

    if with_markfile:
        elm = adir.split('/')
        mark_file = elm[-2] + '_$folder$'
        mark_dir = '/'.join(elm[:-2])
        mark_path = '{}/{}'.format(mark_dir, mark_file)
        linfo("  Delete mark file: {}".format(mark_path))
        to_delete = dict(Objects=[dict(Key=mark_path)])
        delete(to_delete)
        total += 1

    if not dry_run:
        linfo("{} objects has been deleted.".format(total))


def s3_listfile(bucket, adir):
    """S3 디렉토리에 있는 파일들을 리스팅.

    Args:
        bucket (str): S3 버킷. 's3://'는 필요없음.
        adir (str): 버킷 아래 디렉토리 경로. '/'로 시작하지 않고, 종료는 '/'로 끝나야 함.

    Returns:
        list: 파일 리스트
    """
    assert valid_location(bucket, adir, True)
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=adir)

    objects = []
    for item in pages.search('Contents'):
        if item is None:
            continue
        objects.append(item['Key'])

    return objects


def s3_count_sinkmsg(bucket, adir):
    """S3 Sink 된 메시지 카운팅.

    - S3 경로에 S3 Sink 커넥터가 올린 메시지 수를 센다
    - 메시지는 .gz + json 형태 가정

    """
    linfo(f"[ ] s3_count_sinkmsg s3://{bucket}/{adir}")
    assert valid_location(bucket, adir, True)
    s3 = boto3.client('s3')
    mcnt = 0
    for key in s3_listfile(bucket, adir):
        print(key)
        if not key.endswith('.gz'):
            continue
        data = s3.get_object(Bucket=KFKTEST_S3_BUCKET, Key=key)
        body = data['Body'].read()
        cfile = io.BytesIO(body)
        dfile = gzip.GzipFile(fileobj=cfile)
        text = dfile.read().decode('utf8')
        for line in text.split('\n'):
            if line == '':
                continue
            # json 체크
            json.loads(line)
            mcnt += 1
    linfo(f"[v] s3_count_sinkmsg s3://{bucket}/{adir}")
    return mcnt


def rot_insert_proc(profile, num_insert, batch_row):
    """로테이션 테스트용 인서트."""
    linfo(f"[ ] rot_insert_proc {profile}")
    conn, cursor = db_concur(profile)

    for i in range(num_insert):
        # pid 를 메시지 ID 로 대용
        insert_fake(conn, cursor, 1, batch_row, i, profile, table='person')
        time.sleep(1)

    linfo(f"[v] rot_insert_proc {profile}")


def rot_table_proc(profile, max_rot):
    """분당 한 번 테이블 로테이션."""
    from kfktest.table import reset_table

    linfo(f"[ ] rot_table_proc {profile}")
    conn, cursor = db_concur('mssql')
    cnt = 0
    prev = None
    while True:
        now = datetime.now()
        thisdt = now.strftime('%d%H%M')
        if prev == thisdt:
            continue

        # 분이 바뀌었으면 로테이션 테이블로 이름 변경
        linfo(f"== rotate person to person_{prev} ==")
        if profile == 'mssql':
            sql = f"""EXEC sp_rename 'person', person_{prev}"""
        else:
            sql = f"""RENAME TABLE 'person' TO person_{prev}"""
        cursor.execute(sql)
        # 새 테이블 생성
        reset_table(profile, 'person', (conn, cursor))

        cnt += 1
        # 최대 로테이션 수 체크
        if cnt >= max_rot:
            break

        prev = thisdt
        time.sleep(1)

    linfo(f"[v] rot_table_proc {profile}")


@retry(RuntimeError, tries=10, delay=30)
def delete_all_ksql_streams(ksqlssh):
    """ksqlDB 의 모든 스트림 제거."""
    linfo("[ ] delete_all_ksql_streams")
    streams = list_ksql_streams(ksqlssh)
    for stream in streams:
        if stream == 'KSQL_PROCESSING_LOG':
            continue
        _ksql_exec(ksqlssh, f'DROP STREAM {stream}')
    linfo("[v] delete_all_ksql_streams")


@retry(RuntimeError, tries=10, delay=30)
def delete_all_ksql_tables(ksqlssh):
    """ksqlDB 의 모든 테이블 제거."""
    linfo("[ ] delete_all_ksql_tables")
    tables = list_ksql_tables(ksqlssh)
    for table in tables:
        _ksql_exec(ksqlssh, f'DROP TABLE {table} DELETE TOPIC')
    linfo("[v] delete_all_ksql_tables")


def list_ksql_streams(ksqlssh, skip_system=True):
    ret = _ksql_exec(ksqlssh, 'SHOW STREAMS')
    streams = [st['name'] for st in ret[0]['streams']]
    if skip_system:
        _streams = []
        for st in streams:
            if st in ('KSQL_PROCESSING_LOG'):
                continue
            _streams.append(st)
        streams = _streams
    return streams


def list_ksql_tables(ksqlssh):
    ret = _ksql_exec(ksqlssh, 'SHOW TABLES')
    return [st['name'] for st in ret[0]['tables']]


@pytest.fixture
def xksql(xprofile):
    """ksqlDB 초기화."""
    linfo("xksql")
    setup = load_setup(xprofile)
    addr = setup['ksqldb_public_ip']['value']
    ssh = SSH(addr)
    terminate_all_ksql_queries(ssh)
    # 테이블과 스트림 제거는 테스트별 커스텀 픽스쳐에서
    # delete_all_ksql_tables(ssh)
    # delete_all_ksql_streams(ssh)


@retry(RuntimeError, tries=10, delay=10)
def terminate_all_ksql_queries(ssh):
    linfo("[ ] terminate_all_ksql_queries")
    _ksql_exec(ssh, "TERMINATE ALL")
    linfo("[v] terminate_all_ksql_queries")


def ksql_exec(profile, sql, mode='ksql', _props=None, timeout=None):
    """ksqlDB 명령 실행

    profile (str): 프로파일
    sql (str): ksqlDB SQL 명령
    mode (str): 호출 방식. ksql, query 중 하나. ksql, query 중 하나 (기본값 ksql)
        ksql: SELECT 문 외
        query: SELECT 문

    """
    setup = load_setup(profile)
    addr = setup['ksqldb_public_ip']['value']
    ssh = SSH(addr)
    return _ksql_exec(ssh, sql, mode, _props, timeout=timeout)


def _ksql_exec(ssh, sql, mode='ksql', _props=None, timeout=None):
    """ksqlDB API 명령 수행.

    Args:
        ssh: ksqlDB 장비로의 SSH
        sql (string): 실행할 ksqlDB SQL 명령
        mode (string): 호출 방식. ksql, query 중 하나 (기본값 ksql)
            ksql: SELECT 문 외
            query: SELECT 문
        timeout (int): ksqlDB 명령 타임아웃

    Returns:
        dict: 하나의 JSON 인 경우
        list: JSON Lines 인 경우
    """
    linfo(f"_ksql_exec '{sql}'")
    assert mode in ('ksql', 'query')
    sql = sql.strip().replace("'", r"'\''").replace('"', r'\"').replace('\n', '\\n')
    if not sql.endswith(';'):
        sql += ';'
    if _props is None:
        props = ''
    else:
        assert type(_props) is dict
        props = f', "streamsProperties": {json.dumps(_props)}'

    timeout = f"-m {timeout}" if timeout is not None else ''
    cmd = f"""
    curl -s {timeout} -X POST http://localhost:8088/{mode} \
        -H 'content-type: application/vnd.ksql.v1+json; charset=utf-8' \
        -d '{{
            "ksql": "{sql}"
            {props}
          }}'
    """
    ignore_err = True if timeout is not None else False
    ret = ssh_exec(ssh, cmd, kafka_env=False, ignore_err=ignore_err)
    if 'error_code' in ret:
        raise RuntimeError(ret)
    try:
        data = json.loads(ret)
        return data
    except Exception as e:
        # JSON Lines 인 경우 시도
        try:
            lines = []
            for line in ret.split('\n'):
                if len(line) == 0:
                    continue
                data = json.loads(line)
                lines.append(data)
            return lines
        except Exception as e2:
            msg = str(e2)
            import pdb; pdb.set_trace()
            linfo(msg)
            raise RuntimeError(msg)


def setup_filebeat(profile, topic=None):
    """프로듀서 파일비트 설정."""
    setup = load_setup(profile)
    pip = setup['producer_public_ip']['value']
    pssh = SSH(pip, 'producer')
    kip = setup['kafka_private_ip']['value']
    topic = f'{profile}_person' if topic is None else topic
    yml = f'''
filebeat.config.modules.path: \\${{path.config}}/modules.d/*.yml
filebeat.inputs:
  - type: filestream
    id: my-filestream-id
    paths:
      - /home/ubuntu/test.log*
    encoding: utf-8

output.kafka:
  hosts: ["{kip}:9092"]
  codec.format:
    string: '%{{[message]}}'
  topic: '{topic}'
  partition.round_robin:
    reachable_only: false
  required_acks: 1
  compression: gzip
  max_message_bytes: 1000000
    '''
    ssh_exec(pssh, f'cat <<EOT | sudo tee /etc/filebeat/filebeat.yml\n{yml}\nEOT', kafka_env=False)
    ssh_exec(pssh, "sudo sed -i 's/enabled: false/enabled: true/' /etc/filebeat/modules.d/kafka.yml")
    ssh_exec(pssh, 'sudo service filebeat restart', kafka_env=False)
    time.sleep(3)


@pytest.fixture
def xlog(xprofile):
    """프로듀서에 logger 로 생성된 로그 제거."""
    setup = load_setup(xprofile)
    pro_ip = setup['producer_public_ip']['value']
    ssh = SSH(pro_ip, 'producer')
    ssh_exec(ssh, 'rm -f /home/ubuntu/test.log*')
