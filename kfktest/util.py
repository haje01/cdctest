"""

공용 유틸리티

"""
import os
import json
from json import JSONDecodeError
import re
import time
import binascii
import subprocess

import pymssql
from mysql.connector import connect
from faker import Faker
from faker.providers import internet, date_time, company, phone_number
import paramiko
import boto3
import pytest
from retry import retry

# Kafka 내장 토픽 이름
INTERNAL_TOPICS = ['__consumer_offsets', 'connect-configs', 'connect-offsets', 'connect-status']
DB_PORTS = {'mysql': 3306, 'mssql': 1433}
HOME = os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


def db_port(profile):
    return DB_PORTS[profile]


def insert_fake(conn, cursor, epoch, batch, pid, profile):
    """Fake 데이터를 DB insert."""
    assert profile in ('mysql', 'mssql')

    fake = Faker()
    fake.add_provider(internet)
    fake.add_provider(date_time)
    fake.add_provider(company)
    fake.add_provider(phone_number)

    if profile == 'mysql':
        sql = "INSERT INTO person(pid, sid, name, address, ip, birth, company, phone) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
    else:
        sql = "INSERT INTO person VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"

    for j in range(epoch):
        print(f"Inserter {pid} epoch: {j+1}")
        rows = []
        for i in range(batch):
            row = (
                pid,
                j * batch + i,
                fake.name(),
                fake.address(),
                fake.ipv4_public(),
                fake.date(),
                fake.company(),
                fake.phone_number()
            )
            rows.append(row)
        cursor.executemany(sql, rows)
        conn.commit()
        # time.sleep(0.8)


@retry(RuntimeError, tries=10, delay=3)
def SSH(host, name=None):
    """Paramiko SSH 접속 생성."""
    name = host if name is None else f'{name} ({host})'
    print(f"[ ] ssh connect {name}")
    ssh_pkey = os.environ['KFKTEST_SSH_PKEY']
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(ssh_pkey)
    try:
        ssh.connect(host, username='ubuntu', pkey=pkey)
    except paramiko.ssh_exception.NoValidConnectionsError:
        raise RuntimeError("Can not connnect to {host}")
    print(f"[v] ssh connect {name}")
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

    """
    env = "source ~/.kenv && " if kafka_env else ""
    cmd = f'{env}{cmd}'
    _, stdout, stderr = ssh.exec_command(cmd)
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
    print(f'scp_to_remote: {src} to {dst_addr}:{dst_dir}')
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
    print("list_topics at kafka")
    ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --list --bootstrap-server localhost:9092')
    topics = ret.strip().split('\n')
    if skip_internal:
        topics = [t for t in topics if t not in INTERNAL_TOPICS]
    return topics


def create_topic(kfk_ssh, topic, partitions=12, replications=1):
    """토픽 생성.

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        topic (str): 생성할 토픽 이름

    """
    return ssh_exec(kfk_ssh, f'kafka-topics.sh --create --topic {topic} --bootstrap-server localhost:9092 --partitions {partitions} --replication-factor {replications}')


def claim_topic(kfk_ssh, topic, partitions=12, replications=1):
    """토픽이 없는 경우만 생성.

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        topic (str): 생성할 토픽 이름

    """
    print(f"claim_topic: {topic} at kafka")
    topics = list_topics(kfk_ssh)
    if topic not in topics:
        create_topic(kfk_ssh, topic, partitions=partitions, replications=replications)


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


@pytest.fixture
def xkfssh(xprofile):
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

    print(f"[ ] delete_topic '{topic}'")

    try:
        ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --delete --topic {topic}  --bootstrap-server localhost:9092')
    except Exception as e:
        if 'does not exist' in str(e) and ignore_not_exist:
            print(f"   delete_topic '{topic}' - not exist")
            return
        else:
            raise e

    if _check_topic_exists(kfk_ssh, topic):
        raise RuntimeError(f"Topic {topic} still remain.")
    print(f"[v] delete_topic '{topic}'")
    return ret


def delete_all_topics(profile):
    """내장 토픽을 제외한 모든 토픽 제거."""
    topics = list_topics(profile)
    for topic in topics:
        delete_topic(profile, topic)


def reset_topic(profile, topic, partitions=12, replications=1):
    """특정 토픽 초기화."""
    print(f"[ ] reset_topic '{topic}'")
    delete_topic(profile, topic, True)
    create_topic(profile, topic, partitions, replications)
    print(f"[v] reset_topic '{topic}'")


def count_topic_message(kfk_ssh, topic, from_begin=True, timeout=10):
    """토픽의 메시지 수를 카운팅.

    - 카프카 커넥트가 제대로 동작하지 않는 경우
    - 계속 같은 수가 나와 조기 종료되는 문제 있음
    - 이럴 때는 카프카 커넥트를 재시작해야 한다

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        topic (str): 토픽명
        from_begin (bool): 토픽의 첫 메시지부터. 기본값 True
        timeout (int): 컨슘 타임아웃 초. 기본값 10초
            너무 작은 값이면 카운팅이 끝나기 전에 종료될 수 있다.

    """
    print(f"[ ] count_topic_message - topic: {topic}, timeout: {timeout}")
    cmd = f'''kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {topic} --timeout-ms {timeout * 1000}'''
    if from_begin:
        cmd += ' --from-beginning'
    cmd += ' | tail -n 10 | grep Processed'
    ret = ssh_exec(kfk_ssh, cmd, stderr_type="stdout")
    msg = ret.strip().split('\n')[-1]
    match = re.search(r'Processed a total of (\d+) messages', msg)
    if match is not None:
        cnt = int(match.groups()[0])
    else:
        raise Exception(f"Not matching result: {msg}")
    print(f"[v] count_topic_message - topic: {topic}, timeout: {timeout}")
    return cnt


@retry(RuntimeError, tries=6, delay=5)
def register_socon(kfk_ssh, profile, db_addr,
        db_port, db_user, db_passwd, db_name, tables, topic_prefix,
        poll_interval=5000):
    """카프카 JDBC Source 커넥터 등록.

    설정에 관한 설명 참조:
    https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector/

    Args:
        profile (str): 커넥션 URL 용 DBMS 타입. mysql 또는 mssql
        db_addr (str): DB 주소 (Private IP)
        db_port (int): DB 포트
        db_name (str): 사용할 DB 이름
        db_user (str): DB 유저
        db_passwd (str): DB 유저 암호
        tables (str): 대상 테이블 이름. 하나 이상인 경우 ',' 로 구분
        topic_prefix (str): 테이블을 넣을 토픽의 접두사
        poll_interval (int): ms 단위 폴링 간격. 기본값 5000

    """
    assert profile in ('mysql', 'mssql')
    if profile == 'mssql':
        db_url = f"jdbc:sqlserver://{db_addr}:{db_port};databaseName={db_name};encrypt=true;trustServerCertificate=true;"
    else:
        db_url = f"jdbc:mysql://{db_addr}:{db_port}/{db_name}"

    hash = binascii.hexlify(os.urandom(3)).decode('utf8')
    conn_name = f'my-socon-{hash}'
    print(f"[ ] register_socon {conn_name}: {db_url}")
    isolation = 'READ_UNCOMMITTED' if profile == 'mssql' else 'DEFAULT'

    cmd = f'''
curl -vs -X POST 'http://localhost:8083/connectors' \
    -H 'Content-Type: application/json' \
    --data-raw '{{ \
    "name" : "{conn_name}", \
    "config" : {{ \
        "connector.class" : "io.confluent.connect.jdbc.JdbcSourceConnector", \
        "connection.url":"{db_url}", \
        "connection.user":"{db_user}", \
        "connection.password":"{db_passwd}", \
        "auto.create": false, \
        "auto.evolve": false, \
        "delete.enabled": true, \
        "transaction.isolation.mode": "{isolation}", \
        "mode": "incrementing", \
        "incrementing.column.name" : "id", \
        "table.whitelist": "{tables}", \
        "topic.prefix" : "{topic_prefix}", \
        "poll.interval.ms": "{poll_interval}", \
        "tasks.max" : "1" \
    }} \
}}'
'''
    ret = ssh_exec(kfk_ssh, cmd)
    try:
        data = json.loads(ret)
    except Exception as e:
        msg = str(e)
        print(msg)
        raise RuntimeError(msg)
    print("[v] register_socon {profile} for {tables}")
    return data


def list_socons(kfk_ssh):
    """등록된 카프카 Source 커넥터 리스트.

    Returns:
        list: 등록된 커넥터 이름 리스트

    """
    cmd = f'''curl -s http://localhost:8083/connectors'''
    ret = ssh_exec(kfk_ssh, cmd)
    conns = json.loads(ret)
    return conns


def unregister_socon(kfk_ssh, conn_name):
    """카프카 JDBC Source 커넥터 해제.

    Args:
        kfk_ssh : Kafka 노드의 Paramiko SSH 객체
        conn_name (str): 커넥터 이름

    """
    print(f"unregister_socon {conn_name}")
    cmd = f'''curl -X DELETE http://localhost:8083/connectors/{conn_name}'''
    ssh_exec(kfk_ssh, cmd, ignore_err=True)


@retry(RuntimeError, tries=6, delay=5)
def unregister_all_socons(kfk_ssh):
    """등록된 모든 카프카 Source 커넥터 해제."""
    print("[ ] unregister_all_socons")
    for socon in list_socons(kfk_ssh):
        unregister_socon(kfk_ssh, socon)
    print("[v] unregister_all_socons")


def start_zookeeper(kfk_ssh):
    """주키퍼 시작."""
    print(f"[ ] start_zookeeper")
    ssh_exec(kfk_ssh, "cd $KAFKA_HOME && bin/zookeeper-server-start.sh -daemon config/zookeeper.properties && sleep 5")
    print(f"[v] start_zookeeper")


@retry(RuntimeError, tries=6, delay=5)
def stop_zookeeper(kfk_ssh, ignore_err=False):
    """주키퍼 정지."""
    print(f"[ ] stop_zookeeper")
    ssh_exec(kfk_ssh, "zookeeper-server-stop.sh", ignore_err=ignore_err)
    print(f"[v] stop_zookeeper")


@retry(RuntimeError, tries=6, delay=5)
def start_kafka_broker(kfk_ssh):
    """카프카 브로커 시작.

    주: 프로세스 kill 후 몇 번 시도가 필요한 듯.

    """
    print(f"[ ] start_kafka_broker")
    ssh_exec(kfk_ssh, "cd $KAFKA_HOME && bin/kafka-server-start.sh -daemon config/server.properties")
    # 충분한 시간 경과 후에 동작을 확인해야 한다.
    time.sleep(10)
    # 브로커 시작 확인
    ret = ssh_exec(kfk_ssh, 'fuser 9092/tcp', stderr_type='stdout')
    if ret == '':
        raise RuntimeError('Broker not launched!')
    print(f"[v] start_kafka_broker")


def stop_kafka_broker(kfk_ssh, ignore_err=False):
    """카프카 브로커 정지."""
    print(f"[ ] stop_kafka_broker")
    ssh_exec(kfk_ssh, "kafka-server-stop.sh", ignore_err=ignore_err)
    print(f"[v] stop_kafka_broker")


@pytest.fixture
def xkvmstart(xprofile):
    """Kafka VM이 정지상태이면 재개."""
    print('kvmstart')
    started = False
    while True:
        inst = ec2inst_by_name(xprofile, 'kafka')
        state = inst.state['Name']
        if state == 'running':
            print("Kafka VM is running.")
            break
        elif state == 'stopped' and not started:
            vm_start(xprofile, 'kafka')
            started = True
        print(f"Kafka VM state: {state}, Wait for running..")
        time.sleep(5)


@pytest.fixture
def xzookeeper(xsetup, xkfssh, xkvmstart):
    """주키퍼 초기화."""
    # 기존 주키퍼 정지
    stop_zookeeper(xkfssh, True)
    # 주키퍼 시작
    start_zookeeper(xkfssh)
    yield


@pytest.fixture
def xkafka(xsetup, xkfssh, xzookeeper):
    """카프카 브로커 초기화."""
    # 기존 브로커 정지
    stop_kafka_broker(xkfssh, True)
    # 브로커 시작
    start_kafka_broker(xkfssh)
    yield


@pytest.fixture
def xtopic(xkfssh, xprofile, xkafka):
    """테스트용 카프카 토픽 초기화."""
    reset_topic(xkfssh, f"{xprofile}-person")
    yield


def setup_path(profile):
    return f'../temp/{profile}/setup.json'


@pytest.fixture(scope="session")
def xsetup(xprofile):
    """인프라 설치 정보."""
    assert os.path.isfile(setup_path(xprofile))
    with open(setup_path(xprofile), 'rt') as f:
        return json.loads(f.read())


@pytest.fixture(scope="session")
def xcp_setup(xprofile, xsetup):
    """확보된 인프라 설치 정보를 원격 노드에 복사."""
    from kfktest.cpsetup import cp_setup as _cp_setup
    _cp_setup(xprofile)
    yield xsetup


@pytest.fixture
def xdbconcur(xprofile):
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
        print(res)


@pytest.fixture
def xtable(xprofile, xkafka):
    """테스트용 테이블 초기화."""
    from kfktest.table import reset_table

    conn, cursor = reset_table(xprofile)
    yield
    # print("Delete table 'person'")
    # if xprofile == 'mysql':
    #     cursor.execute('DROP TABLE IF EXISTS person;')
    # else:
    #     cursor.execute("IF OBJECT_ID('person', 'U') IS NOT NULL DROP TABLE person")

    conn.commit()


@pytest.fixture
def xconn(xkfssh, xsetup):
    """테스트용 카프카 커넥트 기동 요청.

    - 커넥트가 기동되지 않았으면 기동
    - VM 정지 후 재시작하면 Kafka Connect 가 비정상 -> 재시작해야

    """
    ret = ssh_exec(xkfssh, 'fuser 8083/tcp', stderr_type='stdout')
    if ret == '':
        start_kafka_connect(xkfssh)
    else:
        print("Kafka Connect is already running.")


@retry(RuntimeError, tries=6, delay=5)
def start_kafka_connect(kfk_ssh):
    """카프카 커넥트 시작."""
    print(f"[ ] start_kafka_connect")
    ssh_exec(kfk_ssh, "cd $KAFKA_HOME && bin/connect-distributed.sh -daemon config/connect-distributed.properties")
    # 잠시 후 동작 확인
    time.sleep(4)
    # 브로커 시작 확인
    ret = ssh_exec(kfk_ssh, 'fuser 9092/tcp', stderr_type='stdout')
    if ret == '':
        raise RuntimeError('Connect not launched!')
    print(f"[v] start_kafka_connect")


@pytest.fixture
def xsocon(xprofile, xkfssh, xtable, xtopic, xconn, xsetup):
    """테스트용 카프카 소스 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    db_addr = xsetup[f'{xprofile}_private_ip']['value']
    db_user = xsetup['db_user']['value']
    db_passwd = xsetup['db_passwd']['value']['result']

    unregister_all_socons(xkfssh)
    ret = register_socon(xkfssh, xprofile, db_addr, DB_PORTS[xprofile],
        db_user, db_passwd, "test", "person", f"{xprofile}-")
    try:
        conn_name = ret['name']
    except Exception as e:
        raise Exception(str(ret))
    yield

    # unregister_socon(xprofile, conn_name)


def load_setup(profile):
    """Profile 에 맞는 setup.json 읽기.

    snakemake -f temp/{profile}/setup.json 로 미리 만들어져야 함.

    """
    path = os.path.join(HOME, 'temp', profile, 'setup.json')
    with open(path, 'rt') as f:
        return json.loads(f.read())


def kill_proc_by_port(ssh, port):
    """원격 노드의 프로세스를 열린 포트 번호로 kill.

    Args:
        ssh: 원격 노드의 Paramiko SSH 객체
        port: 대상 포트

    """
    addr = ssh.get_transport().getpeername()[0]
    print(f"kill_proc_by_port {port} at {addr}")
    cmd = f'kill -9 $(fuser {port}/tcp)'
    ssh_exec(ssh, cmd, stderr_type='stdout')


def ec2inst_by_name(profile, inst_name):
    setup = load_setup(profile)
    key = f'{inst_name}_instance_id'
    inst_id = setup[key]['value']
    ec2 = boto3.resource('ec2')
    inst = ec2.Instance(inst_id)
    return inst


def vm_stop(profile, inst_name):
    """VM 을 정지."""
    print(f"vm_stop {inst_name} of {profile}")
    inst = ec2inst_by_name(profile, inst_name)
    inst.stop()
    wait_vm_state(profile, inst_name, 'stopped')


def vm_start(profile, inst_name):
    """VM 을 재개."""
    print(f"vm_start {inst_name} of {profile}")
    inst = ec2inst_by_name(profile, inst_name)
    inst.start()
    wait_vm_state(profile, inst_name, 'running')


@retry(RuntimeError, delay=3)
def wait_vm_state(profile, inst_name, state):
    """원하는 상태가 될 때까지 재시도"""
    while True:
        inst = ec2inst_by_name(profile, 'kafka')
        _state = inst.state['Name']
        time.sleep(3)
        if _state == state:
            print("Kafka VM is running.")
            break
        print(f"[ ] wait for {inst_name} vm of {profile} {state}.. (now {_state})")
    print(f"[v] wait for {inst_name} vm of {profile} {state}.. (now {_state})")
