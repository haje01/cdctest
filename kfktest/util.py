"""

공용 유틸리티

"""
import os
import json
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


def SSH(host):
    """Paramiko SSH 접속 생성."""
    print(f"Connect {host} via SSH")
    ssh_pkey = os.environ['KFKTEST_SSH_PKEY']

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.RSAKey.from_private_key_file(ssh_pkey)
    ssh.connect(host, username='ubuntu', pkey=pkey)
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


def list_topics(profile, skip_internal=True):
    """토픽 리스팅.

    Args:
        profile: 프로파일 명
        skip_internal: 내부 토픽 생략 여부. 기본 True

    Returns:
        list: 토픽 이름 리스트

    """
    print("list_topics at kafka")
    kfk_ssh = get_kafka_ssh(profile)
    ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --list --bootstrap-server localhost:9092')
    topics = ret.strip().split('\n')
    if skip_internal:
        topics = [t for t in topics if t not in INTERNAL_TOPICS]
    return topics


def create_topic(profile, topic, partitions=12, replications=1):
    """토픽 생성.

    Args:
        profile (str): 프로파일 명
        topic (str): 생성할 토픽 이름

    """
    kfk_ssh = get_kafka_ssh(profile)
    return ssh_exec(kfk_ssh, f'kafka-topics.sh --create --topic {topic} --bootstrap-server localhost:9092 --partitions {partitions} --replication-factor {replications}')


def claim_topic(profile, topic, partitions=12, replications=1):
    """토픽이 없는 경우만 생성.

    Args:
        profile (str):
        topic (str): 생성할 토픽 이름

    """
    print(f"claim_topic: {topic} at kafka for {profile}")
    topics = list_topics(profile)
    if topic not in topics:
        create_topic(profile, topic, partitions=partitions, replications=replications)


def describe_topic(profile, topic):
    """토픽 정보 얻기.

    Returns:
        tuple: 토픽 정보, 토픽 파티션들 정보

    """
    kfk_ssh = get_kafka_ssh(profile)
    ret = ssh_exec(kfk_ssh, f'kafka-topics.sh --describe --topic {topic} --bootstrap-server localhost:9092')
    items = ret.strip().split('\n')
    items = [item.split('\t') for item in items]
    return items[0], items[1:]


def get_kafka_ssh(profile):
    """프로파일에 맞는 Kafka SSH 얻기."""
    setup = load_setup(profile)
    ip = setup['kafka_public_ip']['value']
    ssh = SSH(ip)
    return ssh


def check_topic_exists(profile, topic):
    """토픽 존재 여부를 체크."""
    ssh = get_kafka_ssh(profile)
    return _check_topic_exists(ssh, topic)


def _check_topic_exists(kfk_ssh, topic):
    ret = ssh_exec(kfk_ssh, "kafka-topics.sh --list --bootstrap-server localhost:9092")
    topics = ret.strip().split('\n')
    return topic in topics


@retry(RuntimeError, tries=7, delay=3)
def delete_topic(profile, topic, ignore_not_exist=False):
    """토픽 삭제.

    토픽이 가끔 삭제되지 않는 이슈:
    - 프로듀서/컨슈머가 이용중인 토픽을 지우려고 할 때 발생하는 듯
    - 가급적 테스트 종료 후 충분한 시간이 지난 뒤 (재시작시 등) 지울 것
    - 문제 발생시 zookeeper shell 로 지워주어야

    Args:
        profile (str): 프로파일 명
        topic (str): 삭제할 토픽 이름
        ignore_not_exist (bool): 토픽이 존재하지 않는 경우 에러 무시
        retry (int): 삭제가 완료될 때까지 재시도 횟수. 기본값 None

    """

    print(f"try delete_topic '{topic}'")
    kfk_ssh = get_kafka_ssh(profile)

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
    print(f"delete_topic '{topic}' - success")
    return ret


def delete_all_topics(profile):
    """내장 토픽을 제외한 모든 토픽 제거."""
    topics = list_topics(profile)
    for topic in topics:
        delete_topic(profile, topic)


def reset_topic(profile, topic, partitions=12, replications=1):
    """특정 토픽 초기화."""
    print(f"reset_topic '{topic}'")
    delete_topic(profile, topic, True)
    create_topic(profile, topic, partitions, replications)


def count_topic_message(profile, topic, from_begin=True, timeout=10):
    """토픽의 메시지 수를 카운팅.

    - 카프카 커넥트가 제대로 동작하지 않는 경우
    - 계속 같은 수가 나와 조기 종료되는 문제 있음
    - 이럴 때는 카프카 커넥트를 재시작해야 한다

    Args:
        profile (str): 프로파일
        topic (str): 토픽명
        from_begin (bool): 토픽의 첫 메시지부터. 기본값 True
        timeout (int): 컨슘 타임아웃 초. 기본값 10초
            너무 작은 값이면 카운팅이 끝나기 전에 종료될 수 있다.

    """
    setup = load_setup(profile)
    kfk_ssh = get_kafka_ssh(profile)
    print(f"count_topic_message: topic {topic} timeout {timeout}")
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
    return cnt


def register_socon(profile, db_addr,
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
    print(f"register_socon {conn_name}: {db_url}")
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
    kfk_ssh = get_kafka_ssh(profile)
    ret = ssh_exec(kfk_ssh, cmd, stderr_type="ignore")
    return json.loads(ret)


def list_socons(profile):
    """등록된 카프카 Source 커넥터 리스트.

    Returns:
        list: 등록된 커넥터 이름 리스트

    """
    cmd = f'''curl -s http://localhost:8083/connectors'''
    ret = ssh_exec(get_kafka_ssh(profile), cmd)
    conns = json.loads(ret)
    return conns


def unregister_socon(profile, conn_name):
    """카프카 JDBC Source 커넥터 해제.

    Args:
        profile (str): 프로파일 명
        conn_name (str): 커넥터 이름

    """
    print(f"unregister_socon {conn_name}")
    cmd = f'''curl -X DELETE http://localhost:8083/connectors/{conn_name}'''
    ssh_exec(get_kafka_ssh(profile), cmd, ignore_err=True)


def unregister_all_socons(profile):
    """등록된 모든 카프카 Source 커넥터 해제."""
    print("unregister_all_socons")
    for socon in list_socons(profile):
        unregister_socon(profile, socon)


def start_zookeeper(profile):
    """주키퍼 시작."""
    print(f"=== start_zookeeper for {profile} ===")
    kfk_ssh = get_kafka_ssh(profile)
    ssh_exec(kfk_ssh, "cd $KAFKA_HOME && bin/zookeeper-server-start.sh -daemon config/zookeeper.properties && sleep 5")


def stop_zookeeper(profile, ignore_err=False):
    """주키퍼 정지."""
    print(f"=== stop_zookeeper for {profile} ===")
    kfk_ssh = get_kafka_ssh(profile)
    ssh_exec(kfk_ssh, "zookeeper-server-stop.sh", ignore_err=ignore_err)


@retry(RuntimeError, tries=6, delay=5)
def start_kafka_broker(profile):
    """카프카 브로커 시작.

    주: 프로세스 kill 후 몇 번 시도가 필요한 듯.

    """
    print(f"try start_kafka_broker for {profile}")
    kfk_ssh = get_kafka_ssh(profile)
    ssh_exec(kfk_ssh, "cd $KAFKA_HOME && bin/kafka-server-start.sh -daemon config/server.properties")
    # 충분한 시간 경과 후에 동작을 확인해야 한다.
    time.sleep(10)
    # 브로커 시작 확인
    ret = ssh_exec(kfk_ssh, 'fuser 9092/tcp', stderr_type='stdout')
    if ret == '':
        raise RuntimeError('Broker not launched!')
    print(f"=== start_kafka_broker for {profile} ===")


def stop_kafka_broker(profile, ignore_err=False):
    """카프카 브로커 정지."""
    print(f"=== stop_kafka_broker for {profile} ===")
    kfk_ssh = get_kafka_ssh(profile)
    ssh_exec(kfk_ssh, "kafka-server-stop.sh", ignore_err=ignore_err)


@pytest.fixture
def xkvmstart(xprofile):
    """Kafka VM이 정지상태이면 재개."""
    print('kvmstart')
    while True:
        inst = ec2inst_by_name(xprofile, 'kafka')
        state = inst.state['Name']
        if state == 'running':
            print("Kafka VM is running.")
            break
        print(f"Kafka VM state: {state}, Wait for running..")
        time.sleep(5)


@pytest.fixture
def xzookeeper(xsetup, xprofile, xkvmstart):
    """주키퍼 초기화."""
    # 기존 주키퍼 정지
    stop_zookeeper(xprofile, True)
    # 주키퍼 시작
    start_zookeeper(xprofile)
    yield


@pytest.fixture
def xkafka(xsetup, xprofile, xzookeeper):
    """카프카 브로커 초기화."""
    # 기존 브로커 정지
    stop_kafka_broker(xprofile, True)
    # 브로커 시작
    start_kafka_broker(xprofile)
    yield


@pytest.fixture
def xtopic(xprofile, xkafka):
    """테스트용 카프카 토픽 초기화."""
    reset_topic(xprofile, f"{xprofile}-person")
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
def xsocon(xprofile, xtable, xtopic, xsetup):
    """테스트용 카프카 커넥터 초기화 (테이블과 토픽 먼저 생성)."""
    cons_ip = xsetup['consumer_public_ip']['value']
    kafka_ip = xsetup['kafka_private_ip']['value']
    db_addr = xsetup[f'{xprofile}_private_ip']['value']
    db_user = xsetup['db_user']['value']
    db_passwd = xsetup['db_passwd']['value']['result']

    ssh = SSH(cons_ip)

    unregister_all_socons(xprofile)
    ret = register_socon(xprofile, db_addr, DB_PORTS[xprofile],
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
    print(f"=== kill_proc_by_port {port} at {addr} === ")
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
    ret = inst.stop()
    import pdb; pdb.set_trace()
    return ret


def vm_start(profile, inst_name):
    """VM 을 재개."""
    print(f"vm_start {inst_name} of {profile}")
    inst = ec2inst_by_name(profile, inst_name)
    ret = inst.start()
    return ret
